// Critics' scores for the catalogue's films, from Wikidata.
//
// The Cinnamon score's only measure of how good a film is used to be its
// TMDB user rating, and TMDB's users rate like fans: Avengers: Infinity War
// (8.2) and Sound of Freedom (8.0) outrated The Lighthouse (7.5), and the
// Cinnamon Hundred filled up with crowd favourites. Critics are the other
// half of "is it brilliant", and Wikidata carries their scores openly
// (CC0): Metacritic's Metascore, and Rotten Tomatoes' Tomatometer and
// critics' average rating, as review-score statements (P444) qualified by
// who gave them (P447). Films are matched on their IMDb id (P345), which
// every scored film has.
//
// Only critics' scores are kept. A statement whose determination method
// (P459) names an audience or user score is skipped, as are deprecated
// statements and any value whose shape does not fit its source (Metascore
// "NN/100", Tomatometer "NN%", average "N.N/10"). Where a source has
// several values over time, the latest by point in time (P585) wins.
//
// Upserts critic_scores by movie_id. A film Wikidata has nothing on gets
// no row; a film whose scores vanish from Wikidata keeps its last ones,
// since a released film's critics' scores barely move.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { pathToFileURL } from 'node:url';
import { createClient } from '@supabase/supabase-js';
import { API_STATUSES, GATEWAY_STATUSES, resilientFetch } from './resilient-fetch';

// Below this a film almost never has a Metascore or a Tomatometer, and
// nothing that ranks by the Cinnamon score reaches that far down.
const VOTE_FLOOR = 50;
// IMDb ids per query. Sent as a GET (so a failed one can safely be sent
// again), and 150 keeps the address well inside what the service accepts.
const BATCH = 150;
// The query service asks clients to go gently: one query at a time, with
// a pause between.
const PAUSE_MS = 1000;
const UPSERT_BATCH = 500;

const ENDPOINT = 'https://query.wikidata.org/sparql';
// Wikimedia asks every client to say who it is.
const USER_AGENT = 'CinnamonDB-pipeline/1.0 (https://www.cinnamondb.com)';

export const METACRITIC = 'Q150248';
export const ROTTEN_TOMATOES = 'Q105584';

// Audience and user scores are what TMDB already gives us; only critics'
// scores belong here.
const AUDIENCE = /audience|user|popcorn|verified|fan/i;

export interface Binding {
  imdb?: { value: string };
  item?: { value: string };
  value?: { value: string };
  by?: { value: string };
  methodLabel?: { value: string };
  date?: { value: string };
}

interface Pick {
  value: number;
  // ISO date of the value, or '' when the statement has none.
  date: string;
}

export interface Found {
  wikidata: string | null;
  metacritic: Pick | null;
  tomatometer: Pick | null;
  average: Pick | null;
}

// The latest dated value wins; an undated one only fills a gap.
function better(current: Pick | null, next: Pick): Pick {
  if (!current) return next;
  if (next.date && (!current.date || next.date > current.date)) return next;
  return current;
}

function lastSegment(uri: string): string {
  return uri.slice(uri.lastIndexOf('/') + 1);
}

export function parse(bindings: Binding[]): Map<string, Found> {
  const out = new Map<string, Found>();
  for (const b of bindings) {
    const imdb = b.imdb?.value;
    const raw = b.value?.value?.trim();
    const by = b.by ? lastSegment(b.by.value) : '';
    if (!imdb || !raw || !by) continue;
    if (AUDIENCE.test(b.methodLabel?.value ?? '')) continue;

    const date = b.date?.value ?? '';
    const found: Found = out.get(imdb) ?? { wikidata: null, metacritic: null, tomatometer: null, average: null };
    if (!found.wikidata && b.item) found.wikidata = lastSegment(b.item.value);

    if (by === METACRITIC) {
      const m = raw.match(/^(\d{1,3})(?:\s*\/\s*100)?$/);
      const v = m ? Number(m[1]) : NaN;
      if (v >= 0 && v <= 100) found.metacritic = better(found.metacritic, { value: v, date });
    } else if (by === ROTTEN_TOMATOES) {
      const pct = raw.match(/^(\d{1,3})\s*%$/);
      const avg = raw.match(/^(\d{1,2}(?:\.\d+)?)\s*\/\s*10$/);
      if (pct) {
        const v = Number(pct[1]);
        if (v >= 0 && v <= 100) found.tomatometer = better(found.tomatometer, { value: v, date });
      } else if (avg) {
        const v = Math.round(Number(avg[1]) * 10) / 10;
        if (v >= 0 && v <= 10) found.average = better(found.average, { value: v, date });
      }
    }
    out.set(imdb, found);
  }
  return out;
}

export function sparql(imdbIds: string[]): string {
  const values = imdbIds.map((id) => `"${id}"`).join(' ');
  return `SELECT ?imdb ?item ?value ?by ?methodLabel ?date WHERE {
  VALUES ?imdb { ${values} }
  VALUES ?by { wd:${METACRITIC} wd:${ROTTEN_TOMATOES} }
  ?item wdt:P345 ?imdb ;
        p:P444 ?st .
  ?st ps:P444 ?value ;
      pq:P447 ?by ;
      wikibase:rank ?rank .
  FILTER(?rank != wikibase:DeprecatedRank)
  OPTIONAL { ?st pq:P459 ?method . ?method rdfs:label ?methodLabel . FILTER(LANG(?methodLabel) = "en") }
  OPTIONAL { ?st pq:P585 ?date . }
}`;
}

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_ROLE_KEY'), {
    global: { fetch: resilientFetch({ label: 'database', retryOn: GATEWAY_STATUSES }) },
  });
  // A query is a read, so it is sent again on a dropped connection, a rate
  // limit or a 5xx (see resilient-fetch.ts).
  const wikidataFetch = resilientFetch({ label: 'Wikidata', retryOn: API_STATUSES, attempts: 5 });

  const startedAt = new Date().toISOString();

  // Films to look up, by IMDb id. Several catalogue rows can share one id.
  const byImdb = new Map<string, string[]>();
  const PAGE = 1000;
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await supabase
      .from('movies')
      .select('id, imdb_id')
      .eq('scoring_status', 'scored')
      .gte('vote_count', VOTE_FLOOR)
      .not('imdb_id', 'is', null)
      .order('id', { ascending: true })
      .range(from, from + PAGE - 1);
    if (error) throw new Error(`Reading films failed: ${error.message}`);
    const rows = (data ?? []) as { id: string; imdb_id: string }[];
    for (const r of rows) {
      if (!/^tt\d{5,10}$/.test(r.imdb_id)) continue;
      const ids = byImdb.get(r.imdb_id) ?? [];
      ids.push(r.id);
      byImdb.set(r.imdb_id, ids);
    }
    if (rows.length < PAGE) break;
  }
  const imdbIds = [...byImdb.keys()];
  console.log(`${imdbIds.length} films to look up on Wikidata.`);

  const found = new Map<string, Found>();
  const methods = new Map<string, number>();
  let failedBatches = 0;

  for (let i = 0; i < imdbIds.length; i += BATCH) {
    const batch = imdbIds.slice(i, i + BATCH);
    const url = `${ENDPOINT}?format=json&query=${encodeURIComponent(sparql(batch))}`;
    try {
      const res = await wikidataFetch(url, {
        headers: { Accept: 'application/sparql-results+json', 'User-Agent': USER_AGENT },
      });
      if (!res.ok) throw new Error(`answered ${res.status}`);
      const json = (await res.json()) as { results?: { bindings?: Binding[] } };
      const bindings = json.results?.bindings ?? [];
      for (const b of bindings) {
        const label = b.methodLabel?.value ?? '(none)';
        methods.set(label, (methods.get(label) ?? 0) + 1);
      }
      for (const [imdb, f] of parse(bindings)) found.set(imdb, f);
    } catch (err) {
      failedBatches++;
      console.error(`Batch at ${i} failed: ${(err as Error).message}`);
    }
    if ((i / BATCH) % 20 === 0) {
      console.log(`Progress: ${Math.min(i + BATCH, imdbIds.length)}/${imdbIds.length}, ${found.size} with scores.`);
    }
    await sleep(PAUSE_MS);
  }

  const stamp = new Date().toISOString();
  const rows = [];
  for (const [imdb, f] of found) {
    if (!f.metacritic && !f.tomatometer && !f.average) continue;
    for (const movieId of byImdb.get(imdb) ?? []) {
      rows.push({
        movie_id: movieId,
        imdb_id: imdb,
        wikidata_id: f.wikidata,
        metacritic: f.metacritic?.value ?? null,
        rt_tomatometer: f.tomatometer?.value ?? null,
        rt_average: f.average?.value ?? null,
        fetched_at: stamp,
      });
    }
  }

  let written = 0;
  let failedRows = 0;
  for (let i = 0; i < rows.length; i += UPSERT_BATCH) {
    const chunk = rows.slice(i, i + UPSERT_BATCH);
    const { error } = await supabase.from('critic_scores').upsert(chunk, { onConflict: 'movie_id' });
    if (error) {
      failedRows += chunk.length;
      console.error(`Writing rows ${i}-${i + chunk.length} failed: ${error.message}`);
    } else {
      written += chunk.length;
    }
  }

  const count = (pick: (f: Found) => unknown) => [...found.values()].filter(pick).length;
  console.log('\nDetermination methods seen (statements):');
  for (const [label, n] of [...methods.entries()].sort((a, b) => b[1] - a[1])) console.log(`  ${n}\t${label}`);
  console.log(
    `\nFound: ${count((f) => f.metacritic)} Metascores, ${count((f) => f.average)} Rotten Tomatoes averages, ` +
      `${count((f) => f.tomatometer)} Tomatometers, across ${rows.length} films.`,
  );

  // Logged next to the other pipeline jobs. Best-effort: a logging failure
  // must not fail a run that otherwise worked.
  const total = Math.ceil(imdbIds.length / BATCH);
  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'critic_scores_refresh',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: written,
    rows_failed: failedRows,
    status: (total > 0 && failedBatches === total) || (rows.length > 0 && written === 0) ? 'failed' : 'success',
  });
  if (logError) console.error(`pipeline_runs logging failed (non-fatal): ${logError.message}`);

  console.log(`\nDone. ${written} written, ${failedRows} failed, ${failedBatches} of ${total} queries failed.`);
  if (total > 0 && failedBatches === total) process.exit(1);
}

// Run when called as a script, not when imported (the parser is tested on
// its own).
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}
