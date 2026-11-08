// Critics' scores for the catalogue's films and series, from Wikidata.
//
// The Cinnamon score's only measure of how good a title is used to be its
// TMDB user rating, and TMDB's users rate like fans: Avengers: Infinity War
// (8.2) and Sound of Freedom (8.0) outrated The Lighthouse (7.5), and the
// Cinnamon Hundred filled up with crowd favourites. The television Hundred
// had the same problem, with Ghost Whisperer, Gossip Girl and Redo of
// Healer in it. Critics are the other half of "is it brilliant", and
// Wikidata carries their scores openly (CC0): Metacritic's Metascore, and
// Rotten Tomatoes' Tomatometer and critics' average rating, as
// review-score statements (P444) qualified by who gave them (P447).
// Titles are matched on their IMDb id (P345).
//
// Only critics' scores are kept. A statement whose determination method
// (P459) names an audience or user score is skipped, as are deprecated
// statements and any value whose shape does not fit its source (Metascore
// "NN/100", Tomatometer "NN%", average "N.N/10"). Where a source has
// several values over time, the latest by point in time (P585) wins.
//
// Series are scored season by season on both sites, and Wikidata keeps
// them that way: on the season's own item (a television series season,
// part of the series by P179), or on the series qualified by the season
// (P4908). A series' score is the mean of its seasons', the way both sites
// sum a series up; a value for the series as a whole counts only when no
// season has one.
//
// Upserts critic_scores by movie_id and tv_critic_scores by tv_id. A title
// Wikidata has nothing on gets no row; a title whose scores vanish from
// Wikidata keeps its last ones. Series run first: they are the shorter
// pass, and a failure in one pass does not stop the other.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { pathToFileURL } from 'node:url';
import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import { API_STATUSES, GATEWAY_STATUSES, resilientFetch } from './resilient-fetch';

// Below these a title almost never has a Metascore or a Tomatometer, and
// nothing that ranks by the Cinnamon score reaches that far down.
const VOTE_FLOOR = 50;
const TV_VOTE_FLOOR = 40;
// IMDb ids per query. Sent as a GET (so a failed one can safely be sent
// again), and 150 keeps the address well inside what the service accepts.
// Series ask for their seasons too, so fewer per query.
const BATCH = 150;
const TV_BATCH = 50;
// The query service asks clients to go gently: one query at a time, with
// a pause between.
const PAUSE_MS = 1000;
// The series pass gives up at this point and writes what it has, so the
// film pass always gets its turn inside the job's time limit.
const TV_TIME_LIMIT_MS = 20 * 60 * 1000;
// The service stops a query at 60 seconds; this leaves room for one more
// try, and stops a request that never answers from holding up the run.
const ASK_TIMEOUT_MS = 130 * 1000;
const UPSERT_BATCH = 500;
const PAGE = 1000;

const ENDPOINT = 'https://query.wikidata.org/sparql';
// Wikimedia asks every client to say who it is.
const USER_AGENT = 'CinnamonDB-pipeline/1.0 (https://www.cinnamondb.com)';

export const METACRITIC = 'Q150248';
export const ROTTEN_TOMATOES = 'Q105584';
// "television series season"
export const SEASON = 'Q3464665';

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

export interface TvBinding extends Binding {
  // The statement, so one that comes back twice is counted once.
  st?: { value: string };
  // The season a value belongs to; absent for the series as a whole.
  part?: { value: string };
}

interface Pick {
  value: number;
  // ISO date of the value, or '' when the statement has none.
  date: string;
}

type Kind = 'metacritic' | 'tomatometer' | 'average';

export interface Found {
  wikidata: string | null;
  metacritic: Pick | null;
  tomatometer: Pick | null;
  average: Pick | null;
}

export interface TvFound {
  wikidata: string | null;
  // How many seasons the most complete of the three scores was averaged
  // over; 0 when it is a value for the series as a whole.
  seasons: number;
  metacritic: number | null;
  tomatometer: number | null;
  average: number | null;
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

// Which of the three scores a statement is, and its value; null when it is
// none of them or its value does not fit its source.
export function reading(by: string, raw: string): { kind: Kind; value: number } | null {
  if (by === METACRITIC) {
    const m = raw.match(/^(\d{1,3})(?:\s*\/\s*100)?$/);
    const v = m ? Number(m[1]) : NaN;
    return v >= 0 && v <= 100 ? { kind: 'metacritic', value: v } : null;
  }
  if (by === ROTTEN_TOMATOES) {
    const pct = raw.match(/^(\d{1,3})\s*%$/);
    if (pct) {
      const v = Number(pct[1]);
      return v >= 0 && v <= 100 ? { kind: 'tomatometer', value: v } : null;
    }
    const avg = raw.match(/^(\d{1,2}(?:\.\d+)?)\s*\/\s*10$/);
    if (avg) {
      const v = Math.round(Number(avg[1]) * 10) / 10;
      return v >= 0 && v <= 10 ? { kind: 'average', value: v } : null;
    }
  }
  return null;
}

export function parse(bindings: Binding[]): Map<string, Found> {
  const out = new Map<string, Found>();
  for (const b of bindings) {
    const imdb = b.imdb?.value;
    const raw = b.value?.value?.trim();
    const by = b.by ? lastSegment(b.by.value) : '';
    if (!imdb || !raw || !by) continue;
    if (AUDIENCE.test(b.methodLabel?.value ?? '')) continue;

    const found: Found = out.get(imdb) ?? { wikidata: null, metacritic: null, tomatometer: null, average: null };
    if (!found.wikidata && b.item) found.wikidata = lastSegment(b.item.value);
    const r = reading(by, raw);
    if (r) found[r.kind] = better(found[r.kind], { value: r.value, date: b.date?.value ?? '' });
    out.set(imdb, found);
  }
  return out;
}

const round1 = (x: number) => Math.round(x * 10) / 10;

// The mean of a series' seasons, or its whole-series value when no season
// has one.
function seriesValue(byPart: Map<string, Pick>): { value: number | null; seasons: number } {
  const seasons = [...byPart.entries()].filter(([part]) => part !== '').map(([, p]) => p.value);
  if (seasons.length) return { value: round1(seasons.reduce((a, b) => a + b, 0) / seasons.length), seasons: seasons.length };
  const whole = byPart.get('');
  return { value: whole ? whole.value : null, seasons: 0 };
}

export function parseTv(bindings: TvBinding[]): Map<string, TvFound> {
  const acc = new Map<string, { wikidata: string | null; seen: Set<string>; parts: Record<Kind, Map<string, Pick>> }>();
  for (const b of bindings) {
    const imdb = b.imdb?.value;
    const raw = b.value?.value?.trim();
    const by = b.by ? lastSegment(b.by.value) : '';
    if (!imdb || !raw || !by) continue;
    if (AUDIENCE.test(b.methodLabel?.value ?? '')) continue;

    let entry = acc.get(imdb);
    if (!entry) {
      entry = { wikidata: null, seen: new Set(), parts: { metacritic: new Map(), tomatometer: new Map(), average: new Map() } };
      acc.set(imdb, entry);
    }
    if (!entry.wikidata && b.item) entry.wikidata = lastSegment(b.item.value);

    const r = reading(by, raw);
    if (!r) continue;
    // A statement qualified by both a season and a part comes back once
    // for each; it counts once.
    const st = b.st?.value;
    if (st) {
      if (entry.seen.has(st)) continue;
      entry.seen.add(st);
    }
    const part = b.part ? lastSegment(b.part.value) : '';
    const byPart = entry.parts[r.kind];
    byPart.set(part, better(byPart.get(part) ?? null, { value: r.value, date: b.date?.value ?? '' }));
  }

  const out = new Map<string, TvFound>();
  for (const [imdb, e] of acc) {
    const mc = seriesValue(e.parts.metacritic);
    const tm = seriesValue(e.parts.tomatometer);
    const avg = seriesValue(e.parts.average);
    out.set(imdb, {
      wikidata: e.wikidata,
      seasons: Math.max(mc.seasons, tm.seasons, avg.seasons),
      metacritic: mc.value,
      tomatometer: tm.value,
      average: avg.value,
    });
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

// Two questions per batch, each anchored on the series the way the film
// query is: its own review scores, then its seasons'. Asked as one query
// with a UNION, the first series pass had not written anything after half
// an hour: with the series' statements inside the UNION, the query service
// is free to gather every Metacritic and Rotten Tomatoes statement on
// Wikidata before it knows which series it wants. The written order is
// kept (hint:optimizer "None") so each question starts from the IMDb ids.
export function sparqlTvSeries(imdbIds: string[]): string {
  const values = imdbIds.map((id) => `"${id}"`).join(' ');
  return `SELECT ?imdb ?item ?part ?st ?value ?by ?methodLabel ?date WHERE {
  hint:Query hint:optimizer "None" .
  VALUES ?imdb { ${values} }
  ?item wdt:P345 ?imdb .
  ?item p:P444 ?st .
  ?st pq:P447 ?by .
  FILTER(?by IN (wd:${METACRITIC}, wd:${ROTTEN_TOMATOES}))
  ?st ps:P444 ?value ;
      wikibase:rank ?rank .
  FILTER(?rank != wikibase:DeprecatedRank)
  OPTIONAL { ?st pq:P4908|pq:P518 ?part . }
  OPTIONAL { ?st pq:P459 ?method . ?method rdfs:label ?methodLabel . FILTER(LANG(?methodLabel) = "en") }
  OPTIONAL { ?st pq:P585 ?date . }
}`;
}

export function sparqlTvSeasons(imdbIds: string[]): string {
  const values = imdbIds.map((id) => `"${id}"`).join(' ');
  return `SELECT ?imdb ?item ?part ?st ?value ?by ?methodLabel ?date WHERE {
  hint:Query hint:optimizer "None" .
  VALUES ?imdb { ${values} }
  ?item wdt:P345 ?imdb .
  ?part wdt:P179 ?item .
  ?part wdt:P31 wd:${SEASON} .
  ?part p:P444 ?st .
  ?st pq:P447 ?by .
  FILTER(?by IN (wd:${METACRITIC}, wd:${ROTTEN_TOMATOES}))
  ?st ps:P444 ?value ;
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

interface PassResult {
  written: number;
  failedRows: number;
  failedBatches: number;
  batches: number;
}

// Catalogue rows to look up, by IMDb id. Several rows can share one id.
async function idsByImdb(supabase: SupabaseClient, table: 'movies' | 'tv_shows', floor: number): Promise<Map<string, string[]>> {
  const byImdb = new Map<string, string[]>();
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await supabase
      .from(table)
      .select('id, imdb_id')
      .eq('scoring_status', 'scored')
      .gte('vote_count', floor)
      .not('imdb_id', 'is', null)
      .order('id', { ascending: true })
      .range(from, from + PAGE - 1);
    if (error) throw new Error(`Reading ${table} failed: ${error.message}`);
    const rows = (data ?? []) as { id: string; imdb_id: string }[];
    for (const r of rows) {
      if (!/^tt\d{5,10}$/.test(r.imdb_id)) continue;
      const ids = byImdb.get(r.imdb_id) ?? [];
      ids.push(r.id);
      byImdb.set(r.imdb_id, ids);
    }
    if (rows.length < PAGE) break;
  }
  return byImdb;
}

async function ask<B>(wikidataFetch: typeof fetch, query: string): Promise<B[]> {
  const url = `${ENDPOINT}?format=json&query=${encodeURIComponent(query)}`;
  const res = await wikidataFetch(url, {
    headers: { Accept: 'application/sparql-results+json', 'User-Agent': USER_AGENT },
    signal: AbortSignal.timeout(ASK_TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`answered ${res.status}`);
  const json = (await res.json()) as { results?: { bindings?: B[] } };
  return json.results?.bindings ?? [];
}

async function upsert(supabase: SupabaseClient, table: string, key: string, rows: Record<string, unknown>[]) {
  let written = 0;
  let failedRows = 0;
  for (let i = 0; i < rows.length; i += UPSERT_BATCH) {
    const chunk = rows.slice(i, i + UPSERT_BATCH);
    const { error } = await supabase.from(table).upsert(chunk, { onConflict: key });
    if (error) {
      failedRows += chunk.length;
      console.error(`Writing ${table} rows ${i}-${i + chunk.length} failed: ${error.message}`);
    } else {
      written += chunk.length;
    }
  }
  return { written, failedRows };
}

async function seriesPass(supabase: SupabaseClient, wikidataFetch: typeof fetch): Promise<PassResult> {
  const byImdb = await idsByImdb(supabase, 'tv_shows', TV_VOTE_FLOOR);
  const imdbIds = [...byImdb.keys()];
  console.log(`${imdbIds.length} series to look up on Wikidata.`);

  const found = new Map<string, TvFound>();
  let failedBatches = 0;
  const deadline = Date.now() + TV_TIME_LIMIT_MS;
  for (let i = 0; i < imdbIds.length; i += TV_BATCH) {
    if (Date.now() > deadline) {
      const left = Math.ceil((imdbIds.length - i) / TV_BATCH);
      failedBatches += left;
      console.error(`Series pass stopped at its time limit with ${left} queries not asked.`);
      break;
    }
    try {
      const batch = imdbIds.slice(i, i + TV_BATCH);
      const series = await ask<TvBinding>(wikidataFetch, sparqlTvSeries(batch));
      await sleep(PAUSE_MS);
      const seasons = await ask<TvBinding>(wikidataFetch, sparqlTvSeasons(batch));
      for (const [imdb, f] of parseTv([...series, ...seasons])) found.set(imdb, f);
    } catch (err) {
      failedBatches++;
      console.error(`Series batch at ${i} failed: ${(err as Error).message}`);
    }
    if ((i / TV_BATCH) % 20 === 0) {
      console.log(`Series: ${Math.min(i + TV_BATCH, imdbIds.length)}/${imdbIds.length}, ${found.size} with statements.`);
    }
    await sleep(PAUSE_MS);
  }

  const stamp = new Date().toISOString();
  const rows: Record<string, unknown>[] = [];
  for (const [imdb, f] of found) {
    if (f.metacritic == null && f.tomatometer == null && f.average == null) continue;
    for (const tvId of byImdb.get(imdb) ?? []) {
      rows.push({
        tv_id: tvId,
        imdb_id: imdb,
        wikidata_id: f.wikidata,
        metacritic: f.metacritic,
        rt_tomatometer: f.tomatometer,
        rt_average: f.average,
        seasons: f.seasons,
        fetched_at: stamp,
      });
    }
  }
  const { written, failedRows } = await upsert(supabase, 'tv_critic_scores', 'tv_id', rows);

  const count = (pick: (f: TvFound) => unknown) => [...found.values()].filter(pick).length;
  console.log(
    `Series found: ${count((f) => f.metacritic != null)} Metascores, ${count((f) => f.average != null)} Rotten Tomatoes averages, ` +
      `${count((f) => f.tomatometer != null)} Tomatometers (${count((f) => f.seasons > 0)} by season), across ${rows.length} series.`,
  );
  return { written, failedRows, failedBatches, batches: Math.ceil(imdbIds.length / TV_BATCH) };
}

async function filmPass(supabase: SupabaseClient, wikidataFetch: typeof fetch): Promise<PassResult> {
  const byImdb = await idsByImdb(supabase, 'movies', VOTE_FLOOR);
  const imdbIds = [...byImdb.keys()];
  console.log(`${imdbIds.length} films to look up on Wikidata.`);

  const found = new Map<string, Found>();
  const methods = new Map<string, number>();
  let failedBatches = 0;
  for (let i = 0; i < imdbIds.length; i += BATCH) {
    try {
      const bindings = await ask<Binding>(wikidataFetch, sparql(imdbIds.slice(i, i + BATCH)));
      for (const b of bindings) {
        const label = b.methodLabel?.value ?? '(none)';
        methods.set(label, (methods.get(label) ?? 0) + 1);
      }
      for (const [imdb, f] of parse(bindings)) found.set(imdb, f);
    } catch (err) {
      failedBatches++;
      console.error(`Film batch at ${i} failed: ${(err as Error).message}`);
    }
    if ((i / BATCH) % 20 === 0) {
      console.log(`Films: ${Math.min(i + BATCH, imdbIds.length)}/${imdbIds.length}, ${found.size} with statements.`);
    }
    await sleep(PAUSE_MS);
  }

  const stamp = new Date().toISOString();
  const rows: Record<string, unknown>[] = [];
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
  const { written, failedRows } = await upsert(supabase, 'critic_scores', 'movie_id', rows);

  const count = (pick: (f: Found) => unknown) => [...found.values()].filter(pick).length;
  console.log('Determination methods seen (film statements):');
  for (const [label, n] of [...methods.entries()].sort((a, b) => b[1] - a[1])) console.log(`  ${n}\t${label}`);
  console.log(
    `Films found: ${count((f) => f.metacritic)} Metascores, ${count((f) => f.average)} Rotten Tomatoes averages, ` +
      `${count((f) => f.tomatometer)} Tomatometers, across ${rows.length} films.`,
  );
  return { written, failedRows, failedBatches, batches: Math.ceil(imdbIds.length / BATCH) };
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_ROLE_KEY'), {
    global: { fetch: resilientFetch({ label: 'database', retryOn: GATEWAY_STATUSES }) },
  });
  // A query is a read, so it is sent again on a dropped connection, a rate
  // limit or a 5xx (see resilient-fetch.ts). A series query that ran out of
  // time on the service's side will run out again, so it gets three tries
  // rather than five.
  const filmFetch = resilientFetch({ label: 'Wikidata', retryOn: API_STATUSES, attempts: 5 });
  const seriesFetch = resilientFetch({ label: 'Wikidata', retryOn: API_STATUSES, attempts: 3 });

  const startedAt = new Date().toISOString();
  const passes: PassResult[] = [];
  let broken = false;
  const runs: [string, () => Promise<PassResult>][] = [
    ['Series', () => seriesPass(supabase, seriesFetch)],
    ['Films', () => filmPass(supabase, filmFetch)],
  ];
  for (const [name, run] of runs) {
    try {
      const r = await run();
      passes.push(r);
      console.log(`${name}: ${r.written} written, ${r.failedRows} failed, ${r.failedBatches} of ${r.batches} queries failed.\n`);
      if (r.batches > 0 && r.failedBatches === r.batches) broken = true;
    } catch (err) {
      broken = true;
      console.error(`${name} pass failed: ${(err as Error).message}\n`);
    }
  }

  const written = passes.reduce((n, r) => n + r.written, 0);
  const failedRows = passes.reduce((n, r) => n + r.failedRows, 0);

  // Logged next to the other pipeline jobs. Best-effort: a logging failure
  // must not fail a run that otherwise worked.
  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'critic_scores_refresh',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: written,
    rows_failed: failedRows,
    status: broken || (written === 0 && failedRows > 0) ? 'failed' : 'success',
  });
  if (logError) console.error(`pipeline_runs logging failed (non-fatal): ${logError.message}`);

  console.log(`Done. ${written} written, ${failedRows} failed.`);
  if (broken) process.exit(1);
}

// Run when called as a script, not when imported (the parsers are tested
// on their own).
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}
