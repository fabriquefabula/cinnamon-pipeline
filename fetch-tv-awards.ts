// Major awards and nominations for the catalogue's series, from Wikidata.
//
// Series have almost no critics' scores on record (Wikidata has them for
// fewer than one series in fifty), and an audience rating alone lets fan
// favourites in: anime and children's cartoons rate as highly on IMDb as
// The Sopranos. What critics and the industry think of a series shows up
// instead in what it has won and been nominated for. This counts, for
// each series, the award-received (P166) and nominated-for (P1411)
// statements on its Wikidata item whose award is one of the major
// television prizes: the Emmys, the Golden Globes, the BAFTAs, the
// Peabody, the critics' and the guild awards. Daytime, children's, sports
// and news Emmys are left out. Series are matched on their IMDb id (P345).
//
// Upserts tv_awards by tv_id for every scored series Wikidata knows. A
// series with no major awards gets a row of zeros, so the score can tell
// "none" from "not looked up".
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { pathToFileURL } from 'node:url';
import { createClient } from '@supabase/supabase-js';
import { API_STATUSES, GATEWAY_STATUSES, resilientFetch } from './resilient-fetch';

const TV_VOTE_FLOOR = 40;
const BATCH = 50;
const PAUSE_MS = 1000;
const ASK_TIMEOUT_MS = 130 * 1000;
const UPSERT_BATCH = 500;
const PAGE = 1000;

const ENDPOINT = 'https://query.wikidata.org/sparql';
const USER_AGENT = 'CinnamonDB-pipeline/1.0 (https://www.cinnamondb.com)';

// The prizes that count, by their English name on Wikidata.
const MAJOR =
  /\b(emmy|golden globe|bafta|british academy television|peabody|critics' choice|critics choice|television critics association|tca award|writers guild|directors guild|screen actors guild|producers guild|afi award|satellite award)/i;
// Emmys and other prizes that are not about prime-time series.
const MINOR = /daytime|children|kids|youth|sports emmy|news emmy|news and documentary|regional emmy|student/i;

export interface AwardBinding {
  imdb?: { value: string };
  item?: { value: string };
  st?: { value: string };
  awardLabel?: { value: string };
}

export interface Tally {
  wins: number;
  nominations: number;
}

export function isMajor(label: string): boolean {
  return MAJOR.test(label) && !MINOR.test(label);
}

// Statements per series, each counted once.
export function tally(wins: AwardBinding[], nominations: AwardBinding[]): Map<string, Tally> {
  const out = new Map<string, Tally>();
  const seen = new Set<string>();
  const add = (rows: AwardBinding[], key: keyof Tally) => {
    for (const b of rows) {
      const imdb = b.imdb?.value;
      const label = b.awardLabel?.value ?? '';
      const st = b.st?.value ?? '';
      if (!imdb || !st || !isMajor(label)) continue;
      if (seen.has(st)) continue;
      seen.add(st);
      const t = out.get(imdb) ?? { wins: 0, nominations: 0 };
      t[key] += 1;
      out.set(imdb, t);
    }
  };
  add(wins, 'wins');
  add(nominations, 'nominations');
  return out;
}

function query(property: 'P166' | 'P1411', imdbIds: string[]): string {
  const values = imdbIds.map((id) => `"${id}"`).join(' ');
  return `SELECT ?imdb ?item ?st ?awardLabel WHERE {
  hint:Query hint:optimizer "None" .
  VALUES ?imdb { ${values} }
  ?item wdt:P345 ?imdb .
  ?item p:${property} ?st .
  ?st ps:${property} ?award ;
      wikibase:rank ?rank .
  FILTER(?rank != wikibase:DeprecatedRank)
  ?award rdfs:label ?awardLabel .
  FILTER(LANG(?awardLabel) = "en")
}`;
}

export const winsQuery = (ids: string[]) => query('P166', ids);
export const nominationsQuery = (ids: string[]) => query('P1411', ids);

// Series Wikidata has an item for, so a series with no major awards can be
// told apart from one Wikidata does not know.
export function knownQuery(imdbIds: string[]): string {
  const values = imdbIds.map((id) => `"${id}"`).join(' ');
  return `SELECT ?imdb WHERE {
  VALUES ?imdb { ${values} }
  ?item wdt:P345 ?imdb .
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
  const wikidataFetch = resilientFetch({ label: 'Wikidata', retryOn: API_STATUSES, attempts: 3 });
  const startedAt = new Date().toISOString();

  const byImdb = new Map<string, string[]>();
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await supabase
      .from('tv_shows')
      .select('id, imdb_id')
      .eq('scoring_status', 'scored')
      .gte('vote_count', TV_VOTE_FLOOR)
      .not('imdb_id', 'is', null)
      .order('id', { ascending: true })
      .range(from, from + PAGE - 1);
    if (error) throw new Error(`Reading series failed: ${error.message}`);
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
  console.log(`${imdbIds.length} series to look up on Wikidata.`);

  const ask = async <B>(q: string): Promise<B[]> => {
    const res = await wikidataFetch(`${ENDPOINT}?format=json&query=${encodeURIComponent(q)}`, {
      headers: { Accept: 'application/sparql-results+json', 'User-Agent': USER_AGENT },
      signal: AbortSignal.timeout(ASK_TIMEOUT_MS),
    });
    if (!res.ok) throw new Error(`answered ${res.status}`);
    const json = (await res.json()) as { results?: { bindings?: B[] } };
    return json.results?.bindings ?? [];
  };

  const found = new Map<string, Tally>();
  const known = new Set<string>();
  let failed = 0;
  for (let i = 0; i < imdbIds.length; i += BATCH) {
    const batch = imdbIds.slice(i, i + BATCH);
    try {
      const ids = await ask<{ imdb?: { value: string } }>(knownQuery(batch));
      await sleep(PAUSE_MS);
      const wins = await ask<AwardBinding>(winsQuery(batch));
      await sleep(PAUSE_MS);
      const noms = await ask<AwardBinding>(nominationsQuery(batch));
      for (const b of ids) if (b.imdb?.value) known.add(b.imdb.value);
      for (const [imdb, t] of tally(wins, noms)) found.set(imdb, t);
    } catch (err) {
      failed++;
      console.error(`Batch at ${i} failed: ${(err as Error).message}`);
    }
    if ((i / BATCH) % 20 === 0) {
      console.log(`Series: ${Math.min(i + BATCH, imdbIds.length)}/${imdbIds.length}, ${found.size} with major awards.`);
    }
    await sleep(PAUSE_MS);
  }

  const stamp = new Date().toISOString();
  const rows: Record<string, unknown>[] = [];
  for (const imdb of known) {
    const t = found.get(imdb) ?? { wins: 0, nominations: 0 };
    for (const tvId of byImdb.get(imdb) ?? []) {
      rows.push({ tv_id: tvId, imdb_id: imdb, wins: t.wins, nominations: t.nominations, fetched_at: stamp });
    }
  }

  let written = 0;
  let failedRows = 0;
  for (let i = 0; i < rows.length; i += UPSERT_BATCH) {
    const chunk = rows.slice(i, i + UPSERT_BATCH);
    const { error } = await supabase.from('tv_awards').upsert(chunk, { onConflict: 'tv_id' });
    if (error) {
      failedRows += chunk.length;
      console.error(`Writing rows ${i}-${i + chunk.length} failed: ${error.message}`);
    } else {
      written += chunk.length;
    }
  }

  const total = Math.ceil(imdbIds.length / BATCH);
  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'tv_awards_refresh',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: written,
    rows_failed: failedRows,
    status: (total > 0 && failed === total) || (rows.length > 0 && written === 0) ? 'failed' : 'success',
  });
  if (logError) console.error(`pipeline_runs logging failed (non-fatal): ${logError.message}`);

  console.log(
    `Done. ${known.size} series on Wikidata, ${found.size} with major awards or nominations; ` +
      `${written} written, ${failedRows} failed, ${failed} of ${total} batches failed.`,
  );
  if (total > 0 && failed === total) process.exit(1);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}
