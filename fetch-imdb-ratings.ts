// IMDb ratings for the catalogue's series, from IMDb's own data files.
//
// The television Cinnamon score needs a measure of how good a series is,
// and neither source it had works for series. TMDB's ratings come from a
// small, fan-heavy crowd (Redo of Healer 8.3, Ghost Whisperer 8.1), and
// Wikidata has critics' scores for fewer than one series in fifty. IMDb
// publishes every title's rating and vote count daily in
// title.ratings.tsv.gz (tconst, averageRating, numVotes), from a far
// larger audience, for personal and non-commercial use. That licence is
// why the ratings stay out of the public API (see the tv_imdb_ratings
// migration): only percentiles made from them reach a page.
//
// Upserts tv_imdb_ratings by tv_id for every scored series with an IMDb id
// that IMDb rates. A series IMDb stops rating keeps its last row.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { pathToFileURL } from 'node:url';
import { gunzipSync } from 'node:zlib';
import { createClient } from '@supabase/supabase-js';
import { API_STATUSES, GATEWAY_STATUSES, resilientFetch } from './resilient-fetch';

const SOURCE = 'https://datasets.imdbws.com/title.ratings.tsv.gz';
const USER_AGENT = 'CinnamonDB-pipeline/1.0 (https://www.cinnamondb.com)';
// The file is about 8 MB; this only stops a download that never ends.
const DOWNLOAD_TIMEOUT_MS = 5 * 60 * 1000;
const PAGE = 1000;
const UPSERT_BATCH = 500;

export interface Rating {
  rating: number;
  votes: number;
}

// The rows of title.ratings.tsv for the ids asked for. The first line is
// the header; a line that does not read as an id, a rating from 1 to 10
// and a positive whole number of votes is skipped.
export function parseRatings(tsv: string, wanted: Set<string>): Map<string, Rating> {
  const out = new Map<string, Rating>();
  for (const line of tsv.split('\n').slice(1)) {
    const [tconst, avg, votes] = line.trim().split('\t');
    if (!tconst || !wanted.has(tconst)) continue;
    const rating = Number(avg);
    const count = Number(votes);
    if (!(rating >= 1 && rating <= 10) || !Number.isInteger(count) || count <= 0) continue;
    out.set(tconst, { rating, votes: count });
  }
  return out;
}

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_ROLE_KEY'), {
    global: { fetch: resilientFetch({ label: 'database', retryOn: GATEWAY_STATUSES }) },
  });
  const startedAt = new Date().toISOString();

  // Series to rate, by IMDb id. Several catalogue rows can share one id.
  const byImdb = new Map<string, string[]>();
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await supabase
      .from('tv_shows')
      .select('id, imdb_id')
      .eq('scoring_status', 'scored')
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
  console.log(`${byImdb.size} series to rate.`);

  // A download is a read, so it is sent again on a dropped connection, a
  // rate limit or a 5xx (see resilient-fetch.ts).
  const download = resilientFetch({ label: 'IMDb', retryOn: API_STATUSES, attempts: 4 });
  const res = await download(SOURCE, {
    headers: { 'User-Agent': USER_AGENT },
    signal: AbortSignal.timeout(DOWNLOAD_TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`IMDb answered ${res.status}`);
  const tsv = gunzipSync(Buffer.from(await res.arrayBuffer())).toString('utf8');
  const found = parseRatings(tsv, new Set(byImdb.keys()));
  console.log(`IMDb rates ${found.size} of them.`);

  const stamp = new Date().toISOString();
  const rows: Record<string, unknown>[] = [];
  for (const [imdb, r] of found) {
    for (const tvId of byImdb.get(imdb) ?? []) {
      rows.push({ tv_id: tvId, imdb_id: imdb, rating: r.rating, votes: r.votes, fetched_at: stamp });
    }
  }

  let written = 0;
  let failedRows = 0;
  for (let i = 0; i < rows.length; i += UPSERT_BATCH) {
    const chunk = rows.slice(i, i + UPSERT_BATCH);
    const { error } = await supabase.from('tv_imdb_ratings').upsert(chunk, { onConflict: 'tv_id' });
    if (error) {
      failedRows += chunk.length;
      console.error(`Writing rows ${i}-${i + chunk.length} failed: ${error.message}`);
    } else {
      written += chunk.length;
    }
  }

  // Logged next to the other pipeline jobs. Best-effort: a logging failure
  // must not fail a run that otherwise worked.
  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'imdb_ratings_refresh',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: written,
    rows_failed: failedRows,
    status: written === 0 && rows.length > 0 ? 'failed' : 'success',
  });
  if (logError) console.error(`pipeline_runs logging failed (non-fatal): ${logError.message}`);

  console.log(`Done. ${written} written, ${failedRows} failed.`);
  if (written === 0 && rows.length > 0) process.exit(1);
}

// Run when called as a script, not when imported (the parser is tested on
// its own).
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}
