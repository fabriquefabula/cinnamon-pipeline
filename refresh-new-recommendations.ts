// Incremental companion to refresh-recommendations.ts: computes the
// recommendation rails for scored movies that are missing any of them --
// newly-scored movies from the weekly ingest -> credits -> submit ->
// collect chain, and movies that only got part of their rails. Uses the
// p_specific_ids parameter added to all 5 compute_* functions
// specifically for this (backward compatible -- refresh-recommendations.ts
// never passes it and is unaffected).
//
// Order is fixed and matters: closest_match, same_mood, darker_pick,
// more_accessible, hidden_gem -- each excludes picks (and series) already
// used by the ones before it. So a movie is recomputed from its FIRST
// missing rail onward: the rails before that one are still valid, the
// ones after it were picked without knowing what it will hold.
//
// "Missing any rail", not "has no rows at all": the old test treated a
// movie with even one rail as done for good. A movie scored while a full
// refresh was part-way through gets only the rails that run after it --
// 548 movies scored on 2026-10-18 got hidden_gem only -- and a chunk
// skipped at the floor below left a movie with a gap that was never
// retried. films_missing_rails() returns every scored movie with a gap
// and where the gap starts.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const TOP_K = 10;
const NET_SIZE = 300;
// Was a fixed 250 with no retry -- a single chunk timeout threw
// immediately and killed the ENTIRE run, abandoning every other chunk
// even if they would have succeeded. That's the real reason the backlog
// compounded from 1,832 (noted when this script was written) to 7,415:
// every failed run made zero progress, and each week's newly-scored
// movies piled onto an already-stuck backlog. Adaptive chunking instead,
// same pattern already proven on backfill-relaxed-gate-rails.ts: halve
// and retry on timeout, skip and log at the floor, keep moving instead
// of aborting everything over one bad chunk.
//
// INITIAL_CHUNK_SIZE was 100 -- confirmed via a real run that this was
// still too high: every 100-chunk cascaded through 100->50->50->25->25
// (3 wasted timeout-and-split round trips) before finally succeeding,
// and even 25 occasionally still timed out. The broadened cluster_score
// added to darker_pick/more_accessible (checking keyword-group overlap
// via an extra unnest+join per candidate, not just tonal clusters) made
// every rail computation more expensive than when 100 was chosen. 20
// gives real margin below the observed failure threshold rather than
// splitting the difference.
const INITIAL_CHUNK_SIZE = 20;
const MIN_CHUNK_SIZE = 10;

// films_missing_rails() checks every scored movie against movie_neighbors
// (~50k movies x up to 5 index probes). Measured 18s cold, well inside the
// 60s statement timeout, but it is one query that cannot be split, so a
// timeout under load is retried after a pause rather than killing the run.
const DISCOVERY_MAX_ATTEMPTS = 4;
const DISCOVERY_BACKOFF_MS = 15000;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface RailConfig {
  name: string;
  rpcName: string;
  extraArgs: Record<string, number>;
}

// Position in this array + 1 is the first_missing value films_missing_rails()
// returns -- keep the two orders identical.
const RAILS: RailConfig[] = [
  { name: 'closest_match', rpcName: 'compute_closest_match', extraArgs: { visibility_floor: 250 } },
  { name: 'same_mood', rpcName: 'compute_same_mood', extraArgs: { visibility_floor: 250, top_n_dims: 5 } },
  { name: 'darker_pick', rpcName: 'compute_darker_pick', extraArgs: { visibility_floor: 250, gap_threshold: 15 } },
  { name: 'more_accessible', rpcName: 'compute_more_accessible', extraArgs: { visibility_floor: 250, gap_threshold: 15 } },
  { name: 'hidden_gem', rpcName: 'compute_hidden_gem', extraArgs: { vote_floor: 250, vote_ceiling: 5000 } },
];

interface MissingMovie {
  id: string;
  firstMissing: number; // 1-based position in RAILS
}

function isTimeoutError(message: string): boolean {
  return /statement timeout/i.test(message);
}

// Every scored movie (essence_vector_ext_z present -- a movie without it
// would crash the rails on a NULL similarity score) that is missing at
// least one rail. Paged because PostgREST caps a response at 1,000 rows;
// the function orders by id so the pages are stable.
async function fetchMoviesMissingRails(): Promise<MissingMovie[]> {
  const PAGE_SIZE = 1000;
  const out: MissingMovie[] = [];
  let from = 0;
  while (true) {
    let page: any[] | null = null;
    for (let attempt = 1; attempt <= DISCOVERY_MAX_ATTEMPTS; attempt++) {
      const { data, error } = await supabase.rpc('films_missing_rails').range(from, from + PAGE_SIZE - 1);
      if (!error) {
        page = data ?? [];
        break;
      }
      if (!isTimeoutError(error.message) || attempt === DISCOVERY_MAX_ATTEMPTS) throw error;
      const waitMs = DISCOVERY_BACKOFF_MS * attempt;
      console.log(`  discovery query timed out (attempt ${attempt}/${DISCOVERY_MAX_ATTEMPTS}) -- retrying in ${waitMs / 1000}s`);
      await sleep(waitMs);
    }
    const rows = page ?? [];
    for (const row of rows) {
      const firstMissing = Number(row.first_missing);
      if (firstMissing >= 1 && firstMissing <= RAILS.length) out.push({ id: row.id, firstMissing });
    }
    if (rows.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return out;
}

// Recursively processes a chunk of movie ids for one rail. On a timeout,
// halves the chunk and retries both halves instead of failing the whole
// run. At MIN_CHUNK_SIZE, logs and skips rather than retrying forever --
// a skipped movie keeps its gap and is picked up again by the next run.
async function processChunk(rail: RailConfig, ids: string[]): Promise<number> {
  const { data, error } = await supabase.rpc(rail.rpcName, {
    top_k: TOP_K,
    net_size: NET_SIZE,
    movie_limit: null,
    movie_offset: 0,
    after_vote_count: null,
    after_id: null,
    p_specific_ids: ids,
    ...rail.extraArgs,
  });

  if (!error) return data as number;

  if (isTimeoutError(error.message) && ids.length > MIN_CHUNK_SIZE) {
    const mid = Math.ceil(ids.length / 2);
    console.log(`  timeout on chunk of ${ids.length} -- splitting into ${mid} + ${ids.length - mid}`);
    const first = await processChunk(rail, ids.slice(0, mid));
    const second = await processChunk(rail, ids.slice(mid));
    return first + second;
  }

  if (isTimeoutError(error.message)) {
    console.log(`  SKIP: chunk of ${ids.length} still timing out at the floor -- ids: ${ids.slice(0, 3).join(', ')}...`);
    return 0;
  }

  throw new Error(`${rail.rpcName} failed on a non-timeout error: ${error.message}`);
}

async function main() {
  console.log('Finding scored movies missing recommendation rails...');
  const missing = await fetchMoviesMissingRails();
  const byStart = RAILS.map((r, i) => `${r.name} ${missing.filter((m) => m.firstMissing === i + 1).length}`);
  console.log(`${missing.length} movies need recommendations (first missing rail: ${byStart.join(', ')}).`);

  if (missing.length === 0) {
    console.log('Nothing to do.');
    return;
  }

  for (let r = 0; r < RAILS.length; r++) {
    const rail = RAILS[r];
    // A movie is recomputed from its first missing rail onward.
    const movieIds = missing.filter((m) => m.firstMissing <= r + 1).map((m) => m.id);
    if (movieIds.length === 0) {
      console.log(`\n=== ${rail.name}: no movies need it ===`);
      continue;
    }
    console.log(`\n=== ${rail.name} (${movieIds.length} movies, starting chunks of ${INITIAL_CHUNK_SIZE}) ===`);
    let totalProcessed = 0;
    for (let i = 0; i < movieIds.length; i += INITIAL_CHUNK_SIZE) {
      const chunk = movieIds.slice(i, i + INITIAL_CHUNK_SIZE);
      const processed = await processChunk(rail, chunk);
      totalProcessed += processed;
      console.log(`  chunk starting at ${i}: +${processed} (total ${totalProcessed}/${movieIds.length})`);
    }
    console.log(`${rail.name} done: ${totalProcessed} processed.`);
  }

  console.log('\nDone.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
