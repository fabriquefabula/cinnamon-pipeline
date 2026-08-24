// Incremental companion to refresh-recommendations.ts: computes all 5
// recommendation rails for movies that are scored but have no
// movie_neighbors rows yet -- newly-scored movies from the weekly
// ingest -> credits -> submit -> collect chain. Uses the p_specific_ids
// parameter added to all 5 compute_* functions specifically for this
// (backward compatible -- refresh-recommendations.ts never passes it and
// is unaffected).
//
// Order is fixed and matters: closest_match, same_mood, darker_pick,
// more_accessible, hidden_gem -- each excludes picks already used by the
// ones before it.
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
const INITIAL_CHUNK_SIZE = 100;
const MIN_CHUNK_SIZE = 10;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

interface RailConfig {
  name: string;
  rpcName: string;
  extraArgs: Record<string, number>;
}

const RAILS: RailConfig[] = [
  { name: 'closest_match', rpcName: 'compute_closest_match', extraArgs: { visibility_floor: 250 } },
  { name: 'same_mood', rpcName: 'compute_same_mood', extraArgs: { visibility_floor: 250, top_n_dims: 5 } },
  { name: 'darker_pick', rpcName: 'compute_darker_pick', extraArgs: { visibility_floor: 250, gap_threshold: 15 } },
  { name: 'more_accessible', rpcName: 'compute_more_accessible', extraArgs: { visibility_floor: 250, gap_threshold: 15 } },
  { name: 'hidden_gem', rpcName: 'compute_hidden_gem', extraArgs: { vote_floor: 250, vote_ceiling: 5000 } },
];

async function fetchMoviesWithoutNeighbors(): Promise<string[]> {
  const PAGE_SIZE = 1000;

  const covered = new Set<string>();
  let cfrom = 0;
  while (true) {
    const { data, error } = await supabase
      .from('movie_neighbors')
      .select('source_movie_id')
      .range(cfrom, cfrom + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    for (const row of page as any[]) covered.add(row.source_movie_id);
    if (page.length < PAGE_SIZE) break;
    cfrom += PAGE_SIZE;
  }

  const missing: string[] = [];
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from('movies')
      .select('id')
      .eq('scoring_status', 'scored')
      .not('essence_vector', 'is', null)
      .order('id', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    for (const row of page as any[]) {
      if (!covered.has(row.id)) missing.push(row.id);
    }
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return missing;
}

function isTimeoutError(message: string): boolean {
  return /statement timeout/i.test(message);
}

// Recursively processes a chunk of movie ids for one rail. On a timeout,
// halves the chunk and retries both halves instead of failing the whole
// run. At MIN_CHUNK_SIZE, logs and skips rather than retrying forever --
// a chunk that won't succeed even at the floor needs investigation, not
// an infinite retry loop.
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
  console.log('Finding scored movies with no recommendation rails yet...');
  const movieIds = await fetchMoviesWithoutNeighbors();
  console.log(`${movieIds.length} movies need recommendations.`);

  if (movieIds.length === 0) {
    console.log('Nothing to do.');
    return;
  }

  for (const rail of RAILS) {
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
