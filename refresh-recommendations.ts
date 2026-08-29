// Refreshes all 5 recommendation rails (movie_neighbors) across the
// full catalog by calling the compute_* Postgres functions in chunks,
// adaptively sized -- ported directly from refresh-new-recommendations.ts,
// which already solved this same timeout problem for the incremental
// case. A fixed chunk size was tried twice here (250, then 100) and
// both failed on chunk 0: this script processes movies highest-vote_count
// first, which is also the most expensive case (broad genre/keyword
// overlap with much of the rest of the catalog), and the incremental
// script needed to go as low as 20 even on arbitrary, not
// worst-case-first, movies. A fixed number was never going to hold up
// here; halve-on-timeout and skip-at-floor does.
//
// Order matters and is fixed: closest_match, same_mood, darker_pick,
// more_accessible, hidden_gem -- each rail excludes picks already used
// by the ones before it (see the functions' v_excl logic), so running
// them out of order would break that.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const TOP_K = 10;
const NET_SIZE = 300;
const INITIAL_CHUNK_SIZE = 50;
const MIN_CHUNK_SIZE = 10;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  db: { schema: 'public' },
});

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

// Same ordering the SQL functions used to paginate by internally
// (vote_count desc, id) -- now fetched fully up front so the whole list
// can be sliced into adaptively-sized chunks by id, same as the
// incremental script does.
async function fetchOrderedMovieIds(): Promise<string[]> {
  const all: string[] = [];
  const PAGE_SIZE = 1000;
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from('movies')
      .select('id')
      .not('essence_vector', 'is', null)
      .order('vote_count', { ascending: false })
      .order('id', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    all.push(...(page as any[]).map((r) => r.id));
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return all;
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

async function runRail(rail: RailConfig, orderedIds: string[]) {
  console.log(`\n=== ${rail.name} (${orderedIds.length} movies, starting chunks of ${INITIAL_CHUNK_SIZE}) ===`);
  let totalProcessed = 0;
  for (let i = 0; i < orderedIds.length; i += INITIAL_CHUNK_SIZE) {
    const chunk = orderedIds.slice(i, i + INITIAL_CHUNK_SIZE);
    const processed = await processChunk(rail, chunk);
    totalProcessed += processed;
    console.log(`  chunk starting at ${i}: +${processed} (total ${totalProcessed}/${orderedIds.length})`);
  }
  console.log(`${rail.name} done: ${totalProcessed} processed.`);
}

async function main() {
  console.log('Fetching ordered movie list...');
  const orderedIds = await fetchOrderedMovieIds();
  console.log(`${orderedIds.length} scored movies to process per rail.`);

  for (const rail of RAILS) {
    await runRail(rail, orderedIds);
  }

  console.log('\nAll rails refreshed.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
