// TV counterpart to refresh-new-recommendations.ts: computes all 5
// recommendation rails for shows that are scored but have no tv_neighbors
// rows yet -- newly scored arrivals from the weekly ingest -> submit ->
// collect chain, after assign-tv-clusters.ts has placed them.
//
// This closes the last gap in the TV chain. refresh-tv-recommendations.ts
// exists, but its scheduled trigger only RESUMES an in-progress full run
// and exits immediately when nothing is in progress -- by design, so a
// completed catalogue run can't be restarted by the next tick. That means
// nothing on a schedule ever picked up a newly scored show. The movie side
// has had this covered since refresh-new-recommendations.ts; TV did not.
//
// Order is fixed and matters: closest_match, same_mood, darker_pick,
// more_accessible, hidden_gem -- each excludes picks already used by the
// ones before it.
//
// Cluster assignment must already exist for these shows before this runs:
// cluster_score inside the rail functions reads tv_cluster_assignments, so
// a show with no assignment scores as if it belonged to nothing. The
// workflow runs assign-tv-clusters.ts first in the same job for exactly
// this reason.
//
// Deliberately passes NO tuning parameters -- not top_k, net_size,
// visibility_floor, vote_floor/ceiling or gap_threshold. Every one of those
// already has a TV-specific default baked into the compute_tv_* functions
// (visibility_floor 180 rather than the movie side's 250, vote_ceiling
// 2000 rather than 5000), and those defaults were tuned against the TV
// catalogue. Restating them here would fork the tuning: a later change to a
// function's default would silently not apply to newly ingested shows, and
// new shows would be railed on different parameters from the rest of the
// catalogue. Only the paging arguments and p_specific_ids are passed.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

// Same adaptive-chunking values as the movie side. A single chunk timeout
// must not abort the run: halve and retry, skip and log at the floor, keep
// going. That pattern is why the movie backlog stopped compounding.
const INITIAL_CHUNK_SIZE = 20;
const MIN_CHUNK_SIZE = 10;
const PAGE_SIZE = 1000;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

const RAILS: string[] = [
  'compute_tv_closest_match',
  'compute_tv_same_mood',
  'compute_tv_darker_pick',
  'compute_tv_more_accessible',
  'compute_tv_hidden_gem',
];

async function fetchShowsWithoutNeighbors(): Promise<string[]> {
  const covered = new Set<string>();
  let cfrom = 0;
  while (true) {
    const { data, error } = await supabase
      .from('tv_neighbors')
      .select('source_show_id')
      .range(cfrom, cfrom + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    for (const row of page as any[]) covered.add(row.source_show_id);
    if (page.length < PAGE_SIZE) break;
    cfrom += PAGE_SIZE;
  }

  // Filters on essence_vector_ext_z, not the base essence_vector. The z
  // vectors are derived by trigger, and a show can have essence_vector set
  // while its z vectors were never computed. Railing such a show crashes on
  // a NULL similarity score -- the exact failure that hit the movie
  // pipeline twice at the same offset.
  const missing: string[] = [];
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from('tv_shows')
      .select('id')
      .eq('scoring_status', 'scored')
      .not('essence_vector_ext_z', 'is', null)
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

// Recursively processes a chunk of show ids for one rail. On a timeout,
// halves the chunk and retries both halves instead of failing the whole
// run. At MIN_CHUNK_SIZE, logs and skips rather than retrying forever --
// a chunk that won't succeed even at the floor needs investigation, not
// an infinite retry loop.
async function processChunk(rpcName: string, ids: string[]): Promise<number> {
  const { data, error } = await supabase.rpc(rpcName, {
    show_limit: null,
    show_offset: 0,
    after_vote_count: null,
    after_id: null,
    p_specific_ids: ids,
  });

  if (!error) return data as number;

  if (isTimeoutError(error.message) && ids.length > MIN_CHUNK_SIZE) {
    const mid = Math.ceil(ids.length / 2);
    console.log(`  timeout on chunk of ${ids.length} -- splitting into ${mid} + ${ids.length - mid}`);
    const first = await processChunk(rpcName, ids.slice(0, mid));
    const second = await processChunk(rpcName, ids.slice(mid));
    return first + second;
  }

  if (isTimeoutError(error.message)) {
    console.log(`  SKIP: chunk of ${ids.length} still timing out at the floor -- ids: ${ids.slice(0, 3).join(', ')}...`);
    return 0;
  }

  throw new Error(`${rpcName} failed on a non-timeout error: ${error.message}`);
}

async function main() {
  console.log('Finding scored TV shows with no recommendation rails yet...');
  const showIds = await fetchShowsWithoutNeighbors();
  console.log(`${showIds.length} shows need recommendations.`);

  if (showIds.length === 0) {
    console.log('Nothing to do.');
    return;
  }

  for (const rpcName of RAILS) {
    console.log(`\n=== ${rpcName} (${showIds.length} shows, starting chunks of ${INITIAL_CHUNK_SIZE}) ===`);
    let totalProcessed = 0;
    for (let i = 0; i < showIds.length; i += INITIAL_CHUNK_SIZE) {
      const chunk = showIds.slice(i, i + INITIAL_CHUNK_SIZE);
      const processed = await processChunk(rpcName, chunk);
      totalProcessed += processed;
      console.log(`  chunk starting at ${i}: +${processed} (total ${totalProcessed}/${showIds.length})`);
    }
    console.log(`${rpcName} done: ${totalProcessed} processed.`);
  }

  console.log('\nDone.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
