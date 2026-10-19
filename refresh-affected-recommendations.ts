// Companion to refresh-new-recommendations.ts. That script gives newly
// -scored movies their OWN 5 rails. This script handles the other half
// of keeping the catalogue current without a full recompute: existing
// movies that a newly recommendable movie should now appear next to.
//
// Nearest-neighbour lists are stored, not computed live -- a movie has no
// way to "appear" in an existing movie's top 10 until that existing
// movie's rails are recomputed. Without this script, everything already
// in the catalogue would slowly go stale relative to what arrives.
//
// WHAT COUNTS AS AN ARRIVAL. A movie arrives once: when it is scored, has
// all five rails and has 250+ votes, the floor every rail uses for its
// picks (rail_new_arrivals; rail_arrivals_seen remembers which have been
// handled). That covers both a newly ingested film and -- far more often
// -- one already here whose votes have just passed the floor.
//
// This used to look for movies with no rails at all, which is how
// refresh-new-recommendations.ts defined "new". But that step runs first
// and gives every new movie its rails, so by the time this ran there were
// never any, and it rebuilt nothing on any day it ran after a successful
// first step.
//
// For each arrival, movies_affected_by() finds the existing movies it is
// nearest to by feel and by keywords, genre-gated. Those movies get all
// five rails recomputed, in order, with the same settings as the full
// refresh (net 300 -- this used 100, which would have rebuilt lists from a
// narrower pool than the one they were built from). Only once that has
// run are the arrivals marked as handled, so a failed run retries them.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const TOP_K = 10;
const NET_SIZE = 300;
// Same starting point as refresh-new-recommendations.ts settled on after
// real timeout data.
const INITIAL_CHUNK_SIZE = 20;
const MIN_CHUNK_SIZE = 10;
// How many nearest existing movies to check per arrival, per branch
// (feel and keywords). A "could this arrival plausibly matter to you"
// screen, not a final ranking, so it can be narrower than a rail's pool.
const NEIGHBOR_CHECK_SIZE = 50;
// Arrivals handled per run. Each brings up to ~100 existing movies to
// rebuild; ten keeps a run well inside the workflow's time limit, and
// any left over are handled on the following days, best-known first.
const ARRIVALS_PER_RUN = 10;

// Retry budget for the per-arrival screen, which is one query that cannot
// be split. Measured at ~10s per arrival warm.
const DISCOVERY_MAX_ATTEMPTS = 3;
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

const RAILS: RailConfig[] = [
  { name: 'closest_match', rpcName: 'compute_closest_match', extraArgs: { visibility_floor: 250 } },
  { name: 'same_mood', rpcName: 'compute_same_mood', extraArgs: { visibility_floor: 250, top_n_dims: 5 } },
  { name: 'darker_pick', rpcName: 'compute_darker_pick', extraArgs: { visibility_floor: 250, gap_threshold: 15 } },
  { name: 'more_accessible', rpcName: 'compute_more_accessible', extraArgs: { visibility_floor: 250, gap_threshold: 15 } },
  { name: 'hidden_gem', rpcName: 'compute_hidden_gem', extraArgs: { vote_floor: 250, vote_ceiling: 5000 } },
];

function isTimeoutError(message: string): boolean {
  return /statement timeout/i.test(message);
}

async function fetchArrivals(): Promise<string[]> {
  const { data, error } = await supabase.rpc('rail_new_arrivals', { p_limit: ARRIVALS_PER_RUN });
  if (error) throw error;
  return (data ?? []).map((row: any) => row.movie_id as string);
}

// One arrival at a time: the screen is an exact nearest-neighbour search
// with a genre gate, ~10s each, and several in one call would pass the
// statement timeout.
async function fetchAffected(arrivalId: string): Promise<string[]> {
  for (let attempt = 1; attempt <= DISCOVERY_MAX_ATTEMPTS; attempt++) {
    const { data, error } = await supabase.rpc('movies_affected_by', {
      p_ids: [arrivalId],
      p_neighbor_check_size: NEIGHBOR_CHECK_SIZE,
    });
    if (!error) return (data ?? []).map((row: any) => row.movie_id as string);
    if (!isTimeoutError(error.message) || attempt === DISCOVERY_MAX_ATTEMPTS) throw error;
    const waitMs = DISCOVERY_BACKOFF_MS * attempt;
    console.log(`  screen timed out for ${arrivalId} (attempt ${attempt}/${DISCOVERY_MAX_ATTEMPTS}) -- retrying in ${waitMs / 1000}s`);
    await sleep(waitMs);
  }
  throw new Error('movies_affected_by: exhausted retries');
}

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
  console.log('Finding newly recommendable movies...');
  const arrivals = await fetchArrivals();
  console.log(`${arrivals.length} arrival(s) to place (up to ${ARRIVALS_PER_RUN} per run).`);

  if (arrivals.length === 0) {
    console.log('Nothing to do.');
    return;
  }

  const affected = new Set<string>();
  for (const id of arrivals) {
    const near = await fetchAffected(id);
    for (const n of near) affected.add(n);
    console.log(`  ${id}: ${near.length} nearby movies (${affected.size} distinct so far)`);
  }
  const movieIds = [...affected];
  console.log(`${movieIds.length} existing movies to recheck.`);

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

  const { error } = await supabase.rpc('mark_rail_arrivals_seen', { p_ids: arrivals });
  if (error) throw error;
  console.log(`\nMarked ${arrivals.length} arrival(s) as placed. Done.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
