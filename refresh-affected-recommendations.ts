// Companion to refresh-new-recommendations.ts. That script gives newly
// -scored movies their OWN 5 rails. This script handles the other half
// of keeping the catalog current without a full recompute: existing,
// already-processed movies that a new movie might now belong in.
//
// Nearest-neighbor lists are stored, not computed live -- a new movie
// added this week has no way to "appear" in an existing movie's top 10
// until that existing movie's rails are recomputed. Without this
// script, only brand-new movies would ever get fresh recommendations;
// everything already in the catalog would slowly go stale relative to
// new arrivals, forever.
//
// "Which existing movies might be affected" is answered by
// find_existing_movies_affected_by_new(), an index-accelerated query
// (not a full scan): for each new movie (defined the same way
// refresh-new-recommendations.ts defines it -- no movie_neighbors rows
// yet), find its nearest neighbors among ALREADY-processed movies via
// the same dual-branch fingerprint + keyword lookup mood_pool itself
// uses, genre-gated. Run this AFTER refresh-new-recommendations.ts in
// the same weekly job -- doesn't strictly depend on it (this script
// only reads existing movies' embeddings, not their neighbor rows),
// but running new-movie rails first means a newly-added movie is fully
// ready before anything else's list might start pointing at it.
//
// Expected weekly scale: a few hundred to low thousands of movies
// (new arrivals plus their embedding-neighbors, deduplicated) -- a
// small fraction of the ~48k catalog, not a full recompute. Reuses the
// same adaptive-chunking pattern as refresh-new-recommendations.ts and
// refresh-recommendations.ts: halve and retry on timeout, skip and log
// at the floor rather than aborting the whole run over one bad chunk.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const TOP_K = 10;
const NET_SIZE = 100;
// Same starting point as refresh-new-recommendations.ts settled on
// after real timeout data -- no reason to assume this workload is
// cheaper per-movie, so starting at the same proven floor rather than
// re-discovering it here.
const INITIAL_CHUNK_SIZE = 20;
const MIN_CHUNK_SIZE = 10;
// How many nearest existing movies to check per new movie, per branch
// (fingerprint and keyword). Deliberately smaller than a rail's own
// net_size -- this is a "could this new movie plausibly matter to you"
// screen, not a final ranking, so it can afford to be narrower.
const NEIGHBOR_CHECK_SIZE = 50;

// Retry budget for the discovery query. See fetchAffectedMovieIds.
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

const RAILS: RailConfig[] = [
  { name: 'closest_match', rpcName: 'compute_closest_match', extraArgs: { visibility_floor: 250 } },
  { name: 'same_mood', rpcName: 'compute_same_mood', extraArgs: { visibility_floor: 250, top_n_dims: 5 } },
  { name: 'darker_pick', rpcName: 'compute_darker_pick', extraArgs: { visibility_floor: 250, gap_threshold: 15 } },
  { name: 'more_accessible', rpcName: 'compute_more_accessible', extraArgs: { visibility_floor: 250, gap_threshold: 15 } },
  { name: 'hidden_gem', rpcName: 'compute_hidden_gem', extraArgs: { vote_floor: 250, vote_ceiling: 5000 } },
];

// Retries on timeout instead of aborting the run. This was the ONLY
// database call in this file without timeout handling -- processChunk
// below has always halved-and-retried -- and a single timeout here
// killed the whole job.
//
// It is not chunkable (there is nothing to split: it's one discovery
// query), so the strategy is backoff rather than subdivision, and that
// specifically fits the measured failure. The query costs the same
// whether there is work to do or not: it always anti-joins all ~48.7k
// vector-bearing movies against the 2.4M-row movie_neighbors table,
// which is ~209,000 buffers, about 1.6GB of pages. Measured warm it
// runs in 444ms. It failed in production because this step runs
// immediately after refresh-new-recommendations spends ~30 minutes
// hammering the same database -- cold cache and contention pushed
// 444ms past the 8s statement timeout, on a run where it turned out
// there were zero new movies and therefore nothing to do at all.
//
// Since the first attempt warms exactly the pages the retry needs, a
// short backoff is very likely to succeed where an immediate retry of
// the identical query normally would not.
async function fetchAffectedMovieIds(): Promise<string[]> {
  for (let attempt = 1; attempt <= DISCOVERY_MAX_ATTEMPTS; attempt++) {
    const { data, error } = await supabase.rpc('find_existing_movies_affected_by_new', {
      p_neighbor_check_size: NEIGHBOR_CHECK_SIZE,
    });

    if (!error) return (data ?? []).map((row: any) => row.movie_id as string);

    if (!isTimeoutError(error.message) || attempt === DISCOVERY_MAX_ATTEMPTS) throw error;

    const waitMs = DISCOVERY_BACKOFF_MS * attempt;
    console.log(
      `  discovery query timed out (attempt ${attempt}/${DISCOVERY_MAX_ATTEMPTS}) -- retrying in ${waitMs / 1000}s`,
    );
    await sleep(waitMs);
  }
  // Unreachable: the loop either returns or throws on the final attempt.
  throw new Error('find_existing_movies_affected_by_new: exhausted retries');
}

function isTimeoutError(message: string): boolean {
  return /statement timeout/i.test(message);
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
  console.log('Finding existing movies that new additions might affect...');
  const movieIds = await fetchAffectedMovieIds();
  console.log(`${movieIds.length} existing movies to recheck.`);

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
