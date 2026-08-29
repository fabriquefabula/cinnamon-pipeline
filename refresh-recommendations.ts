// Refreshes all 5 recommendation rails (movie_neighbors) across the
// full catalog by calling the compute_* Postgres functions in chunks --
// a single call across all ~45k movies times out (confirmed directly:
// even 500 movies per call exceeded the query interface's timeout), so
// this mirrors the resumable-batch design the SQL functions already
// have (after_vote_count/after_id) rather than fighting it.
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

// Re-measured directly against the live statement_timeout (2min):
// compute_closest_match on 250 movies took ~80s on its own, with no
// safety margin once real network/pooling overhead is added on top --
// which is exactly what the last run hit (timed out on chunk 0 at 250).
// The compute_* functions also now join movie_recurrence_penalty
// (added to dampen hub movies that were dominating every rail), which
// adds real cost per candidate. 100 gives roughly 3x margin at the
// measured per-movie rate instead of running right at the edge.
const CHUNK_SIZE = 100;
const TOP_K = 10;
const NET_SIZE = 300;

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

// Same ordering the SQL functions paginate by internally (vote_count
// desc, id) -- fetched independently here just to know each chunk's
// cursor boundary, not to duplicate the actual scoring work.
async function fetchOrderedMovieIds(): Promise<{ id: string; vote_count: number }[]> {
  const all: { id: string; vote_count: number }[] = [];
  const PAGE_SIZE = 1000;
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from('movies')
      .select('id, vote_count')
      .not('essence_vector', 'is', null)
      .order('vote_count', { ascending: false })
      .order('id', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    all.push(...(page as any[]));
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return all;
}

async function runRail(rail: RailConfig, orderedMovies: { id: string; vote_count: number }[]) {
  console.log(`\n=== ${rail.name} (${orderedMovies.length} movies, chunks of ${CHUNK_SIZE}) ===`);

  let cursorVoteCount: number | null = null;
  let cursorId: string | null = null;
  let totalProcessed = 0;
  let chunkIndex = 0;

  while (totalProcessed < orderedMovies.length) {
    const args: Record<string, any> = {
      top_k: TOP_K,
      net_size: NET_SIZE,
      movie_limit: CHUNK_SIZE,
      movie_offset: 0, // pagination is via after_vote_count/after_id, not offset -- offset stays 0
      after_vote_count: cursorVoteCount,
      after_id: cursorId,
      ...rail.extraArgs,
    };

    const { data, error } = await supabase.rpc(rail.rpcName, args);
    if (error) throw new Error(`${rail.rpcName} failed at chunk ${chunkIndex}: ${error.message}`);

    const processedThisChunk = data as number;
    if (processedThisChunk === 0) break; // nothing left to process

    totalProcessed += processedThisChunk;
    chunkIndex++;

    const nextCursorEntry = orderedMovies[totalProcessed - 1];
    if (!nextCursorEntry) break;
    cursorVoteCount = nextCursorEntry.vote_count;
    cursorId = nextCursorEntry.id;

    console.log(`  chunk ${chunkIndex}: +${processedThisChunk} (total ${totalProcessed}/${orderedMovies.length})`);
  }

  console.log(`${rail.name} done: ${totalProcessed} movies processed.`);
}

async function main() {
  console.log('Fetching ordered movie list for pagination cursors...');
  const orderedMovies = await fetchOrderedMovieIds();
  console.log(`${orderedMovies.length} scored movies to process per rail.`);

  for (const rail of RAILS) {
    await runRail(rail, orderedMovies);
  }

  console.log('\nAll rails refreshed.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
