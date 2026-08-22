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
// ones before it. Chunked the same as refresh-recommendations.ts -- the
// backlog isn't always small (checked before assuming otherwise: 1,832
// movies were missing neighbors when this was written).
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const TOP_K = 10;
const NET_SIZE = 300;
const CHUNK_SIZE = 250; // same tested-safe size as refresh-recommendations.ts -- checked the real backlog before assuming this script could skip chunking (1,832 movies right now, well past a single safe call)

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

  // Bug fixed here: this query previously had no .range() at all, so it
  // silently capped at Supabase's default 1000-row limit against a
  // table with 1.7M+ rows -- almost every movie looked "uncovered" as a
  // result (44,499 flagged in a real run instead of the actual ~1,832),
  // and the run then failed trying to recompute the whole catalog.
  // Paginated properly now, matching the pattern the second query in
  // this function already used correctly -- should have been consistent
  // from the start.
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

async function main() {
  console.log('Finding scored movies with no recommendation rails yet...');
  const movieIds = await fetchMoviesWithoutNeighbors();
  console.log(`${movieIds.length} movies need recommendations.`);

  if (movieIds.length === 0) {
    console.log('Nothing to do.');
    return;
  }

  for (const rail of RAILS) {
    console.log(`\n=== ${rail.name} (${movieIds.length} movies, chunks of ${CHUNK_SIZE}) ===`);
    let totalProcessed = 0;
    for (let i = 0; i < movieIds.length; i += CHUNK_SIZE) {
      const chunk = movieIds.slice(i, i + CHUNK_SIZE);
      const { data, error } = await supabase.rpc(rail.rpcName, {
        top_k: TOP_K,
        net_size: NET_SIZE,
        movie_limit: null,
        movie_offset: 0,
        after_vote_count: null,
        after_id: null,
        p_specific_ids: chunk,
        ...rail.extraArgs,
      });
      if (error) throw new Error(`${rail.rpcName} failed on chunk starting at ${i}: ${error.message}`);
      totalProcessed += data as number;
      console.log(`  chunk ${Math.floor(i / CHUNK_SIZE) + 1}: +${data} (total ${totalProcessed}/${movieIds.length})`);
    }
    console.log(`${rail.name} done: ${totalProcessed} processed.`);
  }

  console.log('\nDone.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
