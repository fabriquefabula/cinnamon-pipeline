// One-time backfill: re-runs darker_pick, more_accessible, and
// hidden_gem for every movie whose results were computed before the
// overly-strict by_emotion-branch keyword requirement was fixed (see
// the Postgres migrations fix_hidden_gem_overly_strict_gates /
// fix_darker_lighter_overly_strict_gates). closest_match and same_mood
// never had this bug, so they're untouched here -- no reason to redo
// correct work.
//
// "Affected" = fewer than 10 results in ANY of the 3 fixed rails --
// checked directly against the live data before writing this (26,056
// movies matched), not assumed to be a small cleanup.
//
// This is a one-time cleanup, not an ongoing automation -- no scheduled
// workflow, run once by hand and this file can be deleted after.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const TOP_K = 10;
const NET_SIZE = 300;
const CHUNK_SIZE = 250;
const INCOMPLETE_THRESHOLD = 10; // fewer than this in a fixed rail = affected

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

// Order matters -- darker_pick excludes closest_match/same_mood's picks,
// more_accessible excludes those plus darker_pick's. Re-running out of
// order would compute against stale exclusion sets.
const RAILS: RailConfig[] = [
  { name: 'darker_pick', rpcName: 'compute_darker_pick', extraArgs: { visibility_floor: 250, gap_threshold: 15 } },
  { name: 'more_accessible', rpcName: 'compute_more_accessible', extraArgs: { visibility_floor: 250, gap_threshold: 15 } },
  { name: 'hidden_gem', rpcName: 'compute_hidden_gem', extraArgs: { vote_floor: 250, vote_ceiling: 5000 } },
];

async function fetchAffectedMovieIds(): Promise<string[]> {
  const counts = new Map<string, number>(); // "movieId:type" -> count
  const PAGE_SIZE = 1000;
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from('movie_neighbors')
      .select('source_movie_id, recommendation_type')
      .in('recommendation_type', ['darker_pick', 'more_accessible', 'hidden_gem'])
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    for (const row of page as any[]) {
      const key = `${row.source_movie_id}:${row.recommendation_type}`;
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }

  const perMovie = new Map<string, number>();
  for (const [key, n] of counts) {
    const movieId = key.split(':')[0];
    perMovie.set(movieId, Math.min(perMovie.get(movieId) ?? Infinity, n));
  }

  // Also catch scored movies missing from the tally entirely for any of
  // the 3 rails (0 rows) -- those are affected too, not just the ones
  // with 1-9.
  const allScored: { id: string }[] = [];
  let f = 0;
  while (true) {
    const { data, error } = await supabase
      .from('movies')
      .select('id')
      .eq('scoring_status', 'scored')
      .not('essence_vector', 'is', null)
      .order('id', { ascending: true })
      .range(f, f + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    allScored.push(...(page as any[]));
    if (page.length < PAGE_SIZE) break;
    f += PAGE_SIZE;
  }

  const affected: string[] = [];
  for (const row of allScored) {
    const min = perMovie.get(row.id) ?? 0;
    if (min < INCOMPLETE_THRESHOLD) affected.push(row.id);
  }
  return affected;
}

async function main() {
  console.log('Finding movies affected by the overly-strict gate bug...');
  const movieIds = await fetchAffectedMovieIds();
  console.log(`${movieIds.length} movies affected.`);

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
