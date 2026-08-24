// Refreshes popularity (and vote_count/vote_average, since they drift
// the same way) for movies released in the last 12 months -- the exact
// window POPULAR_THIS_WEEK (lib/collections.ts) ranks by.
//
// Real bug this fixes: ingest.ts explicitly filters OUT any movie
// already in the catalog before hydrating (toFetch = topIds.filter(r =>
// !existing.has(r.id))), so popularity is write-once -- set at first
// ingestion and never touched again by anything in the pipeline. TMDB's
// own definition (per ingest.ts's comment) is that popularity is a
// same-day activity score, not a lasting metric, so an existing movie's
// stored value becomes meaningless within days of ingestion. "Popular
// This Week" was ranking by a signal that, for most of the movies it
// could show, stopped updating months ago.
//
// Scoped to the last 12 months only (758 movies, checked directly) --
// older movies' current-week popularity doesn't matter for this
// feature, so there's no reason to hit TMDB's rate limit refreshing the
// full 44k+ catalog for a signal only this one feature uses.
//
// Required env vars: TMDB_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const TMDB_API_KEY = requireEnv('TMDB_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const RECENT_MONTHS = 12; // matches POPULAR_THIS_WEEK's own cutoff exactly
const CONCURRENCY = 20; // same as ingest.ts -- stays well under TMDB's rate limit
const UPDATE_BATCH_SIZE = 100;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

async function tmdbGet(path: string): Promise<any> {
  const res = await fetch(`https://api.themoviedb.org/3${path}`, {
    headers: { Authorization: `Bearer ${TMDB_API_KEY}` },
  });
  if (!res.ok) return null;
  return res.json();
}

interface RecentMovie {
  id: string;
  tmdb_id: number;
}

async function fetchRecentMovies(): Promise<RecentMovie[]> {
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - RECENT_MONTHS);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  const movies: RecentMovie[] = [];
  const PAGE_SIZE = 1000;
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from('movies')
      .select('id, tmdb_id')
      .eq('scoring_status', 'scored')
      .gte('release_date', cutoffStr)
      .order('id', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    movies.push(...(page as RecentMovie[]));
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return movies;
}

interface FreshValues {
  id: string;
  popularity: number | null;
  vote_count: number | null;
  vote_average: number | null;
}

async function fetchFreshValues(movie: RecentMovie): Promise<FreshValues | null> {
  const d = await tmdbGet(`/movie/${movie.tmdb_id}`);
  if (!d) return null;
  return {
    id: movie.id,
    popularity: d.popularity ?? null,
    vote_count: d.vote_count ?? null,
    vote_average: d.vote_average ?? null,
  };
}

async function runWithConcurrency<T, R>(
  items: T[],
  limit: number,
  worker: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let next = 0;
  async function runner() {
    while (next < items.length) {
      const i = next++;
      results[i] = await worker(items[i]);
    }
  }
  await Promise.all(Array.from({ length: limit }, runner));
  return results;
}

async function main() {
  console.log(`Fetching movies released in the last ${RECENT_MONTHS} months...`);
  const movies = await fetchRecentMovies();
  console.log(`${movies.length} movies to refresh.`);

  let updated = 0;
  let failed = 0;

  for (let i = 0; i < movies.length; i += UPDATE_BATCH_SIZE) {
    const batch = movies.slice(i, i + UPDATE_BATCH_SIZE);
    const fresh = await runWithConcurrency(batch, CONCURRENCY, fetchFreshValues);

    for (const row of fresh) {
      if (!row) {
        failed++;
        continue;
      }
      const { error } = await supabase
        .from('movies')
        .update({ popularity: row.popularity, vote_count: row.vote_count, vote_average: row.vote_average })
        .eq('id', row.id);
      if (error) {
        console.error(`Update failed for ${row.id}: ${error.message}`);
        failed++;
      } else {
        updated++;
      }
    }
    console.log(`Progress: ${Math.min(i + UPDATE_BATCH_SIZE, movies.length)}/${movies.length}, ${updated} updated, ${failed} failed so far.`);
  }

  console.log(`\nDone. ${updated} updated, ${failed} failed.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
