// The TV counterpart to refresh-popularity.ts.
//
// Real bug this fixes: ingest-tv.ts skips any show already in the
// catalogue, and nothing else in the pipeline writes popularity, so a
// show's value is set once at first ingestion and never touched again.
// TMDB defines popularity as a same-day activity score, not a lasting
// metric, so the TV "Popular this week" row was ranking 10,746 scored
// shows by numbers frozen on the day each was ingested.
//
// It refreshes MORE than popularity, and that is the point. A series is
// not a finished object the way a film is: it gains episodes, ends, or
// comes back. last_air_date, in_production and the season/episode counts
// drift for exactly the same write-once reason -- and the TV row's
// "aired in the last 90 days" rule reads last_air_date. Refreshing
// popularity alone would have left that row slowly emptying itself as
// every frozen air date aged past the window, which is a worse failure
// than the one being fixed because it looks like there is simply
// nothing on.
//
// Scope: in production, or last aired within 18 months. The 18 is
// deliberately wider than the 12 the row needs -- last_air_date is
// itself one of the stale fields, so a tight scope would be deciding
// what to refresh using the very data it exists to correct, and a show
// that came back after a long gap could never re-enter the pool.
//
// Required env vars: TMDB_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const TMDB_API_KEY = requireEnv('TMDB_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const ACTIVE_MONTHS = 18;
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

interface ActiveShow {
  id: string;
  tmdb_id: number;
}

async function fetchActiveShows(): Promise<ActiveShow[]> {
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - ACTIVE_MONTHS);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  const shows: ActiveShow[] = [];
  const PAGE_SIZE = 1000;
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from('tv_shows')
      .select('id, tmdb_id')
      .eq('scoring_status', 'scored')
      .or(`in_production.is.true,last_air_date.gte.${cutoffStr}`)
      .order('id', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    shows.push(...(page as ActiveShow[]));
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return shows;
}

interface FreshValues {
  id: string;
  popularity: number | null;
  vote_count: number | null;
  vote_average: number | null;
  last_air_date: string | null;
  in_production: boolean | null;
  number_of_seasons: number | null;
  number_of_episodes: number | null;
}

async function fetchFreshValues(show: ActiveShow): Promise<FreshValues | null> {
  const d = await tmdbGet(`/tv/${show.tmdb_id}`);
  if (!d) return null;
  return {
    id: show.id,
    popularity: d.popularity ?? null,
    vote_count: d.vote_count ?? null,
    vote_average: d.vote_average ?? null,
    // TMDB sends '' rather than null for an unknown date, which Postgres
    // rejects for a date column -- so an empty string becomes null here
    // rather than failing the whole row's update.
    last_air_date: d.last_air_date || null,
    in_production: typeof d.in_production === 'boolean' ? d.in_production : null,
    number_of_seasons: d.number_of_seasons ?? null,
    number_of_episodes: d.number_of_episodes ?? null,
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
  const startedAt = new Date().toISOString();
  console.log(`Fetching shows in production or aired within ${ACTIVE_MONTHS} months...`);
  const shows = await fetchActiveShows();
  console.log(`${shows.length} shows to refresh.`);

  let updated = 0;
  let failed = 0;

  for (let i = 0; i < shows.length; i += UPDATE_BATCH_SIZE) {
    const batch = shows.slice(i, i + UPDATE_BATCH_SIZE);
    const fresh = await runWithConcurrency(batch, CONCURRENCY, fetchFreshValues);
    const stamp = new Date().toISOString();

    for (const row of fresh) {
      if (!row) {
        failed++;
        continue;
      }
      const { id, ...values } = row;
      const { error } = await supabase
        .from('tv_shows')
        .update({ ...values, popularity_refreshed_at: stamp })
        .eq('id', id);
      if (error) {
        console.error(`Update failed for ${id}: ${error.message}`);
        failed++;
      } else {
        updated++;
      }
    }
    console.log(
      `Progress: ${Math.min(i + UPDATE_BATCH_SIZE, shows.length)}/${shows.length}, ${updated} updated, ${failed} failed so far.`,
    );
  }

  // Best-effort: a logging failure must not fail a refresh that otherwise
  // succeeded. 'tv_popularity_refresh' is accepted by
  // pipeline_runs_run_type_check as of the migration that added this job.
  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'tv_popularity_refresh',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: updated,
    rows_failed: failed,
    status: failed > 0 && updated === 0 ? 'failed' : 'success',
  });
  if (logError) console.error(`pipeline_runs logging failed (non-fatal): ${logError.message}`);

  console.log(`\nDone. ${updated} updated, ${failed} failed.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
