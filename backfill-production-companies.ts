// One-time backfill: adds production_companies to existing movies that
// don't have it yet. Existing rows were hydrated before this field was
// captured (see ingest.ts) -- new movies get it automatically from here on.
// Safe to re-run or interrupt: only selects movies where the column is
// still empty, so a rerun just picks up wherever the last one stopped.
//
// Required env vars: TMDB_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=true, MOVIE_LIMIT=<n>

import { createClient } from '@supabase/supabase-js';

const TMDB_API_KEY = requireEnv('TMDB_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const CONCURRENCY = 8;
const PAGE_SIZE = 500;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

async function fetchProductionCompanies(tmdbId: number): Promise<string[] | null> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const res = await fetch(`https://api.themoviedb.org/3/movie/${tmdbId}`, {
      headers: { Authorization: `Bearer ${TMDB_API_KEY}`, accept: 'application/json' },
      signal: controller.signal,
    });
    if (!res.ok) {
      if (res.status !== 404) console.error(`TMDB ${res.status} for tmdb_id=${tmdbId}`);
      return null;
    }
    const d = await res.json();
    return (d.production_companies ?? []).map((c: any) => c.name);
  } catch (err) {
    const reason = err instanceof Error && err.name === 'AbortError' ? 'timeout' : err;
    console.error(`Fetch failed for tmdb_id=${tmdbId}:`, reason);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function main() {
  const dryRun = process.env.DRY_RUN === 'true';
  const movieLimit = process.env.MOVIE_LIMIT ? parseInt(process.env.MOVIE_LIMIT, 10) : undefined;

  console.log(
    `Starting production_companies backfill${dryRun ? ' (DRY RUN)' : ''}${movieLimit ? `, limit=${movieLimit}` : ''}...`,
  );

  let cursor: string | null = null;
  let processed = 0;
  let updated = 0;
  let failures = 0;

  outer: while (true) {
    // The column was added with `default '{}'::text[]`, so every
    // pre-existing row already reads back as an empty array, not null --
    // filter on that (an empty array literal), not .is(...null).
    let query = supabase
      .from('movies')
      .select('id, tmdb_id')
      .eq('production_companies', [])
      .not('tmdb_id', 'is', null)
      .order('id')
      .limit(PAGE_SIZE);

    if (cursor) query = query.gt('id', cursor);

    const { data: movies, error } = await query;
    if (error) {
      console.error('Failed to fetch movies batch:', error.message);
      break;
    }
    if (!movies || movies.length === 0) break;

    cursor = movies[movies.length - 1].id;

    for (let i = 0; i < movies.length; i += CONCURRENCY) {
      const chunk = movies.slice(i, i + CONCURRENCY);
      const results = await Promise.all(
        chunk.map(async (m) => {
          if (dryRun) return true;
          const companies = await fetchProductionCompanies(m.tmdb_id);
          if (companies === null) return false;
          const { error: updateError } = await supabase
            .from('movies')
            .update({ production_companies: companies })
            .eq('id', m.id);
          if (updateError) {
            console.error(`Update failed for movie ${m.id}:`, updateError.message);
            return false;
          }
          return true;
        }),
      );

      for (const ok of results) (ok ? updated++ : failures++);
      processed += chunk.length;

      if (processed % 200 === 0) {
        console.log(`Processed ${processed} movies (${updated} updated, ${failures} failures)...`);
      }

      if (movieLimit && processed >= movieLimit) break outer;
    }
  }

  console.log(`Done. Processed ${processed} movies. ${updated} updated, ${failures} failures.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
