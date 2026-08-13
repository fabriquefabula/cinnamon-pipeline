import { createClient } from '@supabase/supabase-js';

const TMDB_API_KEY = process.env.TMDB_API_KEY!;
const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// Top-billed only -- TMDB's cast lists can run 50+ deep for some films,
// and only the top of the bill matters for an "Ensemble" section or for
// someone browsing a person's filmography from this site.
const CAST_LIMIT = 20;
const CONCURRENCY = 8;
const PAGE_SIZE = 500;

interface TmdbPerson {
  id: number;
  name: string;
  profile_path: string | null;
  known_for_department: string | null;
  popularity: number | null;
}

interface TmdbCastMember extends TmdbPerson {
  character: string;
  order: number;
}

interface TmdbCrewMember extends TmdbPerson {
  job: string;
  department: string;
}

async function fetchCredits(
  tmdbId: number,
): Promise<{ cast: TmdbCastMember[]; crew: TmdbCrewMember[] } | null> {
  try {
    const res = await fetch(`https://api.themoviedb.org/3/movie/${tmdbId}/credits`, {
      headers: {
        Authorization: `Bearer ${TMDB_API_KEY}`,
        accept: 'application/json',
      },
    });
    if (!res.ok) {
      if (res.status !== 404) console.error(`TMDB ${res.status} for tmdb_id=${tmdbId}`);
      return null;
    }
    return await res.json();
  } catch (err) {
    console.error(`Fetch failed for tmdb_id=${tmdbId}:`, err);
    return null;
  }
}

async function processMovie(movie: {
  id: string;
  tmdb_id: number;
}): Promise<{ cast: number; crew: number } | null> {
  const credits = await fetchCredits(movie.tmdb_id);
  if (!credits) return null;

  const topCast = credits.cast.slice(0, CAST_LIMIT);
  // Directors only from crew -- editors, sound mixers, etc. aren't useful
  // for this site and would bloat the table for no navigational value.
  const directors = credits.crew.filter((c) => c.job === 'Director');

  const people = new Map<number, TmdbPerson>();
  for (const p of [...topCast, ...directors]) people.set(p.id, p);
  if (people.size === 0) return { cast: 0, crew: 0 };

  const { data: upserted, error: upsertError } = await supabase
    .from('people')
    .upsert(
      Array.from(people.values()).map((p) => ({
        tmdb_person_id: p.id,
        name: p.name,
        profile_path: p.profile_path,
        profile_url: p.profile_path
          ? `https://image.tmdb.org/t/p/w185${p.profile_path}`
          : null,
        known_for_department: p.known_for_department,
        popularity: p.popularity,
        hydration_status: 'pending', // only name/profile/etc. populated here -- biography needs a separate /person/{id} pass
      })),
      { onConflict: 'tmdb_person_id' },
    )
    .select('id, tmdb_person_id');

  if (upsertError || !upserted) {
    console.error(`Person upsert failed for movie ${movie.id}:`, upsertError?.message);
    return null;
  }

  const personIdByTmdb = new Map(upserted.map((p) => [p.tmdb_person_id, p.id]));

  const creditRows = [
    ...topCast.map((c) => ({
      movie_id: movie.id,
      person_id: personIdByTmdb.get(c.id),
      credit_type: 'cast',
      character: c.character || null,
      billing_order: c.order,
    })),
    ...directors.map((d) => ({
      movie_id: movie.id,
      person_id: personIdByTmdb.get(d.id),
      credit_type: 'crew',
      job: d.job,
      department: d.department,
    })),
  ].filter((r) => r.person_id);

  if (creditRows.length > 0) {
    const { error: creditError } = await supabase.from('movie_credits').insert(creditRows);
    if (creditError) {
      console.error(`Credit insert failed for movie ${movie.id}:`, creditError.message);
      return null;
    }
  }

  return { cast: topCast.length, crew: directors.length };
}

// For resumability across runs/failures -- built once at start, not
// checked per-movie (which would double the number of round trips).
async function getAlreadyIngestedMovieIds(): Promise<Set<string>> {
  const ids = new Set<string>();
  let cursor: string | null = null;
  while (true) {
    let query = supabase.from('movie_credits').select('id, movie_id').order('id').limit(1000);
    if (cursor) query = query.gt('id', cursor);

    const { data, error } = await query;
    if (error || !data || data.length === 0) break;
    for (const row of data) ids.add(row.movie_id as string);
    cursor = data[data.length - 1].id as string;
    if (data.length < 1000) break;
  }
  return ids;
}

async function main() {
  const dryRun = process.env.DRY_RUN === 'true';
  const movieLimit = process.env.MOVIE_LIMIT ? parseInt(process.env.MOVIE_LIMIT, 10) : undefined;

  console.log(
    `Starting credits ingestion${dryRun ? ' (DRY RUN)' : ''}${movieLimit ? `, limit=${movieLimit}` : ''}...`,
  );

  console.log('Loading already-ingested movie IDs (for resumability)...');
  const alreadyDone = await getAlreadyIngestedMovieIds();
  console.log(`${alreadyDone.size} movies already have credits, will skip them.`);

  let cursor: string | null = null;
  let processed = 0;
  let castTotal = 0;
  let crewTotal = 0;
  let failures = 0;

  outer: while (true) {
    let query = supabase
      .from('movies')
      .select('id, tmdb_id')
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

    cursor = movies[movies.length - 1].id; // advance regardless of how many get skipped below
    const todo = movies.filter((m) => !alreadyDone.has(m.id));

    for (let i = 0; i < todo.length; i += CONCURRENCY) {
      const chunk = todo.slice(i, i + CONCURRENCY);
      const results = await Promise.all(
        chunk.map((m) => (dryRun ? Promise.resolve({ cast: 0, crew: 0 }) : processMovie(m))),
      );

      for (const r of results) {
        if (r) {
          castTotal += r.cast;
          crewTotal += r.crew;
        } else {
          failures++;
        }
      }

      processed += chunk.length;
      if (processed % 200 === 0) {
        console.log(
          `Processed ${processed} movies (${castTotal} cast credits, ${crewTotal} director credits, ${failures} failures)...`,
        );
      }

      if (movieLimit && processed >= movieLimit) break outer;
    }
  }

  console.log(
    `Done. Processed ${processed} movies. ${castTotal} cast credits, ${crewTotal} director credits, ${failures} failures.`,
  );
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
