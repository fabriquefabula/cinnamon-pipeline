// Daily sweep for titles that have not come out yet, across both
// catalogues.
//
// Why this cannot be a flag on the existing ingests: both select on
// vote_count >= 20, and a film nobody has seen has no votes. ingest.ts's
// recency carve-out (popularity.desc, primary_release_date.gte with no
// upper bound) does reach into the future, which is the only reason the
// catalogue had any upcoming films at all -- but its MIN_RECENT_POPULARITY
// floor of 20 excludes almost everything unreleased, and ingest-tv.ts's
// equivalent only looks at premieres that have already happened.
// Measured before writing this: 7 future-dated movies, 0 future-dated
// series.
//
// So the selection rule here is simply "dated in the next 180 days",
// with NO vote or popularity floor. For an unreleased title there is no
// honest quality signal to filter on -- votes and popularity are both
// near zero by definition -- and the clean-data gate (poster, overview,
// not adult) is what keeps the junk out instead.
//
// 180 days rather than the 90 the homepage row displays: the row would
// otherwise stand empty at its own boundary every time a release slips,
// and a title needs to already be in the catalogue on the day it enters
// the window.
//
// Deliberately does NOT import from ingest.ts. That module calls main()
// at the top level, so importing its exported isScoringEligible would
// start a full catalogue ingestion as a side effect. The rule is
// duplicated below instead; if it changes there, change it here.
//
// Required env vars: TMDB_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const TMDB_API_KEY = requireEnv('TMDB_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const HORIZON_DAYS = 180;
const CONCURRENCY = 20; // same as the other ingests -- well under TMDB's soft limit
const BATCH_SIZE = 100;
const SITE_VISIBLE_VOTES = 300; // kept in step with ingest.ts
const MIN_OVERVIEW_CHARS = 40; // kept in step with ingest-tv.ts
const MIN_DESCRIPTIVE_CHARS = 100;

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

function dateOffset(days: number): string {
  const d = new Date();
  d.setDate(d.getDate() + days);
  return d.toISOString().slice(0, 10);
}

async function discoverAll(buildPath: (page: number) => string): Promise<number[]> {
  const ids: number[] = [];
  const seen = new Set<number>();
  let page = 1;
  while (page <= 500) {
    const data = await tmdbGet(buildPath(page));
    if (!data?.results?.length) break;
    for (const r of data.results) {
      // /discover paginates against live data, so the same id can appear
      // on two pages as results shift underneath. De-duplicated here for
      // the same reason ingest-tv.ts does it.
      if (seen.has(r.id)) continue;
      seen.add(r.id);
      ids.push(r.id);
    }
    if (page >= data.total_pages) break;
    page++;
  }
  return ids;
}

async function existingIds(table: string, tmdbIds: number[]): Promise<Set<number>> {
  const found = new Set<number>();
  // Chunked: a single .in() with thousands of values makes a URL long
  // enough to be rejected before it reaches PostgREST.
  for (let i = 0; i < tmdbIds.length; i += 500) {
    const chunk = tmdbIds.slice(i, i + 500);
    const { data, error } = await supabase.from(table).select('tmdb_id').in('tmdb_id', chunk);
    if (error) throw error;
    for (const row of data ?? []) found.add(row.tmdb_id as number);
  }
  return found;
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

// ---------------------------------------------------------------- movies

function movieScoringEligible(input: {
  overview: string | null;
  genres: unknown[];
  keywords: unknown[];
  tagline: string | null;
  voteCount: number;
}): boolean {
  const overviewLen = (input.overview ?? '').trim().length;
  const nKeywords = input.keywords.length;
  const hasTagline = Boolean(input.tagline && input.tagline.trim().length > 0);
  if (input.genres.length === 0) return false;
  return (
    overviewLen >= 100 ||
    (overviewLen >= 40 && (nKeywords >= 5 || hasTagline)) ||
    input.voteCount >= SITE_VISIBLE_VOTES
  );
}

async function hydrateMovie(tmdbId: number): Promise<Record<string, any> | null> {
  const d = await tmdbGet(`/movie/${tmdbId}?append_to_response=keywords,credits`);
  if (!d) return null;
  if (d.adult) return null;
  if (!d.poster_path) return null;
  if (!d.overview || d.overview.trim().length === 0) return null;
  // An unreleased title with no date cannot be placed in an "upcoming"
  // row at all, which is the only thing this script exists to fill.
  if (!d.release_date) return null;

  const keywords: string[] = (d.keywords?.keywords ?? []).map((k: any) => k.name);
  const cast: string[] = (d.credits?.cast ?? []).slice(0, 10).map((c: any) => c.name);
  const directors: string[] = (d.credits?.crew ?? [])
    .filter((c: any) => c.job === 'Director')
    .map((c: any) => c.name);

  return {
    tmdb_id: d.id,
    title: d.title,
    original_title: d.original_title ?? null,
    overview: d.overview,
    release_date: d.release_date,
    release_year: Number(d.release_date.slice(0, 4)),
    runtime: d.runtime ?? null,
    budget: d.budget ?? null,
    revenue: d.revenue ?? null,
    genres: (d.genres ?? []).map((g: any) => g.name),
    poster_path: d.poster_path,
    poster_url: `https://image.tmdb.org/t/p/w500${d.poster_path}`,
    backdrop_path: d.backdrop_path ?? null,
    backdrop_url: d.backdrop_path ? `https://image.tmdb.org/t/p/w1280${d.backdrop_path}` : null,
    vote_average: d.vote_average ?? null,
    vote_count: d.vote_count ?? null,
    popularity: d.popularity ?? null,
    adult: false,
    original_language: d.original_language ?? null,
    imdb_id: d.imdb_id ?? null,
    keywords,
    top_cast: cast,
    directors,
    production_companies: (d.production_companies ?? []).map((c: any) => c.name),
    tagline: d.tagline || null,
    // Unreleased titles have no meaningful popularity rank, and this
    // column is an import-order artefact rather than a live signal.
    import_rank_popularity: 0,
    hydration_status: 'complete',
    scoring_eligible: movieScoringEligible({
      overview: d.overview,
      genres: d.genres ?? [],
      keywords,
      tagline: d.tagline ?? null,
      voteCount: d.vote_count ?? 0,
    }),
  };
}

// ------------------------------------------------------------------- tv

async function hydrateShow(tmdbId: number): Promise<Record<string, any> | null> {
  const d = await tmdbGet(`/tv/${tmdbId}?append_to_response=keywords,aggregate_credits,external_ids`);
  if (!d) return null;
  if (d.adult) return null;
  if (!d.poster_path) return null;
  if (!d.overview || d.overview.trim().length === 0) return null;
  if (!d.first_air_date) return null;

  // /tv returns keywords under `results`, not `keywords` as /movie does.
  // Reading the movie key here yields an empty array for every show and
  // fails the scoring gate silently -- see ingest-tv.ts.
  const keywords: string[] = (d.keywords?.results ?? []).map((k: any) => k.name);
  const cast: string[] = (d.aggregate_credits?.cast ?? []).slice(0, 10).map((c: any) => c.name);

  const overviewLen = d.overview.trim().length as number;
  const descriptiveLen = overviewLen + (d.tagline ?? '').trim().length + keywords.join(', ').length;

  return {
    tmdb_id: d.id,
    title: d.name,
    original_title: d.original_name ?? null,
    overview: d.overview,
    first_air_date: d.first_air_date,
    last_air_date: d.last_air_date || null,
    release_year: Number(d.first_air_date.slice(0, 4)),
    status: d.status ?? null,
    in_production: d.in_production ?? null,
    type: d.type ?? null,
    episode_run_time: d.episode_run_time ?? [],
    number_of_seasons: d.number_of_seasons ?? null,
    number_of_episodes: d.number_of_episodes ?? null,
    genres: (d.genres ?? []).map((g: any) => g.name),
    poster_path: d.poster_path,
    poster_url: `https://image.tmdb.org/t/p/w500${d.poster_path}`,
    backdrop_path: d.backdrop_path ?? null,
    backdrop_url: d.backdrop_path ? `https://image.tmdb.org/t/p/w1280${d.backdrop_path}` : null,
    vote_average: d.vote_average ?? null,
    vote_count: d.vote_count ?? null,
    popularity: d.popularity ?? null,
    adult: false,
    original_language: d.original_language ?? null,
    imdb_id: d.external_ids?.imdb_id ?? null,
    keywords,
    top_cast: cast,
    created_by: (d.created_by ?? []).map((c: any) => c.name),
    networks: (d.networks ?? []).map((n: any) => n.name),
    production_companies: (d.production_companies ?? []).map((c: any) => c.name),
    tagline: d.tagline || null,
    import_rank_popularity: 0,
    hydration_status: 'complete',
    scoring_eligible:
      (d.genres?.length ?? 0) > 0 &&
      (keywords.length >= 2 || Boolean(d.tagline)) &&
      overviewLen >= MIN_OVERVIEW_CHARS &&
      descriptiveLen >= MIN_DESCRIPTIVE_CHARS,
  };
}

// --------------------------------------------------------------- writing

// Fields that genuinely move between announcement and release. Anything
// touching scoring state is deliberately absent: a title already scored
// must not have its essence work invalidated because its poster changed,
// and scoring_eligible must not be re-decided from metadata that is
// still filling in.
const MOVIE_DRIFT = [
  'title', 'overview', 'tagline', 'release_date', 'release_year', 'runtime',
  'poster_path', 'poster_url', 'backdrop_path', 'backdrop_url',
  'popularity', 'vote_count', 'vote_average',
] as const;

const SHOW_DRIFT = [
  'title', 'overview', 'tagline', 'first_air_date', 'last_air_date', 'release_year',
  'status', 'in_production', 'number_of_seasons', 'number_of_episodes',
  'poster_path', 'poster_url', 'backdrop_path', 'backdrop_url',
  'popularity', 'vote_count', 'vote_average',
] as const;

async function writeRows(
  table: string,
  rows: Record<string, any>[],
  existing: Set<number>,
  driftFields: readonly string[],
): Promise<{ inserted: number; updated: number; failed: number }> {
  let inserted = 0;
  let updated = 0;
  let failed = 0;

  const fresh = rows.filter((r) => !existing.has(r.tmdb_id));
  const known = rows.filter((r) => existing.has(r.tmdb_id));

  for (let i = 0; i < fresh.length; i += BATCH_SIZE) {
    const batch = fresh.slice(i, i + BATCH_SIZE);
    const { error } = await supabase.from(table).upsert(batch, { onConflict: 'tmdb_id' });
    if (error) {
      // One bad row in a multi-row statement discards the whole batch, so
      // fall back to per-row rather than lose the rest -- the failure
      // mode ingest-tv.ts was bitten by on its first full run.
      console.error(`${table}: batch insert failed (${batch.length} rows): ${error.message}. Retrying row by row...`);
      for (const row of batch) {
        const { error: rowErr } = await supabase.from(table).upsert(row, { onConflict: 'tmdb_id' });
        if (rowErr) {
          failed++;
          console.error(`  ${table} tmdb_id=${row.tmdb_id} ("${row.title}") failed: ${rowErr.message}`);
        } else inserted++;
      }
    } else {
      inserted += batch.length;
    }
  }

  for (const row of known) {
    const patch: Record<string, any> = {};
    for (const f of driftFields) if (f in row) patch[f] = row[f];
    const { error } = await supabase.from(table).update(patch).eq('tmdb_id', row.tmdb_id);
    if (error) {
      failed++;
      console.error(`  ${table} tmdb_id=${row.tmdb_id} update failed: ${error.message}`);
    } else updated++;
  }

  return { inserted, updated, failed };
}

async function main() {
  const startedAt = new Date().toISOString();
  const from = dateOffset(0);
  const to = dateOffset(HORIZON_DAYS);
  console.log(`Upcoming window: ${from} to ${to}`);

  let processed = 0;
  let failedTotal = 0;

  // -- movies
  const movieIds = await discoverAll(
    (page) =>
      `/discover/movie?sort_by=popularity.desc&primary_release_date.gte=${from}` +
      `&primary_release_date.lte=${to}&page=${page}`,
  );
  console.log(`${movieIds.length} upcoming movie ids discovered.`);
  const existingMovies = await existingIds('movies', movieIds);

  for (let i = 0; i < movieIds.length; i += BATCH_SIZE) {
    const batch = movieIds.slice(i, i + BATCH_SIZE);
    const hydrated = (await runWithConcurrency(batch, CONCURRENCY, hydrateMovie)).filter(
      (r): r is Record<string, any> => r !== null,
    );
    const { inserted, updated, failed } = await writeRows('movies', hydrated, existingMovies, MOVIE_DRIFT);
    processed += inserted + updated;
    failedTotal += failed;
    console.log(
      `movies ${Math.min(i + BATCH_SIZE, movieIds.length)}/${movieIds.length}: +${inserted} new, ${updated} updated, ${failed} failed`,
    );
  }

  // -- tv
  const showIds = await discoverAll(
    (page) =>
      `/discover/tv?sort_by=popularity.desc&first_air_date.gte=${from}` +
      `&first_air_date.lte=${to}&page=${page}`,
  );
  console.log(`${showIds.length} upcoming series ids discovered.`);
  const existingShows = await existingIds('tv_shows', showIds);

  for (let i = 0; i < showIds.length; i += BATCH_SIZE) {
    const batch = showIds.slice(i, i + BATCH_SIZE);
    const hydrated = (await runWithConcurrency(batch, CONCURRENCY, hydrateShow)).filter(
      (r): r is Record<string, any> => r !== null,
    );
    const { inserted, updated, failed } = await writeRows('tv_shows', hydrated, existingShows, SHOW_DRIFT);
    processed += inserted + updated;
    failedTotal += failed;
    console.log(
      `tv ${Math.min(i + BATCH_SIZE, showIds.length)}/${showIds.length}: +${inserted} new, ${updated} updated, ${failed} failed`,
    );
  }

  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'upcoming_ingestion',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: processed,
    rows_failed: failedTotal,
    status: failedTotal > 0 && processed === 0 ? 'failed' : 'success',
  });
  if (logError) console.error(`pipeline_runs logging failed (non-fatal): ${logError.message}`);

  console.log(`\nDone. ${processed} rows written or updated, ${failedTotal} failed.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
