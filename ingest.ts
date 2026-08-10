// Weekly TMDB ingestion for Cinnamon DB.
//
// Selection is vote_count-based, not popularity-based (Aug 10 correction):
// TMDB's `popularity` field is a same-day activity score (views, ratings,
// watchlist adds "for the previous day"), not a lifetime-fame metric — an
// obscure title can spike it the same way a blockbuster can. `vote_count` is
// cumulative and requires sustained broad engagement to climb, which is a
// much harder signal to fake, so it's the actual "mainstream" proxy here.
//
// One carve-out: movies newer than RECENT_MONTHS haven't had time to
// accumulate votes, so they qualify via popularity instead, to avoid
// excluding brand-new mainstream releases just for being new.
//
// CATALOG_TARGET is a cap, not a quota — if fewer than that many movies
// clear MIN_VOTE_COUNT, the catalog will just be smaller. Check the log
// output on first run for the real number and adjust MIN_VOTE_COUNT if it's
// off from what "mainstream" should mean here.
//
// Required env vars: TMDB_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const TMDB_API_KEY = requireEnv('TMDB_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const CATALOG_TARGET = 250_000; // cap, not a quota — see note above
// Overridable per-run so threshold candidates can be probed via DRY_RUN
// without editing this file each time. Falls back to 300 if unset/empty.
const MIN_VOTE_COUNT = process.env.MIN_VOTE_COUNT ? parseInt(process.env.MIN_VOTE_COUNT, 10) : 300;
// When true: run discovery/counting only, log the result, and exit before
// touching Supabase at all (no pipeline_runs row, no hydration, no writes).
// For probing where vote_count stops being a meaningful mainstream signal.
const DRY_RUN = process.env.DRY_RUN === 'true';
const RECENT_MONTHS = 6;
const MIN_RECENT_POPULARITY = 20; // rough floor for "clearly mainstream buzz" on new titles — also untuned
const CONCURRENCY = 20; // stays well under TMDB's ~40-50 req/s soft limit
const INSERT_BATCH_SIZE = 500;
const EARLIEST_YEAR = 1900;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

interface DiscoverResult {
  id: number;
  vote_count: number;
  popularity: number;
  release_date: string;
}

async function tmdbGet(path: string): Promise<any> {
  const res = await fetch(`https://api.themoviedb.org/3${path}`, {
    headers: { Authorization: `Bearer ${TMDB_API_KEY}` },
  });
  if (!res.ok) return null;
  return res.json();
}

// TMDB caps any single query+filter combination at 500 pages (10,000
// results). Slicing by release year keeps each individual query's result
// count well under that cap, while still letting us collect every
// vote_count.gte-qualifying movie globally by combining all years' results
// ourselves rather than relying on any one query to rank everything.
async function discoverByVoteCount(year: number): Promise<DiscoverResult[]> {
  const results: DiscoverResult[] = [];
  let page = 1;
  while (page <= 500) {
    const data = await tmdbGet(
      `/discover/movie?sort_by=vote_count.desc&primary_release_year=${year}&vote_count.gte=${MIN_VOTE_COUNT}&page=${page}`,
    );
    if (!data?.results?.length) break;
    results.push(...data.results);
    if (page >= data.total_pages) break;
    page++;
  }
  return results;
}

async function discoverRecentByPopularity(): Promise<DiscoverResult[]> {
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - RECENT_MONTHS);
  const cutoffStr = cutoff.toISOString().slice(0, 10);
  const results: DiscoverResult[] = [];
  let page = 1;
  while (page <= 500) {
    const data = await tmdbGet(
      `/discover/movie?sort_by=popularity.desc&primary_release_date.gte=${cutoffStr}&page=${page}`,
    );
    if (!data?.results?.length) break;
    results.push(...data.results.filter((r: any) => r.popularity >= MIN_RECENT_POPULARITY));
    if (page >= data.total_pages) break;
    page++;
  }
  return results;
}

async function selectMainstreamIds(): Promise<{ id: number; rank: number }[]> {
  const currentYear = new Date().getFullYear();

  console.log(`Collecting recent releases (last ${RECENT_MONTHS} months) by popularity...`);
  const recent = await discoverRecentByPopularity();
  const recentIds = new Set(recent.map((r) => r.id));
  console.log(`${recent.length} recent releases qualify via the popularity carve-out.`);

  console.log(`Collecting established movies (vote_count >= ${MIN_VOTE_COUNT}), by year...`);
  const established: DiscoverResult[] = [];
  for (let year = EARLIEST_YEAR; year <= currentYear + 1; year++) {
    const yearResults = await discoverByVoteCount(year);
    // drop anything already captured by the recency carve-out to avoid double-counting
    established.push(...yearResults.filter((r) => !recentIds.has(r.id)));
  }
  console.log(`${established.length} established movies clear vote_count >= ${MIN_VOTE_COUNT}.`);

  established.sort((a, b) => b.vote_count - a.vote_count);

  const totalQualifying = recent.length + established.length;
  console.log(
    `Total mainstream-qualifying movies: ${totalQualifying}` +
      (totalQualifying < CATALOG_TARGET
        ? ` — below the ${CATALOG_TARGET} target. Catalog will be smaller than planned unless MIN_VOTE_COUNT is lowered.`
        : ` — capping at ${CATALOG_TARGET}.`),
  );

  const ranked = [
    ...recent.map((r) => r.id),
    ...established.map((r) => r.id),
  ].slice(0, CATALOG_TARGET);

  return ranked.map((id, i) => ({ id, rank: i + 1 }));
}

async function getExistingTmdbIds(): Promise<Set<number>> {
  const ids = new Set<number>();
  let from = 0;
  const pageSize = 1000;
  for (;;) {
    const { data, error } = await supabase
      .from('movies')
      .select('tmdb_id')
      .range(from, from + pageSize - 1);
    if (error) throw error;
    if (!data || data.length === 0) break;
    for (const row of data) ids.add(row.tmdb_id);
    if (data.length < pageSize) break;
    from += pageSize;
  }
  return ids;
}

interface MovieRow {
  tmdb_id: number;
  title: string;
  original_title: string | null;
  overview: string | null;
  release_date: string | null;
  release_year: number | null;
  runtime: number | null;
  budget: number | null;
  revenue: number | null;
  genres: string[];
  poster_path: string;
  poster_url: string;
  backdrop_path: string | null;
  backdrop_url: string | null;
  vote_average: number | null;
  vote_count: number | null;
  popularity: number | null;
  adult: boolean;
  original_language: string | null;
  imdb_id: string | null;
  keywords: string[];
  top_cast: string[];
  directors: string[];
  tagline: string | null;
  import_rank_popularity: number;
  hydration_status: string;
  scoring_eligible: boolean;
}

// Returns null for anything that fails the clean-data gate — reject adult,
// missing poster, or missing overview outright rather than let thin/unreliable
// entries into the catalog.
async function hydrateMovie(tmdbId: number, rank: number): Promise<MovieRow | null> {
  const d = await tmdbGet(`/movie/${tmdbId}?append_to_response=keywords,credits`);
  if (!d) return null;

  if (d.adult) return null;
  if (!d.poster_path) return null;
  if (!d.overview || d.overview.trim().length === 0) return null;

  const keywords: string[] = (d.keywords?.keywords ?? []).map((k: any) => k.name);
  const cast: string[] = (d.credits?.cast ?? []).slice(0, 10).map((c: any) => c.name);
  const directors: string[] = (d.credits?.crew ?? [])
    .filter((c: any) => c.job === 'Director')
    .map((c: any) => c.name);

  const overviewLen = d.overview.trim().length as number;
  const scoringEligible =
    overviewLen >= 100 &&
    (d.genres?.length ?? 0) > 0 &&
    (keywords.length >= 2 || Boolean(d.tagline));

  return {
    tmdb_id: d.id,
    title: d.title,
    original_title: d.original_title ?? null,
    overview: d.overview,
    release_date: d.release_date || null,
    release_year: d.release_date ? Number(d.release_date.slice(0, 4)) : null,
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
    tagline: d.tagline || null,
    import_rank_popularity: rank,
    hydration_status: 'complete',
    scoring_eligible: scoringEligible,
  };
}

async function runWithConcurrency<T, R>(
  items: T[],
  limit: number,
  worker: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let next = 0;
  async function runner() {
    while (next < items.length) {
      const i = next++;
      results[i] = await worker(items[i], i);
    }
  }
  await Promise.all(Array.from({ length: limit }, runner));
  return results;
}

async function main() {
  if (DRY_RUN) {
    console.log(`[DRY RUN] MIN_VOTE_COUNT=${MIN_VOTE_COUNT} — discovery only, no hydration, no DB writes.`);
    await selectMainstreamIds();
    console.log('[DRY RUN] Done. No pipeline_runs row was created and nothing was written to Supabase.');
    return;
  }

  const { data: run, error: runErr } = await supabase
    .from('pipeline_runs')
    .insert({ run_type: 'ingestion', status: 'running' })
    .select()
    .single();
  if (runErr) throw runErr;

  try {
    const topIds = await selectMainstreamIds();

    console.log('Checking existing catalog...');
    const existing = await getExistingTmdbIds();
    const toFetch = topIds.filter((r) => !existing.has(r.id));
    console.log(`${toFetch.length} new ids to hydrate (${existing.size} already in DB).`);

    let inserted = 0;
    let rejected = 0;

    for (let i = 0; i < toFetch.length; i += INSERT_BATCH_SIZE) {
      const batch = toFetch.slice(i, i + INSERT_BATCH_SIZE);
      const hydrated = await runWithConcurrency(batch, CONCURRENCY, (row) =>
        hydrateMovie(row.id, row.rank),
      );
      const rows = hydrated.filter((r): r is MovieRow => r !== null);
      rejected += hydrated.length - rows.length;

      if (rows.length > 0) {
        const { error } = await supabase.from('movies').insert(rows);
        if (error) console.error('Insert error:', error.message);
        else inserted += rows.length;
      }
      console.log(
        `Progress: ${Math.min(i + INSERT_BATCH_SIZE, toFetch.length)}/${toFetch.length} processed, ${inserted} inserted, ${rejected} rejected so far.`,
      );
    }

    await supabase
      .from('pipeline_runs')
      .update({
        status: 'success',
        finished_at: new Date().toISOString(),
        rows_processed: inserted,
        rows_failed: rejected,
      })
      .eq('id', run!.id);

    console.log(`Done. Inserted ${inserted}, rejected ${rejected} (failed the clean-data gate, or 404s).`);
  } catch (err: any) {
    await supabase
      .from('pipeline_runs')
      .update({
        status: 'failed',
        finished_at: new Date().toISOString(),
        error_message: String(err?.message ?? err),
      })
      .eq('id', run!.id);
    throw err;
  }
}

main();
