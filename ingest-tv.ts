// Weekly TMDB TV ingestion for Cinnamon DB.
//
// Mirrors ingest.ts deliberately, including its selection philosophy:
// vote_count-based rather than popularity-based, because TMDB's
// `popularity` is a same-day activity score, not a lifetime-fame metric,
// while vote_count is cumulative and much harder to spike. The same
// recency carve-out applies — a series that premiered last month has had
// no time to accumulate votes, so it qualifies on popularity instead.
//
// TV is a fully separate silo: this writes to tv_shows, and nothing here
// ever touches movies. The one shared resource is `people`, populated by
// ingest-tv-credits.ts, since filmographies are explicitly not siloed.
//
// Required env vars: TMDB_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const TMDB_API_KEY = requireEnv('TMDB_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

// A cap, not a quota, and one that does not currently bind: measured
// 2026-09-12 by dry run, TV simply does not have 20k series worth
// recommending. See the MIN_VOTE_COUNT note below for the numbers. Left
// well above the real population so it never silently truncates as TMDB
// grows; if a future run reports a total near this figure, raise it
// rather than let the catalogue be clipped.
const CATALOG_TARGET = 20_000;
// Measured 2026-09-12 (DRY_RUN, floor 20): 12,517 established series +
// 79 recent premieres = 12,596 qualifying. For contrast, the movie curve
// at the same floor yields ~65k, and at floor 300 still yields ~11k —
// the TV distribution is far steeper, because TV vote counts run
// structurally lower than film.
//
// Floor 20 is a deliberate choice to keep, not a leftover from the movie
// tuning: dropping to 10 would reach roughly 20k, but the marginal ~8k
// are titles with 10-19 total votes and thin overviews, which are
// exactly the ones that then fail the scoring gate or score badly. The
// movie side settled at 20 for the same reason.
//
// Expect roughly 9-10k scoring-eligible shows from this: movies ran
// 65,760 imported -> 48,708 scored, about 74%.
const MIN_VOTE_COUNT = process.env.MIN_VOTE_COUNT ? parseInt(process.env.MIN_VOTE_COUNT, 10) : 20;
// When true: discovery and counting only. Logs the result and exits
// before touching Supabase at all — no pipeline_runs row, no hydration,
// no writes.
const DRY_RUN = process.env.DRY_RUN === 'true';
const RECENT_MONTHS = 6;
const MIN_RECENT_POPULARITY = 20;
const CONCURRENCY = 20; // stays well under TMDB's ~40-50 req/s soft limit
const INSERT_BATCH_SIZE = 500;
// Television, not film: 1900 would burn ~40 pointless year queries.
// A handful of TMDB entries carry implausible early first_air_dates, so
// this starts slightly before the practical start of broadcast TV.
const EARLIEST_YEAR = 1930;

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
  first_air_date: string;
}

async function tmdbGet(path: string): Promise<any> {
  const res = await fetch(`https://api.themoviedb.org/3${path}`, {
    headers: { Authorization: `Bearer ${TMDB_API_KEY}` },
  });
  if (!res.ok) return null;
  return res.json();
}

// TMDB caps any single query+filter combination at 500 pages (10,000
// results). Slicing by first-air year keeps each query well under that
// cap while still collecting every qualifying series globally, by
// combining all years ourselves rather than asking one query to rank
// everything.
//
// Note the parameter name differs from the movie endpoint:
// first_air_date_year here, primary_release_year there.
async function discoverByVoteCount(year: number): Promise<DiscoverResult[]> {
  const results: DiscoverResult[] = [];
  let page = 1;
  while (page <= 500) {
    const data = await tmdbGet(
      `/discover/tv?sort_by=vote_count.desc&first_air_date_year=${year}&vote_count.gte=${MIN_VOTE_COUNT}&page=${page}`,
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
      `/discover/tv?sort_by=popularity.desc&first_air_date.gte=${cutoffStr}&page=${page}`,
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

  console.log(`Collecting recent premieres (last ${RECENT_MONTHS} months) by popularity...`);
  const recent = await discoverRecentByPopularity();
  const recentIds = new Set(recent.map((r) => r.id));
  console.log(`${recent.length} recent premieres qualify via the popularity carve-out.`);

  console.log(`Collecting established series (vote_count >= ${MIN_VOTE_COUNT}), by first-air year...`);
  const established: DiscoverResult[] = [];
  for (let year = EARLIEST_YEAR; year <= currentYear + 1; year++) {
    const yearResults = await discoverByVoteCount(year);
    established.push(...yearResults.filter((r) => !recentIds.has(r.id)));
  }
  console.log(`${established.length} established series clear vote_count >= ${MIN_VOTE_COUNT}.`);

  established.sort((a, b) => b.vote_count - a.vote_count);

  const totalQualifying = recent.length + established.length;
  // Coming in under CATALOG_TARGET is the expected, accepted state here
  // — around 12.6k at floor 20 as of 2026-09-12 — so this is phrased as
  // an observation rather than the movie script's warning. Only a total
  // at or above the cap needs action.
  console.log(
    `Total mainstream-qualifying series: ${totalQualifying}` +
      (totalQualifying < CATALOG_TARGET
        ? ` — under the ${CATALOG_TARGET} cap, as expected. Taking all of them.`
        : ` — at or above the ${CATALOG_TARGET} cap, so the catalogue is being CLIPPED. Raise CATALOG_TARGET.`),
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
      .from('tv_shows')
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

interface ShowRow {
  tmdb_id: number;
  title: string;
  original_title: string | null;
  overview: string | null;
  first_air_date: string | null;
  last_air_date: string | null;
  release_year: number | null;
  status: string | null;
  in_production: boolean | null;
  type: string | null;
  episode_run_time: number[];
  number_of_seasons: number | null;
  number_of_episodes: number | null;
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
  created_by: string[];
  networks: string[];
  production_companies: string[];
  tagline: string | null;
  import_rank_popularity: number;
  hydration_status: string;
  scoring_eligible: boolean;
}

// Returns null for anything failing the clean-data gate: reject adult,
// missing poster, or missing overview outright rather than admit thin
// entries to the catalogue.
//
// Three endpoint differences from the movie version, all load-bearing:
//
//   keywords       -> /tv returns these under `keywords.results`, NOT
//                     `keywords.keywords` as /movie does. Reading the
//                     movie key here yields an empty array for every
//                     show, which would then fail the scoring gate
//                     silently and leave the whole catalogue ineligible.
//   credits        -> aggregate_credits, not credits. Rolls a person's
//                     work up across all episodes, so `character` lives
//                     in a nested roles[] array rather than on the cast
//                     entry itself.
//   imdb_id        -> absent from the base /tv response; requires the
//                     external_ids append.
async function hydrateShow(tmdbId: number, rank: number): Promise<ShowRow | null> {
  const d = await tmdbGet(`/tv/${tmdbId}?append_to_response=keywords,aggregate_credits,external_ids`);
  if (!d) return null;

  if (d.adult) return null;
  if (!d.poster_path) return null;
  if (!d.overview || d.overview.trim().length === 0) return null;

  const keywords: string[] = (d.keywords?.results ?? []).map((k: any) => k.name);

  // aggregate_credits.cast is already ordered by billing. Each entry has
  // roles[] (one per distinct character) rather than a single character
  // field.
  const cast: string[] = (d.aggregate_credits?.cast ?? []).slice(0, 10).map((c: any) => c.name);

  // created_by is the closest authorial equivalent to a film's director.
  // Episode directors are deliberately excluded here — one person
  // directing 3 of 60 episodes is not a series-level authorship claim.
  // They are still captured per-episode-count in tv_credits.
  const createdBy: string[] = (d.created_by ?? []).map((c: any) => c.name);

  const networks: string[] = (d.networks ?? []).map((n: any) => n.name);
  const productionCompanies: string[] = (d.production_companies ?? []).map((c: any) => c.name);

  // Same gate as movies, intentionally, so eligibility means the same
  // thing in both catalogues. Worth measuring after the first real run:
  // TV taglines are far rarer than film taglines, so the
  // "2+ keywords OR a tagline" clause may reject a larger share of shows
  // than it does movies. Do not loosen it blind — check the actual
  // eligible/ineligible split first, which is logged per batch below.
  const overviewLen = d.overview.trim().length as number;
  const scoringEligible =
    overviewLen >= 100 &&
    (d.genres?.length ?? 0) > 0 &&
    (keywords.length >= 2 || Boolean(d.tagline));

  return {
    tmdb_id: d.id,
    // Stored as title/original_title rather than name/original_name so
    // shared card components, search mapping and sort logic work against
    // either table without branching.
    title: d.name,
    original_title: d.original_name ?? null,
    overview: d.overview,
    first_air_date: d.first_air_date || null,
    last_air_date: d.last_air_date || null,
    release_year: d.first_air_date ? Number(d.first_air_date.slice(0, 4)) : null,
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
    created_by: createdBy,
    networks,
    production_companies: productionCompanies,
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
    .insert({ run_type: 'tv_ingestion', status: 'running' })
    .select()
    .single();
  if (runErr) throw runErr;

  try {
    const topIds = await selectMainstreamIds();

    console.log('Checking existing catalogue...');
    const existing = await getExistingTmdbIds();
    const toFetch = topIds.filter((r) => !existing.has(r.id));
    console.log(`${toFetch.length} new ids to hydrate (${existing.size} already in DB).`);

    let inserted = 0;
    let rejected = 0;
    let eligible = 0;

    for (let i = 0; i < toFetch.length; i += INSERT_BATCH_SIZE) {
      const batch = toFetch.slice(i, i + INSERT_BATCH_SIZE);
      const hydrated = await runWithConcurrency(batch, CONCURRENCY, (row) =>
        hydrateShow(row.id, row.rank),
      );
      const rows = hydrated.filter((r): r is ShowRow => r !== null);
      rejected += hydrated.length - rows.length;
      eligible += rows.filter((r) => r.scoring_eligible).length;

      if (rows.length > 0) {
        const { error } = await supabase.from('tv_shows').insert(rows);
        if (error) console.error('Insert error:', error.message);
        else inserted += rows.length;
      }
      console.log(
        `Progress: ${Math.min(i + INSERT_BATCH_SIZE, toFetch.length)}/${toFetch.length} processed, ${inserted} inserted (${eligible} scoring-eligible), ${rejected} rejected so far.`,
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

    // The eligible count is the number that matters next: it is exactly
    // how many Batch API scoring calls the TV pass will cost, and how
    // many sources tv_neighbors will have to compute rails for.
    console.log(
      `Done. Inserted ${inserted} (${eligible} scoring-eligible), rejected ${rejected} (failed the clean-data gate, or 404s).`,
    );
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
