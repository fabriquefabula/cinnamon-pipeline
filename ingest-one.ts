// Add named titles to the catalogue by TMDB id, bypassing every
// selection rule.
//
// The automatic ingests all answer "is this big enough to be worth
// having". That question has no threshold that is right in every case:
// a film can be obviously worth having and have no votes, no popularity
// and nobody recognisable in it -- a festival title, a first feature, a
// genre film somebody read about this morning. Tuning a gate loose
// enough to catch those catches thousands of films nobody wants.
//
// So this is the escape hatch, and it is deliberately not automatic.
// Paste ids, get rows. It skips the fame gate, the vote floor, the
// popularity floor and the date window, and keeps only the clean-data
// gate the other ingests use -- no adult titles, and a poster and
// overview must exist, because a row without those renders as a hole on
// every page it appears on.
//
// Run it from the Actions tab: "Ingest by TMDB id", paste a
// comma-separated list into the input. The id is the number in a TMDB
// URL -- themoviedb.org/movie/1198654-bad-apples is 1198654.
//
// Env: TMDB_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, TMDB_IDS
// Optional: MEDIUM=movie|tv (default movie)

import { createClient } from '@supabase/supabase-js';

const TMDB_API_KEY = requireEnv('TMDB_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');
const MEDIUM = process.env.MEDIUM === 'tv' ? 'tv' : 'movie';

const SITE_VISIBLE_VOTES = 300; // kept in step with ingest.ts
const MIN_OVERVIEW_CHARS = 40; // kept in step with ingest-tv.ts
const MIN_DESCRIPTIVE_CHARS = 100;

// A hand-entered list. Anything longer is a sign somebody meant to
// change a threshold in one of the automatic ingests instead.
const MAX_IDS = 50;

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

// Accepts "123, 456", "123 456", newlines, and a pasted TMDB URL --
// somebody copying an id out of the address bar should not have to
// think about which part of it this wants.
function parseIds(raw: string): number[] {
  const ids = new Set<number>();
  for (const piece of raw.split(/[\s,]+/)) {
    if (!piece) continue;
    const m = piece.match(/(?:movie|tv)\/(\d+)/) ?? piece.match(/^(\d+)/);
    if (!m) {
      console.warn(`Skipping unparseable input: "${piece}"`);
      continue;
    }
    ids.add(Number(m[1]));
  }
  return [...ids];
}

function movieScoringEligible(overview: string, genres: unknown[], keywords: unknown[], tagline: string | null, votes: number): boolean {
  const len = overview.trim().length;
  if (genres.length === 0) return false;
  return (
    len >= 100 ||
    (len >= 40 && (keywords.length >= 5 || Boolean(tagline?.trim()))) ||
    votes >= SITE_VISIBLE_VOTES
  );
}

async function buildMovie(tmdbId: number): Promise<Record<string, any> | null> {
  const d = await tmdbGet(`/movie/${tmdbId}?append_to_response=keywords,credits`);
  if (!d) {
    console.error(`  ${tmdbId}: not found on TMDB`);
    return null;
  }
  if (d.adult) {
    console.error(`  ${tmdbId} ("${d.title}"): adult, refused`);
    return null;
  }
  if (!d.poster_path) {
    console.error(`  ${tmdbId} ("${d.title}"): no poster, refused`);
    return null;
  }
  if (!d.overview || d.overview.trim().length === 0) {
    console.error(`  ${tmdbId} ("${d.title}"): no overview, refused`);
    return null;
  }

  const keywords: string[] = (d.keywords?.keywords ?? []).map((k: any) => k.name);
  const crew: any[] = d.credits?.crew ?? [];

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
    top_cast: (d.credits?.cast ?? []).slice(0, 10).map((c: any) => c.name),
    directors: crew.filter((c) => c.job === 'Director').map((c) => c.name),
    production_companies: (d.production_companies ?? []).map((c: any) => c.name),
    tagline: d.tagline || null,
    import_rank_popularity: 0,
    hydration_status: 'complete',
    scoring_eligible: movieScoringEligible(
      d.overview,
      d.genres ?? [],
      keywords,
      d.tagline ?? null,
      d.vote_count ?? 0,
    ),
  };
}

async function buildShow(tmdbId: number): Promise<Record<string, any> | null> {
  const d = await tmdbGet(
    `/tv/${tmdbId}?append_to_response=keywords,aggregate_credits,external_ids`,
  );
  if (!d) {
    console.error(`  ${tmdbId}: not found on TMDB`);
    return null;
  }
  if (d.adult) {
    console.error(`  ${tmdbId} ("${d.name}"): adult, refused`);
    return null;
  }
  if (!d.poster_path) {
    console.error(`  ${tmdbId} ("${d.name}"): no poster, refused`);
    return null;
  }
  if (!d.overview || d.overview.trim().length === 0) {
    console.error(`  ${tmdbId} ("${d.name}"): no overview, refused`);
    return null;
  }

  // /tv returns keywords under `results`, not `keywords` as /movie does.
  // Reading the movie key here yields an empty array for every show and
  // fails the scoring gate silently -- see ingest-tv.ts.
  const keywords: string[] = (d.keywords?.results ?? []).map((k: any) => k.name);
  const overviewLen = d.overview.trim().length as number;
  const descriptiveLen = overviewLen + (d.tagline ?? '').trim().length + keywords.join(', ').length;

  return {
    tmdb_id: d.id,
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
    top_cast: (d.aggregate_credits?.cast ?? []).slice(0, 10).map((c: any) => c.name),
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

async function main() {
  const raw = process.env.TMDB_IDS;
  if (!raw || raw.trim().length === 0) {
    throw new Error('TMDB_IDS is empty. Pass a comma-separated list of TMDB ids.');
  }

  const ids = parseIds(raw);
  if (ids.length === 0) throw new Error(`No usable ids found in "${raw}"`);
  if (ids.length > MAX_IDS) {
    throw new Error(`${ids.length} ids given, limit is ${MAX_IDS}. This is a hand-entry tool.`);
  }

  const table = MEDIUM === 'tv' ? 'tv_shows' : 'movies';
  console.log(`Adding ${ids.length} ${MEDIUM === 'tv' ? 'series' : 'film'}(s) to ${table}: ${ids.join(', ')}`);

  let added = 0;
  let updated = 0;
  let refused = 0;

  for (const id of ids) {
    const row = MEDIUM === 'tv' ? await buildShow(id) : await buildMovie(id);
    if (!row) {
      refused++;
      continue;
    }

    const { data: existing } = await supabase
      .from(table)
      .select('id')
      .eq('tmdb_id', id)
      .maybeSingle();

    const { error } = await supabase.from(table).upsert(row, { onConflict: 'tmdb_id' });
    if (error) {
      refused++;
      console.error(`  ${id} ("${row.title}") failed: ${error.message}`);
      continue;
    }

    if (existing) {
      updated++;
      console.log(`  ~ ${row.title} (${row.release_year ?? '—'}) already present, refreshed`);
    } else {
      added++;
      console.log(
        `  + ${row.title} (${row.release_year ?? '—'})` +
          (row.scoring_eligible
            ? ' -- will be scored on the next scoring run'
            : ' -- NOT scoring-eligible: too little metadata, so it will not appear in' +
              ' recommendations or shelves until TMDB fills in more'),
      );
    }
  }

  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'manual_ingestion',
    started_at: new Date().toISOString(),
    finished_at: new Date().toISOString(),
    rows_processed: added + updated,
    rows_failed: refused,
    status: refused > 0 && added + updated === 0 ? 'failed' : 'success',
  });
  if (logError) console.error(`pipeline_runs logging failed (non-fatal): ${logError.message}`);

  console.log(`\nDone. ${added} added, ${updated} refreshed, ${refused} refused.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
