// TV credits ingestion. Mirrors ingest-credits.ts, writing to
// tv_credits.
//
// The `people` table is SHARED with movies and is the one deliberate
// exception to the TV/movie silo: filmographies are not siloed, so
// /person/[id] unions movie_credits and tv_credits into a single page.
// Person rows are upserted on tmdb_person_id, so an actor who appears in
// both catalogues gets one row, not two.
//
// Required env vars: TMDB_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const TMDB_API_KEY = process.env.TMDB_API_KEY!;
const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Same depth as the movie side. Aggregate cast lists for long-running
// series can run to hundreds of names once every guest star across ten
// seasons is included, so the cap matters more here than it does for film.
const CAST_LIMIT = 20;
// An episode director on 2 of 62 episodes is not an authorship signal
// the way a film's director is. They are still stored (with
// episode_count, so the UI can threshold), but this floor keeps
// single-episode journeymen out of the table entirely.
const MIN_DIRECTOR_EPISODES = 2;
const CONCURRENCY = 8;
const PAGE_SIZE = 500;

interface TmdbPerson {
  id: number;
  name: string;
  profile_path: string | null;
  known_for_department: string | null;
  popularity: number | null;
}

// aggregate_credits differs structurally from /credits: a person appears
// once per series with their work rolled up across episodes, so the
// character lives in a nested roles[] array (a soap actor may have
// played three characters) and job in a nested jobs[] array.
interface AggregateCastMember extends TmdbPerson {
  roles: { character: string; episode_count: number }[];
  total_episode_count: number;
  order: number;
}

interface AggregateCrewMember extends TmdbPerson {
  jobs: { job: string; episode_count: number }[];
  department: string;
  total_episode_count: number;
}

interface ShowCredits {
  created_by: TmdbPerson[];
  aggregate_credits: {
    cast: AggregateCastMember[];
    crew: AggregateCrewMember[];
  };
}

// One call, not two: created_by lives on the series detail response
// while the cast/crew rollup lives on aggregate_credits, and
// append_to_response fetches both in a single request. created_by is
// needed here rather than read from tv_shows.created_by because that
// column stores names only -- no tmdb person ids to join people on.
async function fetchShowCredits(tmdbId: number): Promise<ShowCredits | null> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  try {
    const res = await fetch(
      `https://api.themoviedb.org/3/tv/${tmdbId}?append_to_response=aggregate_credits`,
      {
        headers: {
          Authorization: `Bearer ${TMDB_API_KEY}`,
          accept: 'application/json',
        },
        signal: controller.signal,
      },
    );
    if (!res.ok) {
      if (res.status !== 404) console.error(`TMDB ${res.status} for tv tmdb_id=${tmdbId}`);
      return null;
    }
    return await res.json();
  } catch (err) {
    const reason = err instanceof Error && err.name === 'AbortError' ? 'timeout' : err;
    console.error(`Fetch failed for tv tmdb_id=${tmdbId}:`, reason);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

// roles[] is not guaranteed ordered, and for a series where someone
// played two parts the one they played longest is the one worth showing.
function primaryRole(c: AggregateCastMember): { character: string | null; episodes: number } {
  if (!c.roles?.length) return { character: null, episodes: c.total_episode_count ?? 0 };
  const best = c.roles.reduce((a, b) => ((b.episode_count ?? 0) > (a.episode_count ?? 0) ? b : a));
  return { character: best.character || null, episodes: c.total_episode_count ?? best.episode_count ?? 0 };
}

async function processShow(show: {
  id: string;
  tmdb_id: number;
}): Promise<{ cast: number; creators: number; directors: number } | null> {
  const d = await fetchShowCredits(show.tmdb_id);
  if (!d) return null;

  const topCast = (d.aggregate_credits?.cast ?? []).slice(0, CAST_LIMIT);
  const creators = d.created_by ?? [];

  // Episode directors, thresholded. Kept separate from creators because
  // they mean something different in television: creators are the
  // authorial credit, directors are per-episode craft.
  const directors = (d.aggregate_credits?.crew ?? [])
    .filter((c) => (c.jobs ?? []).some((j) => j.job === 'Director'))
    .filter((c) => (c.total_episode_count ?? 0) >= MIN_DIRECTOR_EPISODES);

  const people = new Map<number, TmdbPerson>();
  for (const p of [...topCast, ...creators, ...directors]) people.set(p.id, p);
  if (people.size === 0) return { cast: 0, creators: 0, directors: 0 };

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
        hydration_status: 'pending', // biography needs a separate /person/{id} pass, same as movies
      })),
      { onConflict: 'tmdb_person_id' },
    )
    .select('id, tmdb_person_id');

  if (upsertError || !upserted) {
    console.error(`Person upsert failed for show ${show.id}:`, upsertError?.message);
    return null;
  }

  const personIdByTmdb = new Map(upserted.map((p) => [p.tmdb_person_id, p.id]));

  const creditRows = [
    ...topCast.map((c) => {
      const role = primaryRole(c);
      return {
        show_id: show.id,
        person_id: personIdByTmdb.get(c.id),
        credit_type: 'cast',
        character: role.character,
        billing_order: c.order,
        episode_count: role.episodes,
      };
    }),
    // 'creator' rather than 'crew': created_by has no equivalent in
    // movie_credits, and collapsing it into 'crew' would make it
    // indistinguishable from episode directors at query time.
    ...creators.map((c) => ({
      show_id: show.id,
      person_id: personIdByTmdb.get(c.id),
      credit_type: 'creator',
      job: 'Creator',
      department: 'Production',
    })),
    ...directors.map((c) => ({
      show_id: show.id,
      person_id: personIdByTmdb.get(c.id),
      credit_type: 'crew',
      job: 'Director',
      department: c.department ?? 'Directing',
      episode_count: c.total_episode_count ?? null,
    })),
  ].filter((r) => r.person_id);

  if (creditRows.length > 0) {
    const { error: creditError } = await supabase.from('tv_credits').insert(creditRows);
    if (creditError) {
      console.error(`Credit insert failed for show ${show.id}:`, creditError.message);
      return null;
    }
  }

  // tv_shows.creator_credits is the fast-path JSONB column cards read to
  // avoid a join per render -- the direct analogue of
  // movies.director_credits, which ingest-credits.ts originally forgot
  // to populate. Written here for the same reason and in the same shape.
  if (creators.length > 0) {
    const creatorCredits = creators
      .map((c) => ({ id: personIdByTmdb.get(c.id), name: c.name }))
      .filter((c) => c.id);
    const { error: ccError } = await supabase
      .from('tv_shows')
      .update({ creator_credits: creatorCredits })
      .eq('id', show.id);
    if (ccError) console.error(`creator_credits update failed for show ${show.id}:`, ccError.message);
  }

  return { cast: topCast.length, creators: creators.length, directors: directors.length };
}

// Built once at start for resumability, not checked per-show, which
// would double the round trips.
async function getAlreadyIngestedShowIds(): Promise<Set<string>> {
  const ids = new Set<string>();
  let cursor: string | null = null;
  while (true) {
    let query = supabase.from('tv_credits').select('id, show_id').order('id').limit(1000);
    if (cursor) query = query.gt('id', cursor);

    const { data, error } = await query;
    if (error || !data || data.length === 0) break;
    for (const row of data) ids.add(row.show_id as string);
    cursor = data[data.length - 1].id as string;
    if (data.length < 1000) break;
  }
  return ids;
}

async function main() {
  const dryRun = process.env.DRY_RUN === 'true';
  const showLimit = process.env.SHOW_LIMIT ? parseInt(process.env.SHOW_LIMIT, 10) : undefined;

  console.log(
    `Starting TV credits ingestion${dryRun ? ' (DRY RUN)' : ''}${showLimit ? `, limit=${showLimit}` : ''}...`,
  );

  console.log('Loading already-ingested show IDs (for resumability)...');
  const alreadyDone = await getAlreadyIngestedShowIds();
  console.log(`${alreadyDone.size} shows already have credits, will skip them.`);

  let cursor: string | null = null;
  let processed = 0;
  let castTotal = 0;
  let creatorTotal = 0;
  let directorTotal = 0;
  let failures = 0;

  outer: while (true) {
    let query = supabase
      .from('tv_shows')
      .select('id, tmdb_id')
      .not('tmdb_id', 'is', null)
      .order('id')
      .limit(PAGE_SIZE);

    if (cursor) query = query.gt('id', cursor);

    const { data: shows, error } = await query;

    if (error) {
      console.error('Failed to fetch shows batch:', error.message);
      break;
    }
    if (!shows || shows.length === 0) break;

    cursor = shows[shows.length - 1].id; // advance regardless of how many get skipped below
    const todo = shows.filter((s) => !alreadyDone.has(s.id));
    console.log(
      `Page fetched: ${shows.length} shows, ${todo.length} new, ${shows.length - todo.length} already done. Cursor now ${cursor}.`,
    );

    for (let i = 0; i < todo.length; i += CONCURRENCY) {
      const chunk = todo.slice(i, i + CONCURRENCY);
      const results = await Promise.all(
        chunk.map((s) =>
          dryRun ? Promise.resolve({ cast: 0, creators: 0, directors: 0 }) : processShow(s),
        ),
      );

      for (const r of results) {
        if (r) {
          castTotal += r.cast;
          creatorTotal += r.creators;
          directorTotal += r.directors;
        } else {
          failures++;
        }
      }

      processed += chunk.length;
      if (processed % 200 === 0) {
        console.log(
          `Processed ${processed} shows (${castTotal} cast, ${creatorTotal} creator, ${directorTotal} director credits, ${failures} failures)...`,
        );
      }

      if (showLimit && processed >= showLimit) break outer;
    }
  }

  console.log(
    `Done. Processed ${processed} shows. ${castTotal} cast, ${creatorTotal} creator, ${directorTotal} director credits, ${failures} failures.`,
  );
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
