// Films with a recognisable name attached, dated near now, that the
// other two ingests cannot see.
//
// THE GAP THIS CLOSES
//
// A film enters the catalogue by exactly one of three routes before
// this script, and all three are blind to the same case:
//
//   ingest.ts        vote_count >= 20, or released within 6 months AND
//                    popularity >= 20
//   ingest-tv.ts     the same, for premieres that have already happened
//   ingest-upcoming  dated in the next 180 days, but sorted
//                    popularity.desc and capped at the top 300 globally
//
// So a film with no votes yet, popularity under 20, that is either
// already out (the upcoming sweep only looks forward from today) or is
// dated ahead but not among the world's 300 most-trafficked upcoming
// titles, is in none of them. That is precisely where an anticipated
// film sits in the weeks either side of release: it has a trailer, a
// cast and press, and no ratings at all. Measured when this was
// written: Bad Apples, a Saoirse Ronan film, was missing on exactly
// those grounds.
//
// WHY IT ASKS RATHER THAN FILTERS
//
// The obvious build is to enumerate the window by release date and keep
// the films with a known name. That means hydrating every dated title on
// earth to read its cast -- tens of thousands of calls a run -- because
// /discover returns no credits.
//
// TMDB's with_people filter inverts it. Hand it the people and it
// returns their films directly, so the fame gate IS the query and
// nothing is fetched unless a known name is already attached. One run is
// a few hundred requests rather than tens of thousands, and the volume
// is bounded by the size of the people list rather than by the calendar.
//
// The list comes from notable_people() in Postgres: a top-3 billing or a
// director credit on a film or series above a vote floor. At the default
// floor that is ~5,000 people. Any-credit was tested at 50,070 and is
// far too loose -- a film passes if any one name matches, so that would
// let in most of the calendar.
//
// THE SECOND GATE, WHICH IS THE ONE THAT MATTERS
//
// with_people matches cast AND crew, including one-line parts and
// second-unit work. So a match is only a candidate. After hydration the
// film must put a notable person in its top five billed or in the
// director's chair -- the difference between a film built around someone
// and a film they walked through. Without this the discovery is precise
// and the output is not.
//
// Env: TMDB_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=true, MIN_PERSON_VOTES, MAX_NEW, BACK_DAYS, FORWARD_DAYS

import { createClient } from '@supabase/supabase-js';

const TMDB_API_KEY = requireEnv('TMDB_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

// Discovery only, no writes, no pipeline_runs row. Run this first after
// any threshold change -- the whole point of the gate is volume, and the
// only honest way to know the volume is to count it.
const DRY_RUN = process.env.DRY_RUN === 'true';

// Vote floor for "a name people know". 2,500 gives ~5,000 people, 1,000
// gives ~10,500, 5,000 gives ~2,550. Tunable per run.
const MIN_PERSON_VOTES = intEnv('MIN_PERSON_VOTES', 2500);

// Backstop, not a target. If a threshold change or a TMDB behaviour
// change makes this job suddenly match thousands of films, it stops and
// says so rather than quietly hydrating and scoring all of them. Hitting
// the cap is a reason to look, not a normal outcome.
const MAX_NEW = intEnv('MAX_NEW', 150);

// Behind far enough to actually reach the films that motivated this job.
//
// 120 days was the first guess and it was wrong in the most pointless
// way available: Bad Apples -- the Saoirse Ronan film this whole script
// was written to catch -- released in 2025 and fell outside it. The
// window was never the volume control. The fame gate and MAX_NEW are.
//
// Widening backwards is close to free, because a film that old with a
// notable lead has almost always cleared ingest.ts's twenty-vote floor
// already, so it is filtered out as existing before anything is
// hydrated. What is left is exactly the residue worth having: older
// films with a real name attached that never accumulated votes.
//
// Ahead, further than ingest-upcoming's 180 days, because a film with a
// cast attached is announced long before it is marketable and there is
// no cost to holding it early.
const BACK_DAYS = intEnv('BACK_DAYS', 1095);
const FORWARD_DAYS = intEnv('FORWARD_DAYS', 365);

// URL length, not a TMDB limit. Twenty-five seven-digit ids plus
// separators is a comfortable query string; a few hundred is not.
const PEOPLE_PER_QUERY = 25;

// Hard stop on pagination per group, and the reason this exists: the
// first version let each group run to TMDB's 500-page ceiling. If
// with_people is ever ignored or mis-spelled, every query returns the
// entire release calendar instead of one group's films, and 500 pages
// times two hundred groups is forty thousand sequential requests -- a
// job that grinds until the workflow timeout kills it and writes
// nothing. Measured against the real shape of the data this is
// generous: twenty-five people with a film or two each inside the
// window is two or three pages.
const MAX_PAGES_PER_GROUP = 10;
const DISCOVER_CONCURRENCY = 8;

const CONCURRENCY = 20; // as the other ingests -- well under TMDB's soft limit
const INSERT_BATCH_SIZE = 100;
const SITE_VISIBLE_VOTES = 300; // kept in step with ingest.ts
const TOP_BILLED = 5;

// TMDB's genre id for Documentary, excluded at the query.
//
// This is the fame gate's one systematic blind spot and it is a big
// one. A documentary ABOUT a famous person bills that person in its top
// five, so every making-of, retrospective and celebrity profile passes
// a test designed to find films built around them. The first live run
// took 108 films and 40 were exactly this: The Odyssey: The Making of
// an Epic, Toy Story: 30 Years and Beyond, Generations: The Evolution
// of Spider-Man, Making Marie Antoinette, Euphoria: A Look Back.
//
// A documentary about a film is not the film. Excluded at the query
// rather than after hydration, so they cost nothing, and checked again
// below in case the genre only arrives on the detail response.
const DOCUMENTARY_GENRE_ID = 99;

// Shorts, specials and festival featurettes. Only applied when TMDB
// actually knows the runtime -- an unreleased film very often has 0 or
// null there, and treating unknown as disqualifying would throw out the
// genuine upcoming features this job exists to catch.
const MIN_FEATURE_RUNTIME = 60;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

function intEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const n = parseInt(raw, 10);
  return Number.isFinite(n) ? n : fallback;
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

function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
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

// ------------------------------------------------------------- the list

// ONE call, deliberately.
//
// This was briefly paged, on the theory that PostgREST would silently
// truncate a five-thousand-row response at some ceiling. It killed the
// job: PostgREST re-evaluates the whole function for every page, and
// notable_people takes about 2.6 seconds, so six pages was sixteen
// seconds of database time and the statement timeout cancelled it --
// 'canceling statement due to statement timeout', nine seconds in,
// before a single film had been looked at.
//
// Supabase does not set db-max-rows, so there is no ceiling to defeat.
// Instead of paging, the count is checked: a result landing exactly on
// a round number is the signature of a cap, and worth shouting about
// rather than quietly running on a fraction of the list.
async function loadNotablePeople(): Promise<number[]> {
  const { data, error } = await supabase.rpc('notable_people', {
    p_min_votes: MIN_PERSON_VOTES,
  });
  if (error) throw error;

  const ids = ((data ?? []) as { tmdb_person_id: number }[])
    .map((r) => r.tmdb_person_id)
    .filter((id): id is number => typeof id === 'number');

  if (ids.length === 0) throw new Error('notable_people returned nobody -- refusing to run');
  if (ids.length % 1000 === 0) {
    console.warn(
      `WARNING: got exactly ${ids.length} people, which looks like a row cap rather than an ` +
        `answer. The gate may be running on a fraction of the list.`,
    );
  }
  return ids;
}

// --------------------------------------------------------------- search

async function discoverForPeople(people: number[], from: string, to: string): Promise<number[]> {
  const groups = chunk(people, PEOPLE_PER_QUERY);
  let cappedGroups = 0;

  const perGroup = await runWithConcurrency(groups, DISCOVER_CONCURRENCY, async (group) => {
    const ids: number[] = [];
    // | is OR in TMDB's filter syntax; a comma would be AND and would
    // match nothing, since no film has twenty-five of these people in
    // it. Percent-encoded rather than raw: a bare pipe in a query
    // string is not something to trust every hop to preserve.
    const withPeople = encodeURIComponent(group.join('|'));

    for (let page = 1; page <= MAX_PAGES_PER_GROUP; page++) {
      const data = await tmdbGet(
        `/discover/movie?with_people=${withPeople}` +
          `&primary_release_date.gte=${from}&primary_release_date.lte=${to}` +
          `&without_genres=${DOCUMENTARY_GENRE_ID}` +
          `&sort_by=primary_release_date.asc&page=${page}`,
      );
      if (!data?.results?.length) break;
      for (const r of data.results) ids.push(r.id);
      const total = data.total_pages ?? 1;
      if (page >= total) break;
      if (page === MAX_PAGES_PER_GROUP) {
        cappedGroups++;
        break;
      }
    }

    return ids;
  });

  const found = new Set<number>();
  for (const ids of perGroup) for (const id of ids) found.add(id);

  // Every group hitting the cap means the filter is not filtering --
  // each query is returning the whole calendar rather than these
  // people's films. Said loudly, because the symptom otherwise is just
  // a job that takes too long and a catalogue full of films nobody
  // recognises.
  if (cappedGroups > groups.length / 2) {
    console.warn(
      `WARNING: ${cappedGroups} of ${groups.length} groups hit the ${MAX_PAGES_PER_GROUP}-page ` +
        `cap. with_people is probably not being applied -- check the filter before trusting ` +
        `this run's output.`,
    );
  } else if (cappedGroups > 0) {
    console.log(`${cappedGroups} group(s) hit the page cap; their later films were skipped.`);
  }

  return [...found];
}

async function existingTmdbIds(tmdbIds: number[]): Promise<Set<number>> {
  const found = new Set<number>();
  // Chunked: one .in() with thousands of values makes a URL long enough
  // to be rejected before PostgREST sees it.
  for (const group of chunk(tmdbIds, 500)) {
    const { data, error } = await supabase.from('movies').select('tmdb_id').in('tmdb_id', group);
    if (error) throw error;
    for (const row of data ?? []) found.add(row.tmdb_id as number);
  }
  return found;
}

// ------------------------------------------------------------- hydration

function scoringEligible(input: {
  overview: string | null;
  genres: unknown[];
  keywords: unknown[];
  tagline: string | null;
  voteCount: number;
}): boolean {
  const overviewLen = (input.overview ?? '').trim().length;
  if (input.genres.length === 0) return false;
  return (
    overviewLen >= 100 ||
    (overviewLen >= 40 && (input.keywords.length >= 5 || Boolean(input.tagline?.trim()))) ||
    input.voteCount >= SITE_VISIBLE_VOTES
  );
}

interface Candidate {
  row: Record<string, any>;
  lead: string;
}

async function hydrate(tmdbId: number, notable: Set<number>): Promise<Candidate | null> {
  const d = await tmdbGet(`/movie/${tmdbId}?append_to_response=keywords,credits`);
  if (!d) return null;

  // Same clean-data gate as the other ingests.
  if (d.adult) return null;
  if (!d.poster_path) return null;
  if (!d.overview || d.overview.trim().length === 0) return null;
  if (!d.release_date) return null;

  // Re-checked here as well as at the query: without_genres filters on
  // what /discover knows, and a title can arrive with its genres only
  // populated on the detail response.
  const genreIds: number[] = (d.genres ?? []).map((g: any) => g.id);
  if (genreIds.includes(DOCUMENTARY_GENRE_ID)) return null;

  // No genre at all means scoring_eligible would be false anyway, so
  // the row would sit in the catalogue reachable and unscored forever.
  if ((d.genres?.length ?? 0) === 0) return null;

  // Known-short only. See MIN_FEATURE_RUNTIME.
  if (d.runtime && d.runtime < MIN_FEATURE_RUNTIME) return null;

  const castRaw: any[] = d.credits?.cast ?? [];
  const crewRaw: any[] = d.credits?.crew ?? [];
  const directorsRaw = crewRaw.filter((c) => c.job === 'Director');

  // The second gate. with_people matched this film on somebody, but
  // that somebody may have had two lines. Require a notable name in the
  // top five billed or directing, and record which one it was so the
  // log says why each film was taken.
  const topBilled = castRaw.slice(0, TOP_BILLED);
  const hit =
    topBilled.find((c) => notable.has(c.id)) ?? directorsRaw.find((c) => notable.has(c.id));
  if (!hit) return null;

  const keywords: string[] = (d.keywords?.keywords ?? []).map((k: any) => k.name);

  return {
    lead: hit.name ?? 'unknown',
    row: {
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
      backdrop_url: d.backdrop_path
        ? `https://image.tmdb.org/t/p/w1280${d.backdrop_path}`
        : null,
      vote_average: d.vote_average ?? null,
      vote_count: d.vote_count ?? null,
      popularity: d.popularity ?? null,
      adult: false,
      original_language: d.original_language ?? null,
      imdb_id: d.imdb_id ?? null,
      keywords,
      top_cast: castRaw.slice(0, 10).map((c) => c.name),
      directors: directorsRaw.map((c) => c.name),
      production_companies: (d.production_companies ?? []).map((c: any) => c.name),
      tagline: d.tagline || null,
      // No meaningful popularity rank for a title this new; the column
      // is an import-order artefact rather than a live signal.
      import_rank_popularity: 0,
      hydration_status: 'complete',
      scoring_eligible: scoringEligible({
        overview: d.overview,
        genres: d.genres ?? [],
        keywords,
        tagline: d.tagline ?? null,
        voteCount: d.vote_count ?? 0,
      }),
    },
  };
}

// ---------------------------------------------------------------- main

async function main() {
  const startedAt = new Date().toISOString();
  const from = dateOffset(-BACK_DAYS);
  const to = dateOffset(FORWARD_DAYS);

  console.log(
    `Window ${from} to ${to}. Person vote floor ${MIN_PERSON_VOTES}, cap ${MAX_NEW}.` +
      (DRY_RUN ? ' [DRY RUN]' : ''),
  );

  const people = await loadNotablePeople();
  const notable = new Set(people);
  console.log(`${people.length} notable people, in ${Math.ceil(people.length / PEOPLE_PER_QUERY)} queries.`);

  const candidates = await discoverForPeople(people, from, to);
  console.log(`${candidates.length} films in the window have one of them attached.`);

  const existing = await existingTmdbIds(candidates);
  const fresh = candidates.filter((id) => !existing.has(id));
  console.log(`${fresh.length} are not in the catalogue (${existing.size} already are).`);

  const capped = fresh.slice(0, MAX_NEW);
  if (fresh.length > MAX_NEW) {
    console.warn(
      `CAP HIT: ${fresh.length} new films matched, taking ${MAX_NEW}. ` +
        `Raise MIN_PERSON_VOTES or look at why this jumped before raising MAX_NEW.`,
    );
  }

  let taken = 0;
  let rejected = 0;
  const rows: Record<string, any>[] = [];

  for (const group of chunk(capped, INSERT_BATCH_SIZE)) {
    const hydrated = await runWithConcurrency(group, CONCURRENCY, (id) => hydrate(id, notable));
    for (const h of hydrated) {
      if (!h) {
        rejected++;
        continue;
      }
      taken++;
      rows.push(h.row);
      console.log(
        `  + ${h.row.title} (${h.row.release_year}) -- ${h.lead}` +
          (h.row.scoring_eligible ? '' : ' [not scoring-eligible yet]'),
      );
    }
  }

  console.log(
    `\n${taken} films pass the top-billing check, ${rejected} rejected ` +
      `(documentary, short, bit-part match, or failed the clean-data gate).`,
  );

  if (DRY_RUN) {
    console.log('[DRY RUN] Nothing written. No pipeline_runs row created.');
    return;
  }

  let inserted = 0;
  let failed = 0;
  for (const group of chunk(rows, INSERT_BATCH_SIZE)) {
    const { error } = await supabase.from('movies').upsert(group, { onConflict: 'tmdb_id' });
    if (error) {
      // One bad row discards the whole statement, so fall back per-row
      // rather than lose the rest -- the failure ingest-tv.ts was bitten
      // by on its first full run.
      console.error(`Batch insert failed (${group.length} rows): ${error.message}. Row by row...`);
      for (const row of group) {
        const { error: rowErr } = await supabase
          .from('movies')
          .upsert(row, { onConflict: 'tmdb_id' });
        if (rowErr) {
          failed++;
          console.error(`  tmdb_id=${row.tmdb_id} ("${row.title}") failed: ${rowErr.message}`);
        } else inserted++;
      }
    } else {
      inserted += group.length;
    }
  }

  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'anticipated_ingestion',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: inserted,
    rows_failed: failed,
    status: failed > 0 && inserted === 0 ? 'failed' : 'success',
  });
  if (logError) console.error(`pipeline_runs logging failed (non-fatal): ${logError.message}`);

  console.log(`Done. ${inserted} inserted, ${failed} failed.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
