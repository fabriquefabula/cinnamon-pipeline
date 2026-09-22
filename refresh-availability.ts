// Where each title can be streamed, rented or bought, per country.
//
// Source is TMDB's /watch/providers endpoint, whose data comes from
// JustWatch. TMDB's terms are explicit that the source must be
// attributed to JustWatch wherever it is shown, and that they will
// revoke API access over it -- see components/WhereToWatch.tsx in
// cinnamon-web, which carries that credit.
//
// What is stored is deliberately thin: provider NAMES, grouped by
// country and by how you get it. No logo paths, no per-provider links,
// no display_priority. That is not laziness about the UI -- the whole
// design depends on it. The column is embedded in a statically rendered
// page so the viewer's own browser can pick its country out of it, with
// no per-view request and no geo-IP. Carrying artwork for ~200 countries
// per title would make that payload too big to ship, and the fallback --
// fetching per view -- is a function invocation on every page load,
// which is the cost line this project has already had to cut once.
//
// Scope: titles a visitor can actually reach, plus anything any user has
// saved, so an obscure film someone put on their watchlist still shows
// availability. About 17,500 of the 78,000 in the catalogue.
// Availability churns constantly, so a daily call for a title nobody can
// find is pure waste.
//
// Required env vars: TMDB_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const TMDB_API_KEY = requireEnv('TMDB_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

// Reachable means the LOWEST floor any surface uses, which is not the
// floor the site's rows use -- the mistake this started with. Discover
// admits TV from 50 votes, so a floor of 180 here left 3,446 of the
// 6,199 shows Discover can return with no availability ever fetched,
// while film, whose Discover floor of 400 sits above this job's 300,
// came out 99% covered and gave no sign anything was wrong.
//
// So: whenever a surface is given a lower floor than these, this is the
// other half of that change.
const MOVIE_VOTE_FLOOR = 300;
const TV_VOTE_FLOOR = 50;

const CONCURRENCY = 20; // same as the other TMDB jobs
// 100, not 500. The first working run wrote 9,051 rows and failed
// exactly 5,000 -- ten whole batches, which is the signature of the
// request body being rejected rather than of bad data. A batch of 500
// popular titles carries availability for dozens of countries each and
// runs to megabytes; a batch of obscure ones does not, which is why
// some batches went through and others did not.
const WRITE_BATCH = 100;
const PAGE_SIZE = 1000;

// Ordered as they are displayed. 'free' and 'ads' only appear for some
// countries, and are worth keeping: "free with ads" is a real answer to
// "where can I watch this".
const KINDS = ['flatrate', 'free', 'ads', 'rent', 'buy'] as const;

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

interface Title {
  id: string;
  tmdb_id: number;
}

interface Row {
  id: string;
  providers: Record<string, Record<string, string[]>> | null;
  refreshed_at: string;
}

// Where saved titles are read from. `column` is the title id to collect;
// `orderBy` is that table's PRIMARY KEY, and it has to be, because range
// paging over a non-unique sort key is not stable between requests --
// two users save the same film, two lists hold the same show, and a row
// sitting on a page boundary can come back twice or not at all.
const SAVED_SOURCES = {
  movies: [
    { table: 'user_movies', column: 'movie_id', orderBy: ['id'] },
    { table: 'list_movies', column: 'movie_id', orderBy: ['list_id', 'movie_id'] },
  ],
  tv_shows: [
    { table: 'user_tv_shows', column: 'show_id', orderBy: ['id'] },
    { table: 'list_tv_shows', column: 'show_id', orderBy: ['list_id', 'show_id'] },
  ],
} as const;

async function pageAll(build: (from: number, to: number) => any): Promise<Title[]> {
  const rows: Title[] = [];
  let from = 0;
  for (;;) {
    const { data, error } = await build(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = (data ?? []) as Title[];
    rows.push(...page);
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return rows;
}

// Two passes rather than one clever query: PostgREST cannot express
// "above the floor OR referenced by one of four other tables" in a
// single filter, and doing it in SQL would mean a view to maintain.
// Merged on id here instead.
async function scopedTitles(
  table: 'movies' | 'tv_shows',
  voteFloor: number,
  savedFrom: readonly { table: string; column: string; orderBy: readonly string[] }[],
): Promise<Title[]> {
  const byFloor = await pageAll((from, to) =>
    supabase
      .from(table)
      .select('id, tmdb_id')
      .eq('scoring_status', 'scored')
      .gte('vote_count', voteFloor)
      .order('id', { ascending: true })
      .range(from, to),
  );

  // Paged, not limited. A flat .limit() stops collecting saved titles
  // the moment one of these tables outgrows it, and the only symptom is
  // availability quietly missing from somebody's watchlist.
  const savedIds = new Set<string>();
  for (const src of savedFrom) {
    const cols = Array.from(new Set([src.column, ...src.orderBy])).join(', ');
    let from = 0;
    for (;;) {
      let q = supabase.from(src.table).select(cols);
      for (const col of src.orderBy) q = q.order(col, { ascending: true });
      const { data, error } = await q.range(from, from + PAGE_SIZE - 1);
      if (error) throw error;
      const page = data ?? [];
      for (const row of page) {
        const v = (row as any)[src.column];
        if (v) savedIds.add(v as string);
      }
      if (page.length < PAGE_SIZE) break;
      from += PAGE_SIZE;
    }
  }

  const have = new Set(byFloor.map((t) => t.id));
  const missing = [...savedIds].filter((id) => !have.has(id));

  const extra: Title[] = [];
  for (let i = 0; i < missing.length; i += 500) {
    const chunk = missing.slice(i, i + 500);
    const { data, error } = await supabase.from(table).select('id, tmdb_id').in('id', chunk);
    if (error) throw error;
    extra.push(...((data ?? []) as Title[]));
  }

  return [...byFloor, ...extra];
}

// TMDB returns, per country, arrays of provider objects plus a `link`.
// Everything but the names is dropped. Countries with nothing at all are
// dropped too, so the stored object holds only places the title can
// actually be watched.
function compact(results: any): Record<string, Record<string, string[]>> | null {
  const out: Record<string, Record<string, string[]>> = {};
  for (const [country, entry] of Object.entries(results ?? {})) {
    const bucket: Record<string, string[]> = {};
    for (const kind of KINDS) {
      const list = (entry as any)?.[kind];
      if (!Array.isArray(list) || list.length === 0) continue;
      // De-duplicated: the same service often appears twice in one
      // country under regional sub-brands with different provider_ids.
      const names = Array.from(
        new Set(list.map((p: any) => p?.provider_name).filter((n: any) => typeof n === 'string' && n)),
      );
      if (names.length > 0) bucket[kind] = names;
    }
    if (Object.keys(bucket).length > 0) out[country] = bucket;
  }
  return Object.keys(out).length > 0 ? out : null;
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

// Halves on failure, down to single rows. The batch size above avoids
// the size cliff in the common case; this is what stops one oversized
// or malformed row taking the other 99 with it -- the failure mode
// ingest-tv.ts hit on its first full run, where a single duplicate in a
// 500-row statement discarded all 500.
async function writeRows(media: 'movie' | 'tv', rows: Row[]): Promise<{ written: number; failed: number }> {
  if (rows.length === 0) return { written: 0, failed: 0 };

  const { data, error } = await supabase.rpc('apply_watch_providers', {
    p_media: media,
    p_rows: rows,
  });

  if (!error) {
    // apply_watch_providers is an UPDATE ... FROM, so it can never
    // create a row. It returns how many it matched; a shortfall means
    // ids that are not in the catalogue, which is worth counting rather
    // than reporting as success.
    const matched = Number(data ?? 0);
    if (matched !== rows.length) {
      console.error(`${media}: ${rows.length - matched} id(s) matched no row.`);
    }
    return { written: matched, failed: rows.length - matched };
  }

  if (rows.length === 1) {
    console.error(`${media}: row ${rows[0].id} failed: ${error.message}`);
    return { written: 0, failed: 1 };
  }

  const mid = Math.floor(rows.length / 2);
  const left = await writeRows(media, rows.slice(0, mid));
  const right = await writeRows(media, rows.slice(mid));
  return { written: left.written + right.written, failed: left.failed + right.failed };
}

async function refresh(
  media: 'movie' | 'tv',
  titles: Title[],
): Promise<{ written: number; failed: number; withProviders: number }> {
  let written = 0;
  let failed = 0;
  let withProviders = 0;

  for (let i = 0; i < titles.length; i += WRITE_BATCH) {
    const batch = titles.slice(i, i + WRITE_BATCH);
    const stamp = new Date().toISOString();

    const fetched = await runWithConcurrency(batch, CONCURRENCY, async (t) => {
      const d = await tmdbGet(`/${media}/${t.tmdb_id}/watch/providers`);
      if (!d) return null;
      const providers = compact(d.results);
      if (providers) withProviders++;
      return { id: t.id, providers, refreshed_at: stamp } as Row;
    });

    const good = fetched.filter((r): r is Row => r !== null);
    failed += fetched.length - good.length;

    const result = await writeRows(media, good);
    written += result.written;
    failed += result.failed;

    console.log(
      `${media} ${Math.min(i + WRITE_BATCH, titles.length)}/${titles.length}: ${written} written, ${withProviders} with availability, ${failed} failed`,
    );
  }

  return { written, failed, withProviders };
}

async function main() {
  const startedAt = new Date().toISOString();

  console.log('Collecting titles in scope...');
  const movies = await scopedTitles('movies', MOVIE_VOTE_FLOOR, SAVED_SOURCES.movies);
  const shows = await scopedTitles('tv_shows', TV_VOTE_FLOOR, SAVED_SOURCES.tv_shows);
  console.log(`${movies.length} movies, ${shows.length} shows.`);

  const m = await refresh('movie', movies);
  const t = await refresh('tv', shows);

  const processed = m.written + t.written;
  const failedTotal = m.failed + t.failed;

  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'availability_refresh',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: processed,
    rows_failed: failedTotal,
    status: failedTotal > 0 && processed === 0 ? 'failed' : 'success',
  });
  if (logError) console.error(`pipeline_runs logging failed (non-fatal): ${logError.message}`);

  console.log(
    `\nDone. ${processed} written (${m.withProviders + t.withProviders} have availability somewhere), ${failedTotal} failed.`,
  );
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
