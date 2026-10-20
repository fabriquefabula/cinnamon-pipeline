// Film series sweep: records which series (TMDB "collection") each film
// belongs to -- Jurassic Park Collection, The Maze Runner Collection,
// Godzilla (Showa) Collection -- so a recommendation list can hold at most
// a few films from one series instead of filling up with sequels.
//
// The first run looks up every film in the catalogue. After that it only
// looks up films that have never been checked, so the daily scheduled run
// picks up whatever the ingests added since. A film that belongs to no
// series is still marked as checked (collection_checked_at), so it is
// never fetched twice.
//
// Safe to re-run or interrupt: only unchecked films are fetched, and a
// film is marked checked in the same update that stores its series. A
// TMDB error other than 404 leaves the film unchecked for the next run.
//
// Required env vars: TMDB_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=true, MOVIE_LIMIT=<n>

import { createClient } from '@supabase/supabase-js';
import { GATEWAY_STATUSES, resilientFetch } from './resilient-fetch';

const TMDB_API_KEY = requireEnv('TMDB_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const CONCURRENCY = 12; // under TMDB's ~40-50 req/s soft limit
const PAGE_SIZE = 500;
const MAX_ATTEMPTS = 4;

// A dropped connection or a gateway error is sent again where that is
// safe (see resilient-fetch.ts), instead of ending the run. This job only
// reads and updates, and both are safe to send twice. The TMDB lookups
// keep their own loop in fetchCollection, which already sends a request
// again on a dropped connection, a 429 or a 5xx, so they do not go
// through resilientFetch as well.
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  global: { fetch: resilientFetch({ label: 'database', retryOn: GATEWAY_STATUSES }) },
});

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

type MovieRef = { id: string; tmdb_id: number };

type Lookup =
  | { kind: 'found'; collectionId: number | null; collectionName: string | null }
  | { kind: 'gone' } // 404: the id no longer exists on TMDB
  | { kind: 'failed' };

async function fetchCollection(tmdbId: number): Promise<Lookup> {
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    try {
      const res = await fetch(`https://api.themoviedb.org/3/movie/${tmdbId}`, {
        headers: { Authorization: `Bearer ${TMDB_API_KEY}`, accept: 'application/json' },
        signal: controller.signal,
      });
      if (res.status === 404) return { kind: 'gone' };
      if (res.status === 429 || res.status >= 500) {
        // Rate limited or a TMDB hiccup: back off and try again.
        const retryAfter = Number(res.headers.get('retry-after'));
        await sleep(Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1000 : 1000 * attempt);
        continue;
      }
      if (!res.ok) {
        console.error(`TMDB ${res.status} for tmdb_id=${tmdbId}`);
        return { kind: 'failed' };
      }
      const d = await res.json();
      const c = d.belongs_to_collection;
      return {
        kind: 'found',
        collectionId: c?.id ?? null,
        collectionName: c?.name ?? null,
      };
    } catch (err) {
      if (attempt === MAX_ATTEMPTS) {
        const reason = err instanceof Error && err.name === 'AbortError' ? 'timeout' : err;
        console.error(`Fetch failed for tmdb_id=${tmdbId}:`, reason);
        return { kind: 'failed' };
      }
      await sleep(1000 * attempt);
    } finally {
      clearTimeout(timeout);
    }
  }
  return { kind: 'failed' };
}

async function main() {
  const dryRun = process.env.DRY_RUN === 'true';
  const movieLimit = process.env.MOVIE_LIMIT ? parseInt(process.env.MOVIE_LIMIT, 10) : undefined;

  console.log(
    `Starting film series sweep${dryRun ? ' (DRY RUN)' : ''}${movieLimit ? `, limit=${movieLimit}` : ''}...`,
  );

  let cursor: string | null = null;
  let processed = 0;
  let inSeries = 0;
  let noSeries = 0;
  let gone = 0;
  let failures = 0;

  outer: while (true) {
    let query = supabase
      .from('movies')
      .select('id, tmdb_id')
      .is('collection_checked_at', null)
      .not('tmdb_id', 'is', null)
      .order('id')
      .limit(PAGE_SIZE);

    if (cursor) query = query.gt('id', cursor);

    const { data, error } = await query;
    if (error) {
      console.error('Failed to fetch movies batch:', error.message);
      process.exitCode = 1;
      break;
    }
    const page = (data ?? []) as MovieRef[];
    if (page.length === 0) break;

    cursor = page[page.length - 1].id;

    for (let i = 0; i < page.length; i += CONCURRENCY) {
      const chunk = page.slice(i, i + CONCURRENCY);
      await Promise.all(
        chunk.map(async (m) => {
          if (dryRun) {
            processed++;
            return;
          }
          const lookup = await fetchCollection(m.tmdb_id);
          if (lookup.kind === 'failed') {
            failures++;
            processed++;
            return;
          }
          const patch =
            lookup.kind === 'found'
              ? {
                  collection_id: lookup.collectionId,
                  collection_name: lookup.collectionName,
                  collection_checked_at: new Date().toISOString(),
                }
              : { collection_id: null, collection_name: null, collection_checked_at: new Date().toISOString() };
          const { error: updateError } = await supabase.from('movies').update(patch).eq('id', m.id);
          if (updateError) {
            console.error(`Update failed for movie ${m.id}:`, updateError.message);
            failures++;
          } else if (lookup.kind === 'gone') {
            gone++;
          } else if (lookup.collectionId !== null) {
            inSeries++;
          } else {
            noSeries++;
          }
          processed++;
        }),
      );

      if (processed % 1000 < CONCURRENCY) {
        console.log(
          `Processed ${processed}: ${inSeries} in a series, ${noSeries} in none, ${gone} gone from TMDB, ${failures} failures...`,
        );
      }

      if (movieLimit && processed >= movieLimit) break outer;
    }
  }

  console.log(
    `Done. Processed ${processed}: ${inSeries} in a series, ${noSeries} in none, ${gone} gone from TMDB, ${failures} failures` +
      (failures > 0 ? ' (left unchecked; the next run retries them).' : '.'),
  );
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
