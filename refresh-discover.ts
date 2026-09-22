// Rebuilds what the Discover tool and the recommendation rails stand
// on, in the one order that is correct.
//
// FOUR STEPS, and the order of the first two is not arbitrary:
//
//   1. tv_shows.derived_genres -- genres TMDB's television taxonomy
//      does not have. It gives TV sixteen genres and Horror is not one
//      of them, though it is one for film, so Stranger Things is filed
//      "Action & Adventure, Mystery, Sci-Fi & Fantasy", The Last of Us
//      is "Drama" and Hannibal is "Drama, Crime". Romance nominally
//      exists but 15 shows in the whole catalogue carry it, because
//      romantic series land under Drama and Soap.
//
//      These are inferred from a keyword gate plus the emotional
//      fingerprint -- see refresh_tv_derived_genres in the database for
//      why it is that way round and not either signal alone.
//
//   2. discover_genre_profiles / discover_genre_pools -- the per-genre
//      centroids and spreads the sliders are built from. These read
//      genres || derived_genres, so running this BEFORE step 1 would
//      build profiles for a Horror genre that does not exist yet, or
//      worse, build them from last week's membership.
//
//   3/4. movie_facet_df and tv_facet_df -- how many scored titles carry
//      each genre, keyword, director or creator, cast member, studio or
//      network, language and cluster. The consensus engine weighs what a
//      collection has in common by how rare it is, which is the only
//      thing separating "all seven are dramas" from "all seven are
//      Studio Ghibli", and a value rare enough gets turned into a hard
//      filter. Stale counts make that judgement quietly wrong in the
//      direction of over-filtering.
//
//      Two tables, not one with a media column: the catalogues have
//      different sizes and different scales, so a count from one says
//      nothing about rarity in the other. The TV counts read
//      genres || derived_genres, which is why they come after step 1.
//
// Weekly because that is the cadence of the scoring pipeline feeding
// it: there is nothing to be gained by being more current than the
// data.
//
// TWO WAYS A FUNCTION THAT WORKS IN THE SQL EDITOR FAILS FROM HERE, both
// of which this job has already been bitten by. The editor connects as
// postgres; this connects through PostgREST, as a role with different
// rules:
//
//   - pg_safeupdate is loaded in that session and rejects any UPDATE or
//     DELETE without a WHERE clause. refresh_discover_genre_profiles
//     returned 400 on its first scheduled run after a dozen clean runs
//     by hand.
//   - statement_timeout is 8 seconds. refresh_movie_facet_df takes 6.8
//     and was cancelled the first time the job ran it. Anything near
//     that has to set its own timeout on the function, as
//     refresh_tv_derived_genres and both facet recounts now do.
//
// Anything added below has to be proved through the API, not the editor.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// Everything supabase-js knows about a failure, not just the headline.
//
// Two different kinds of thing arrive as `error` and they need opposite
// treatment. A PostgREST refusal carries a code (a SQLSTATE like 42501,
// or a PGRSTnnn) and a usable message. A transport failure -- the
// request never completing -- arrives as a stringified undici
// TypeError with an empty code, and undici puts the only useful part,
// the reason, on `cause`: ENOTFOUND, ECONNRESET, UND_ERR_CONNECT_TIMEOUT.
// Printing `.message` alone reduces every one of those to the same
// four words, "TypeError: fetch failed", which is how the first run of
// this job failed with nothing to go on.
function describe(err: unknown): string {
  if (err && typeof err === 'object') {
    const e = err as Record<string, unknown>;
    const parts = [String(e.message ?? e)];
    if (e.code) parts.push(`code=${String(e.code)}`);
    if (e.details) parts.push(`details=${String(e.details)}`);
    if (e.hint) parts.push(`hint=${String(e.hint)}`);
    if (e.cause) parts.push(`cause=${describe(e.cause)}`);
    if (e.errno) parts.push(`errno=${String(e.errno)}`);
    if (e.syscall) parts.push(`syscall=${String(e.syscall)}`);
    return parts.join(' | ');
  }
  return String(err);
}

// A refusal is an answer: the same call will be refused the same way in
// ten seconds, so retrying it only delays the report. A dropped or
// unmade connection is not an answer, and on a hosted runner it is
// routinely a one-off. Only the second kind is worth another attempt.
function isTransportFailure(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false;
  const e = err as Record<string, unknown>;
  if (e.code) return false; // PostgREST or Postgres said something.
  const text = `${String(e.message ?? '')} ${String(e.details ?? '')}`.toLowerCase();
  return (
    text.includes('fetch failed') ||
    text.includes('network') ||
    text.includes('socket') ||
    text.includes('econnreset') ||
    text.includes('enotfound') ||
    text.includes('eai_again') ||
    text.includes('etimedout')
  );
}

const ATTEMPTS = 3;

async function rpc<T>(name: string): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    const { data, error } = await supabase.rpc(name);
    if (!error) return data as T;

    if (!isTransportFailure(error) || attempt === ATTEMPTS) {
      throw new Error(`${name} failed: ${describe(error)}`);
    }

    const waitMs = 5000 * attempt;
    console.warn(
      `  ${name} did not reach the database: ${describe(error)}\n` +
        `  Retrying in ${waitMs / 1000}s (attempt ${attempt + 1} of ${ATTEMPTS}).`,
    );
    await sleep(waitMs);
  }
}

// Both facet recounts fail the same way and for the same reason, so
// they are checked the same way. An empty facet table does not break
// the consensus engine, which is worse than if it did: every idf would
// collapse to the same number, nothing would ever qualify as rare, and
// the rails would quietly go back to recommending whatever sits nearest
// the average.
async function recountFacets(fn: string, label: string): Promise<number> {
  console.log(`Recounting ${label} facets...`);
  const rows = (await rpc<{ facet_type: string; values_kept: number }[] | null>(fn)) ?? [];
  for (const row of rows) {
    console.log(`  ${row.facet_type}: ${row.values_kept} values.`);
  }

  const total = rows.reduce((sum, r) => sum + (r.values_kept ?? 0), 0);
  if (total === 0) {
    throw new Error(`Refusing to report success: no ${label} facet counts written.`);
  }
  return total;
}

const startedAt = new Date().toISOString();

async function main() {
  console.log('Deriving TV genres TMDB does not have...');
  const derivedRows = await rpc<{ out_genre: string; out_shows: number }[] | null>(
    'refresh_tv_derived_genres',
  );
  const derived = derivedRows ?? [];
  for (const row of derived) {
    console.log(`  ${row.out_genre}: ${row.out_shows} shows.`);
  }

  // An empty result means every show stopped qualifying, which in
  // practice means the rules table was emptied or the fingerprints went
  // missing. Worth failing on: the next step would then quietly drop
  // Horror and Romance out of the Discover picker with no other signal.
  if (derived.length === 0) {
    throw new Error('No derived genres produced -- refusing to rebuild profiles on an empty set.');
  }

  console.log('Rebuilding Discover genre profiles...');
  const profileRows = await rpc<
    { media: string; genres_kept: number; rows_written: number }[] | null
  >('refresh_discover_genre_profiles');
  const profiles = profileRows ?? [];
  for (const row of profiles) {
    console.log(`  ${row.media}: ${row.genres_kept} genres, ${row.rows_written} rows.`);
  }

  const totalGenres = profiles.reduce((sum, r) => sum + (r.genres_kept ?? 0), 0);
  if (totalGenres === 0) {
    throw new Error('Refusing to report success: no genre profiles written.');
  }

  const movieFacets = await recountFacets('refresh_movie_facet_df', 'film');
  const tvFacets = await recountFacets('refresh_tv_facet_df', 'television');

  await logRun(
    'success',
    profiles.reduce((sum, r) => sum + (r.rows_written ?? 0), 0) + movieFacets + tvFacets,
  );
  console.log('Done.');
}

// Logging is best-effort in both directions. On success it is the only
// record that the job ran at all, which is what pipeline-health reads.
// On failure it is worth attempting even though the database may be
// exactly what could not be reached -- if the failure was in a later
// step, the first call proved the connection works, and a failure row
// is the difference between a job that stopped and a job nobody can
// tell has stopped.
async function logRun(status: 'success' | 'failed', rows: number) {
  const { error } = await supabase.from('pipeline_runs').insert({
    run_type: 'discover_refresh',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: rows,
    rows_failed: status === 'failed' ? 1 : 0,
    status,
  });
  if (error) console.error(`pipeline_runs logging failed (non-fatal): ${describe(error)}`);
}

main().catch(async (err) => {
  console.error(`Fatal error: ${describe(err)}`);
  if (err instanceof Error && err.stack) console.error(err.stack);
  try {
    await logRun('failed', 0);
  } catch {
    // The database being unreachable is the likeliest reason to be
    // here; it must not replace the real error with its own.
  }
  process.exit(1);
});
