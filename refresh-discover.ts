// Rebuilds what the Discover tool stands on, in the one order that is
// correct.
//
// TWO STEPS, and the order is not arbitrary:
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
// Weekly because that is the cadence of the scoring pipeline feeding
// it: there is nothing to be gained by being more current than the
// data.
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

async function main() {
  const startedAt = new Date().toISOString();

  console.log('Deriving TV genres TMDB does not have...');
  const { data: derived, error: derivedErr } = await supabase.rpc('refresh_tv_derived_genres');
  if (derivedErr) throw new Error(`refresh_tv_derived_genres failed: ${derivedErr.message}`);

  const derivedRows = (derived ?? []) as { out_genre: string; out_shows: number }[];
  for (const row of derivedRows) {
    console.log(`  ${row.out_genre}: ${row.out_shows} shows.`);
  }

  // An empty result means every show stopped qualifying, which in
  // practice means the rules table was emptied or the fingerprints went
  // missing. Worth failing on: the next step would then quietly drop
  // Horror and Romance out of the Discover picker with no other signal.
  if (derivedRows.length === 0) {
    throw new Error('No derived genres produced -- refusing to rebuild profiles on an empty set.');
  }

  console.log('Rebuilding Discover genre profiles...');
  const { data: profiles, error: profileErr } = await supabase.rpc(
    'refresh_discover_genre_profiles',
  );
  if (profileErr) throw new Error(`refresh_discover_genre_profiles failed: ${profileErr.message}`);

  const profileRows = (profiles ?? []) as {
    media: string;
    genres_kept: number;
    rows_written: number;
  }[];
  for (const row of profileRows) {
    console.log(`  ${row.media}: ${row.genres_kept} genres, ${row.rows_written} rows.`);
  }

  const totalGenres = profileRows.reduce((sum, r) => sum + (r.genres_kept ?? 0), 0);
  if (totalGenres === 0) {
    throw new Error('Refusing to report success: no genre profiles written.');
  }

  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'discover_refresh',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: profileRows.reduce((sum, r) => sum + (r.rows_written ?? 0), 0),
    rows_failed: 0,
    status: 'success',
  });
  if (logError) console.error(`pipeline_runs logging failed (non-fatal): ${logError.message}`);

  console.log('Done.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
