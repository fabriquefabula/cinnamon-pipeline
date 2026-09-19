// Rebuilds the precomputed mood axis scores for both catalogues.
//
// movie_axis_scores and tv_axis_scores hold one row per (title, axis) --
// roughly 1.4M and 300k respectively. They exist because computing those
// scores at query time cost ~5s per search; precomputed, a descriptive
// search runs in about 1.5s and a name lookup in 55ms.
//
// They go stale on two events, and only these two:
//
//   1. A title is scored or rescored. New arrivals have no rows at all,
//      so they are invisible to mood search until this runs.
//   2. An axis is retuned in mood_axes. Every row for that axis is then
//      wrong, and the search silently ranks on the old direction.
//
// The second is the dangerous one: nothing about the site looks broken,
// the results are just quietly built on superseded weights. This runs
// daily rather than on a trigger because a full rebuild takes ~30s and
// there is no benefit to being more current than the scoring pipeline
// that feeds it, which is weekly.
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

  console.log('Rebuilding movie axis scores...');
  const { data: movieRows, error: movieErr } = await supabase.rpc('refresh_movie_axis_scores');
  if (movieErr) throw new Error(`refresh_movie_axis_scores failed: ${movieErr.message}`);
  console.log(`  ${movieRows} rows.`);

  console.log('Rebuilding TV axis scores...');
  const { data: tvRows, error: tvErr } = await supabase.rpc('refresh_tv_axis_scores');
  if (tvErr) throw new Error(`refresh_tv_axis_scores failed: ${tvErr.message}`);
  console.log(`  ${tvRows} rows.`);

  // A rebuild that writes nothing means the scored catalogue came back
  // empty -- the table has just been truncated, so mood search is now
  // returning nothing at all. Worth failing loudly for.
  if ((movieRows ?? 0) === 0 || (tvRows ?? 0) === 0) {
    throw new Error(`Refusing to report success: movie=${movieRows}, tv=${tvRows}`);
  }

  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'mood_axis_refresh',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: (movieRows ?? 0) + (tvRows ?? 0),
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
