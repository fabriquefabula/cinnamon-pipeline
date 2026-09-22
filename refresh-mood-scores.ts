// Rebuilds the precomputed search indexes for both catalogues.
//
// TWO LAYERS, same trigger:
//
// movie_axis_scores and tv_axis_scores hold one row per (title, axis) --
// roughly 1.4M and 300k respectively. They exist because computing those
// scores at query time cost ~5s per search; precomputed, a descriptive
// search runs in about 1.5s and a name lookup in 55ms.
//
// The premise index (premise_docs / premise_tokens / premise_idf /
// premise_vocab / premise_cooc / premise_assoc) is what answers a query
// that describes what HAPPENS in a film -- "a woman returns to her
// hometown after her father dies" -- by searching the essence_summary the
// scoring pass wrote for every title, and by knowing which words tend to
// appear together in those summaries.
//
// Both go stale on the same two events, and only these two:
//
//   1. A title is scored or rescored. New arrivals have no rows at all,
//      so they are invisible to mood search, and absent from premise
//      search, until this runs.
//   2. An axis is retuned in mood_axes. Every row for that axis is then
//      wrong, and the search silently ranks on the old direction.
//
// The second is the dangerous one: nothing about the site looks broken,
// the results are just quietly built on superseded weights.
//
// ONE AXIS PER CALL. This used to be a single RPC that truncated both
// tables and rebuilt every axis at once. At 29 axes x 49,448 films that
// is 1,433,992 rows and a little over two minutes, and it started
// failing with "upstream request timeout" -- the HTTP gateway in front
// of PostgREST giving up, not the database, whose statement_timeout for
// that function is 900s. A single call cannot be made to outlast the
// gateway, so it has to stop being a single call.
//
// Rebuilding per axis also removes an outage window nobody had noticed:
// the old function truncated the whole table inside its transaction, so
// for the two minutes it ran, a concurrent mood search matched nothing
// at all. Deleting and reinserting one axis leaves the other 28 intact.
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

  // Driven off the table rather than a hardcoded list, so retuning or
  // adding an axis needs no change here.
  const { data: axisRows, error: axisErr } = await supabase
    .from('mood_axes')
    .select('axis')
    .order('axis');
  if (axisErr) throw new Error(`Could not read mood_axes: ${axisErr.message}`);

  const axes = (axisRows ?? []).map((r) => r.axis as string);
  if (axes.length === 0) throw new Error('mood_axes is empty -- nothing to rebuild.');

  console.log(`Rebuilding ${axes.length} axes...`);

  let movieTotal = 0;
  let tvTotal = 0;

  // Sequential on purpose. These are the heaviest writes the database
  // takes all week, and running them in parallel would buy a couple of
  // minutes of wall clock in exchange for contending with whatever is
  // serving the site at the time.
  for (const axis of axes) {
    const t0 = Date.now();

    const { data: movieRows, error: movieErr } = await supabase.rpc(
      'refresh_movie_axis_scores_for',
      { p_axis: axis },
    );
    if (movieErr) throw new Error(`refresh_movie_axis_scores_for('${axis}') failed: ${movieErr.message}`);

    const { data: tvRows, error: tvErr } = await supabase.rpc('refresh_tv_axis_scores_for', {
      p_axis: axis,
    });
    if (tvErr) throw new Error(`refresh_tv_axis_scores_for('${axis}') failed: ${tvErr.message}`);

    movieTotal += movieRows ?? 0;
    tvTotal += tvRows ?? 0;

    // Per-axis timing, so the next time this creeps toward a limit it is
    // visible in the log before it starts failing.
    console.log(
      `  ${axis}: ${movieRows} films, ${tvRows} shows (${((Date.now() - t0) / 1000).toFixed(1)}s)`,
    );
  }

  // One ANALYZE after the loop rather than 29 inside it.
  const { error: analyzeErr } = await supabase.rpc('analyze_axis_scores');
  if (analyzeErr) console.error(`analyze_axis_scores failed (non-fatal): ${analyzeErr.message}`);

  console.log(`Total: ${movieTotal} film rows, ${tvTotal} show rows.`);

  // A rebuild that writes nothing means the scored catalogue came back
  // empty, so mood search is now returning nothing at all. Worth failing
  // loudly for.
  if (movieTotal === 0 || tvTotal === 0) {
    throw new Error(`Refusing to report success: movie=${movieTotal}, tv=${tvTotal}`);
  }

  // Deliberately non-fatal, unlike the axis rebuild above. An empty axis
  // table breaks mood search outright; a stale premise index only means
  // the newest titles rank on their keywords rather than on what they are
  // about, which is a degradation and not an outage. Failing the whole
  // run over it would throw away a successful axis rebuild.
  console.log('Rebuilding the premise index...');
  const { data: premise, error: premiseErr } = await supabase.rpc('refresh_premise_index');
  if (premiseErr) {
    console.error(`refresh_premise_index failed (non-fatal): ${premiseErr.message}`);
  } else {
    const row = Array.isArray(premise) ? premise[0] : premise;
    console.log(
      `  ${row?.docs ?? '?'} docs, ${row?.vocab ?? '?'} vocabulary, ${row?.assoc ?? '?'} associations.`,
    );
  }

  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'mood_axis_refresh',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: movieTotal + tvTotal,
    rows_failed: 0,
    status: 'success',
  });
  // Non-fatal, but no longer silent in practice: 'mood_axis_refresh' was
  // missing from the pipeline_runs run_type CHECK constraint, so this
  // insert had failed on every successful run since the job was written
  // and the table held no record of it at all. The constraint now allows
  // it; this line stays non-fatal so a logging problem still cannot fail
  // a rebuild that worked.
  if (logError) console.error(`pipeline_runs logging failed (non-fatal): ${logError.message}`);

  console.log('Done.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
