// Daily health check for the whole pipeline.
//
// This exists because of a specific failure: there was no way to answer
// "is everything still running?" from the database. Only the two ingest
// scripts logged anything at all, so most jobs left no trace whether they
// succeeded, failed, or silently stopped being scheduled.
//
// It deliberately measures OUTCOMES rather than whether jobs reported in.
// A workflow that stops running never writes a failure row anywhere, so
// per-run logging cannot detect it by construction -- but the thing it was
// supposed to produce going stale can be detected, whatever the cause.
//
// TWO STATES ONLY: pass or fail. There was a 'warn' tier and it was
// removed, because a row that is neither actionable nor ignorable is the
// worst of both -- it trains you to skim the output, and the next real
// failure gets skimmed with it. Anything worth a human's attention fails
// the run. Everything else is printed as context and nothing more.
//
// On any FAIL this process exits non-zero, which fails the workflow and
// triggers GitHub's own notification to the repository owner.
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

const HOUR = 60 * 60 * 1000;
const DAY = 24 * HOUR;

// Thresholds live here rather than in pipeline_health() so they can be
// tuned in a pull request instead of a migration. Each is set with real
// slack against the job's own cadence -- a weekly job is not late at eight
// days -- because a check that cries wolf gets ignored, which is the same
// end state as having no check.
const LIMITS = {
  movieIngestMaxAge: 9 * DAY, // weekly, Sundays 09:00 UTC
  tvIngestMaxAge: 9 * DAY, // weekly, Sundays 11:00 UTC
  popularityMaxAge: 48 * HOUR, // daily, 07:00 UTC
  tvPopularityMaxAge: 48 * HOUR, // daily, 07:30 UTC
  availabilityMaxAge: 48 * HOUR, // daily, 08:30 UTC
  upcomingIngestMaxAge: 48 * HOUR, // daily, 06:00 UTC
  movieRailMaxAge: 48 * HOUR, // daily, 13:00 UTC

  // How long a scored row may sit unprocessed before it counts as stuck.
  //
  // This replaced three separate limits on queue DEPTH, which measured the
  // wrong thing. A scoring batch lands several hundred rows at once and the
  // downstream jobs drain roughly two hundred a day, so a deep queue is the
  // normal state of a healthy pipeline the night after a batch -- and the
  // old limits duly failed the run for it. Meanwhile a single row stuck for
  // thirteen days passed, because one is less than three hundred.
  //
  // Both of those happened. Depth says nothing about health; age says all of
  // it. Three days is one batch's drain time plus a full day of slack.
  drainMaxAge: 72 * HOUR,
};

type Severity = 'ok' | 'fail';

interface Check {
  name: string;
  severity: Severity;
  detail: string;
}

interface Health {
  checked_at: string;
  last_movie_ingest: string | null;
  last_tv_ingest: string | null;
  last_popularity_refresh: string | null;
  last_tv_popularity_refresh: string | null;
  last_availability_refresh: string | null;
  last_upcoming_ingestion: string | null;
  last_movie_rail: string | null;
  last_tv_rail: string | null;
  movies_awaiting_rails: number;
  movies_awaiting_rails_oldest: string | null;
  tv_awaiting_clusters: number;
  tv_awaiting_clusters_oldest: string | null;
  tv_awaiting_rails: number;
  tv_awaiting_rails_oldest: string | null;
  movies_pending_eligible: number;
  tv_pending_eligible: number;
  movies_orphaned_submitted: number;
  tv_orphaned_submitted: number;
  tv_clusters_without_description: number;
  movie_clusters_without_description: number;
}

function ageMs(iso: string | null, now: number): number | null {
  if (!iso) return null;
  return now - new Date(iso).getTime();
}

function fmtAge(ms: number): string {
  const hours = ms / HOUR;
  return hours < 48 ? `${hours.toFixed(1)}h` : `${(ms / DAY).toFixed(1)}d`;
}

// A freshness check. A null timestamp passes: it means the signal has never
// been observed, which is the expected state immediately after a new signal
// is introduced and is not evidence that anything is broken.
function freshness(name: string, iso: string | null, maxAge: number, now: number): Check {
  const age = ageMs(iso, now);
  if (age === null) {
    return { name, severity: 'ok', detail: 'never recorded -- no run observed yet' };
  }
  if (age > maxAge) {
    return {
      name,
      severity: 'fail',
      detail: `last run ${fmtAge(age)} ago, limit ${fmtAge(maxAge)}`,
    };
  }
  return { name, severity: 'ok', detail: `last run ${fmtAge(age)} ago` };
}

// A queue check, judged on how long the oldest row has waited rather than
// on how many are waiting. See LIMITS.drainMaxAge for why.
//
// pipeline_health() only counts a row as awaiting if no run of the relevant
// job has completed since it was scored -- so rows the job has already seen
// and declined, because nothing in the catalogue is near them, never reach
// this function.
function drain(name: string, count: number, oldest: string | null, now: number): Check {
  if (count === 0) return { name, severity: 'ok', detail: 'empty' };

  const age = ageMs(oldest, now);
  if (age === null) {
    // Waiting rows with no scored timestamp. Nothing to judge age by, so
    // this passes rather than guessing -- but it is worth seeing.
    return { name, severity: 'ok', detail: `${count} waiting, age unknown` };
  }
  if (age > LIMITS.drainMaxAge) {
    return {
      name,
      severity: 'fail',
      detail:
        `${count} waiting, oldest ${fmtAge(age)} -- limit ${fmtAge(LIMITS.drainMaxAge)}. ` +
        `Not a backlog, a stall: rows this old will not clear on their own.`,
    };
  }
  return { name, severity: 'ok', detail: `${count} waiting, oldest ${fmtAge(age)}, draining` };
}

function evaluate(h: Health): Check[] {
  const now = new Date(h.checked_at).getTime();

  const checks: Check[] = [
    freshness('movie_ingest', h.last_movie_ingest, LIMITS.movieIngestMaxAge, now),
    freshness('tv_ingest', h.last_tv_ingest, LIMITS.tvIngestMaxAge, now),
    freshness('upcoming_ingest', h.last_upcoming_ingestion, LIMITS.upcomingIngestMaxAge, now),
    freshness('popularity_refresh', h.last_popularity_refresh, LIMITS.popularityMaxAge, now),
    // Watched for the same reason as the movie stamp, and it matters
    // more here than the number alone suggests: this job also refreshes
    // last_air_date, which the TV "Popular this week" row filters on. If
    // it stops, that row does not just go stale -- it empties, as every
    // frozen air date ages past the window.
    freshness('tv_popularity_refresh', h.last_tv_popularity_refresh, LIMITS.tvPopularityMaxAge, now),
    freshness('availability_refresh', h.last_availability_refresh, LIMITS.availabilityMaxAge, now),
    freshness('movie_rails', h.last_movie_rail, LIMITS.movieRailMaxAge, now),

    drain('movies_awaiting_rails', h.movies_awaiting_rails, h.movies_awaiting_rails_oldest, now),
    drain('tv_awaiting_clusters', h.tv_awaiting_clusters, h.tv_awaiting_clusters_oldest, now),
    drain('tv_awaiting_rails', h.tv_awaiting_rails, h.tv_awaiting_rails_oldest, now),
  ];

  // Stranded in 'submitted' with no open batch left to collect them. These
  // rows cannot progress on their own: the batch they belonged to was
  // already collected and will never be read again, so nothing in the
  // pipeline will ever look at them. They need resetting to 'pending' so a
  // later submit can pick them up, and the collector needs to stop leaving
  // them behind.
  checks.push({
    name: 'movies_orphaned_submitted',
    severity: h.movies_orphaned_submitted > 0 ? 'fail' : 'ok',
    detail:
      h.movies_orphaned_submitted > 0
        ? `${h.movies_orphaned_submitted} stranded in 'submitted' with no open batch -- they will never progress`
        : 'none',
  });
  checks.push({
    name: 'tv_orphaned_submitted',
    severity: h.tv_orphaned_submitted > 0 ? 'fail' : 'ok',
    detail:
      h.tv_orphaned_submitted > 0
        ? `${h.tv_orphaned_submitted} stranded in 'submitted' with no open batch -- they will never progress`
        : 'none',
  });

  // Context, not verdicts. These numbers are worth reading when something
  // else has failed and worth nothing on their own, so they never fail the
  // run: awaiting_scoring is the normal state between a submit and its
  // collect, and a cluster without a description renders a thinner page
  // rather than a broken one.
  checks.push({
    name: 'awaiting_scoring',
    severity: 'ok',
    detail: `${h.movies_pending_eligible} movies, ${h.tv_pending_eligible} shows pending eligible`,
  });
  checks.push({
    name: 'tv_cluster_descriptions',
    severity: 'ok',
    detail:
      h.tv_clusters_without_description > 0
        ? `${h.tv_clusters_without_description} without description`
        : 'all written',
  });
  checks.push({
    name: 'movie_cluster_descriptions',
    severity: 'ok',
    detail:
      h.movie_clusters_without_description > 0
        ? `${h.movie_clusters_without_description} without description`
        : 'all written',
  });

  return checks;
}

async function main() {
  const startedAt = new Date().toISOString();

  const { data, error } = await supabase.rpc('pipeline_health');
  if (error) throw new Error(`pipeline_health() failed: ${error.message}`);

  const health = data as Health;
  const checks = evaluate(health);

  const failures = checks.filter((c) => c.severity === 'fail');

  const pad = Math.max(...checks.map((c) => c.name.length));
  console.log(`Pipeline health at ${health.checked_at}\n`);
  for (const c of checks) {
    console.log(`${c.severity === 'ok' ? 'OK  ' : 'FAIL'}  ${c.name.padEnd(pad)}  ${c.detail}`);
  }

  console.log(`\n${failures.length} failing, ${checks.length} checked`);

  // Recorded so health history is queryable alongside the pipeline's own
  // runs. Best-effort: a logging failure must not change the verdict.
  // 'health_check' was rejected by pipeline_runs_run_type_check until the
  // migration that widened it, so every insert from here was silently
  // dropped and no health history exists before that point.
  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'health_check',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: checks.length,
    rows_failed: failures.length,
    status: failures.length > 0 ? 'failed' : 'success',
    error_message:
      failures.length > 0
        ? failures
            .map((f) => `${f.name}: ${f.detail}`)
            .join('; ')
            .slice(0, 2000)
        : null,
  });
  if (logError) console.error(`pipeline_runs logging failed (non-fatal): ${logError.message}`);

  if (failures.length > 0) {
    console.error(`\nFAILING: ${failures.map((f) => f.name).join(', ')}`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
