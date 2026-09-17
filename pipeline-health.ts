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
// On any FAIL this process exits non-zero, which fails the workflow and
// triggers GitHub's own notification to the repository owner. WARN rows
// are printed and recorded but do not fail the run.
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
  movieRailMaxAge: 48 * HOUR, // daily, 13:00 UTC
  moviesAwaitingRails: 200,
  tvAwaitingClusters: 50,
  tvAwaitingRails: 50,
};

type Severity = 'ok' | 'warn' | 'fail';

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
  last_movie_rail: string | null;
  last_tv_rail: string | null;
  movies_awaiting_rails: number;
  tv_awaiting_clusters: number;
  tv_awaiting_rails: number;
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
  return hours < 48 ? `${hours.toFixed(1)}h ago` : `${(ms / DAY).toFixed(1)}d ago`;
}

// A freshness check. A null timestamp is reported as 'warn', never 'fail':
// it means the signal has never been observed, which is the expected state
// immediately after a new signal is introduced and is not evidence that
// anything is broken.
function freshness(name: string, iso: string | null, maxAge: number, now: number): Check {
  const age = ageMs(iso, now);
  if (age === null) {
    return { name, severity: 'warn', detail: 'never recorded -- no run observed yet' };
  }
  if (age > maxAge) {
    return { name, severity: 'fail', detail: `last run ${fmtAge(age)}, limit ${fmtAge(maxAge)}` };
  }
  return { name, severity: 'ok', detail: `last run ${fmtAge(age)}` };
}

function backlog(name: string, count: number, limit: number): Check {
  if (count > limit) {
    return { name, severity: 'fail', detail: `${count} waiting, limit ${limit}` };
  }
  return { name, severity: 'ok', detail: `${count} waiting` };
}

function warnIfAny(name: string, count: number, note: string): Check {
  if (count > 0) return { name, severity: 'warn', detail: `${count} ${note}` };
  return { name, severity: 'ok', detail: 'none' };
}

function evaluate(h: Health): Check[] {
  const now = new Date(h.checked_at).getTime();

  const checks: Check[] = [
    freshness('movie_ingest', h.last_movie_ingest, LIMITS.movieIngestMaxAge, now),
    freshness('tv_ingest', h.last_tv_ingest, LIMITS.tvIngestMaxAge, now),
    freshness('popularity_refresh', h.last_popularity_refresh, LIMITS.popularityMaxAge, now),
    freshness('movie_rails', h.last_movie_rail, LIMITS.movieRailMaxAge, now),

    backlog('movies_awaiting_rails', h.movies_awaiting_rails, LIMITS.moviesAwaitingRails),
    backlog('tv_awaiting_clusters', h.tv_awaiting_clusters, LIMITS.tvAwaitingClusters),
    backlog('tv_awaiting_rails', h.tv_awaiting_rails, LIMITS.tvAwaitingRails),
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

  // Informational: these are normal between a submit and its collect.
  checks.push({
    name: 'awaiting_scoring',
    severity: 'ok',
    detail: `${h.movies_pending_eligible} movies, ${h.tv_pending_eligible} shows pending eligible`,
  });

  checks.push(warnIfAny('tv_cluster_descriptions', h.tv_clusters_without_description, 'clusters have no description'));
  checks.push(
    warnIfAny('movie_cluster_descriptions', h.movie_clusters_without_description, 'clusters have no description'),
  );

  return checks;
}

async function main() {
  const startedAt = new Date().toISOString();

  const { data, error } = await supabase.rpc('pipeline_health');
  if (error) throw new Error(`pipeline_health() failed: ${error.message}`);

  const health = data as Health;
  const checks = evaluate(health);

  const failures = checks.filter((c) => c.severity === 'fail');
  const warnings = checks.filter((c) => c.severity === 'warn');

  const pad = Math.max(...checks.map((c) => c.name.length));
  console.log(`Pipeline health at ${health.checked_at}\n`);
  for (const c of checks) {
    const tag = c.severity === 'ok' ? 'OK  ' : c.severity === 'warn' ? 'WARN' : 'FAIL';
    console.log(`${tag}  ${c.name.padEnd(pad)}  ${c.detail}`);
  }

  const summary = `${failures.length} failing, ${warnings.length} warning, ${checks.length} checked`;
  console.log(`\n${summary}`);

  // Recorded so health history is queryable alongside the pipeline's own
  // runs. Best-effort: a logging failure must not change the verdict.
  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'health_check',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: checks.length,
    rows_failed: failures.length,
    status: failures.length > 0 ? 'failed' : 'success',
    error_message: failures.length > 0 ? failures.map((f) => `${f.name}: ${f.detail}`).join('; ').slice(0, 2000) : null,
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
