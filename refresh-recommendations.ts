// Refreshes all 5 recommendation rails (movie_neighbors) across the
// full catalog. Adaptive per-chunk sizing (INITIAL_CHUNK_SIZE below)
// handles individual RPC timeouts -- but even at a working chunk size,
// a full catalog x 5 rails run takes far longer than a single GitHub
// Actions job is allowed to run (hard platform ceiling of 6h; this
// job's own timeout-minutes is set to 350 to stay under that). A run
// that hits either limit gets killed mid-work with no warning.
//
// So progress checkpoints to bulk_compute_progress (already existed in
// the schema, written by an older cursor-based version of this script)
// after every chunk. If the process is killed, the next invocation
// picks up from the last checkpoint instead of starting the whole
// catalog over from movie #1. The script also self-stops with a clean
// checkpoint at TIME_BUDGET_MS, well under the job's own timeout, so a
// graceful stop-and-resume is the common case, not a hard kill.
//
// EVENT_NAME (passed from the workflow's github.event_name) gates
// whether an idle run (is_running=false) is allowed to start a fresh
// full recompute. The 6h cron schedule exists to RESUME an
// already-in-progress run, not to launch new ones -- without this
// check, a completed run sits at is_running=false and the very next
// scheduled tick would restart the entire ~20h job from scratch,
// forever, unconditionally. Manual workflow_dispatch can always start
// a fresh run; the schedule cannot.
//
// Trigger this repeatedly (workflow_dispatch, or the schedule) until
// bulk_compute_progress.current_type reaches 'DONE' -- each run just
// continues where the last one left off. Once DONE, only an explicit
// workflow_dispatch starts a brand new full recompute.
//
// NET_SIZE and INITIAL_CHUNK_SIZE both re-tuned together after
// profiling with the pipeline paused (no contention): the real
// per-movie cost isn't candidate *gathering* (by_keyword alone profiled
// at 150ms even for a popular movie, genre/keyword filters are
// GIN-indexed) -- it's *scoring* every candidate the pool hands back
// (genre weighting, top5_sim, cluster checks, none of which are
// index-accelerated). net_size 300->100 cuts how many candidates reach
// that expensive step and measured clean at 8.87s->5.21s for the same
// worst-case 13-movie batch, with a spot check (The Godfather) showing
// no quality loss. At the new ~0.40s/movie worst-case rate,
// INITIAL_CHUNK_SIZE moves 13->150 (~60s/chunk, still real margin under
// the ~120s effective timeout) to cut round-trip overhead instead of
// rediscovering a small chunk size that no longer matches the cost.
//
// Order matters and is fixed: closest_match, same_mood, darker_pick,
// more_accessible, hidden_gem -- each rail excludes picks already used
// by the ones before it (see the functions' v_excl logic), so running
// them out of order, or resuming into the wrong rail, would break that.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional env var: EVENT_NAME (github.event_name from the workflow)

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');
const EVENT_NAME = process.env.EVENT_NAME ?? null;

const TOP_K = 10;
const NET_SIZE = 100;
const INITIAL_CHUNK_SIZE = 150;
const MIN_CHUNK_SIZE = 10;
// Job's own timeout-minutes is 350; stopping well before that so a
// checkpoint write always completes rather than racing the kill signal.
const TIME_BUDGET_MS = 320 * 60 * 1000;
const PROGRESS_ROW_ID = 1;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  db: { schema: 'public' },
});

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

interface RailConfig {
  name: string;
  rpcName: string;
  extraArgs: Record<string, number>;
}

const RAILS: RailConfig[] = [
  { name: 'closest_match', rpcName: 'compute_closest_match', extraArgs: { visibility_floor: 250 } },
  { name: 'same_mood', rpcName: 'compute_same_mood', extraArgs: { visibility_floor: 250, top_n_dims: 5 } },
  { name: 'darker_pick', rpcName: 'compute_darker_pick', extraArgs: { visibility_floor: 250, gap_threshold: 15 } },
  { name: 'more_accessible', rpcName: 'compute_more_accessible', extraArgs: { visibility_floor: 250, gap_threshold: 15 } },
  { name: 'hidden_gem', rpcName: 'compute_hidden_gem', extraArgs: { vote_floor: 250, vote_ceiling: 5000 } },
];

interface Progress {
  currentType: string;
  isRunning: boolean;
  cursorVoteCount: number | null;
  cursorId: string | null;
}

async function loadProgress(): Promise<Progress> {
  const { data, error } = await supabase
    .from('bulk_compute_progress')
    .select('current_type, is_running, cursor_vote_count, cursor_id')
    .eq('id', PROGRESS_ROW_ID)
    .maybeSingle();
  if (error) throw error;
  if (!data) {
    return { currentType: RAILS[0].name, isRunning: false, cursorVoteCount: null, cursorId: null };
  }
  return {
    currentType: data.current_type,
    isRunning: data.is_running,
    cursorVoteCount: data.cursor_vote_count,
    cursorId: data.cursor_id,
  };
}

async function saveProgress(
  currentType: string,
  isRunning: boolean,
  processedSoFar: number,
  totalMovies: number,
  cursorVoteCount: number | null,
  cursorId: string | null,
  lastError: string | null = null,
) {
  const { error } = await supabase
    .from('bulk_compute_progress')
    .update({
      current_type: currentType,
      current_offset: processedSoFar,
      total_movies: totalMovies,
      is_running: isRunning,
      last_run_at: new Date().toISOString(),
      last_batch_processed: processedSoFar,
      last_error: lastError,
      cursor_vote_count: cursorVoteCount,
      cursor_id: cursorId,
    })
    .eq('id', PROGRESS_ROW_ID);
  if (error) console.error('  WARNING: failed to save checkpoint:', error.message);
}

async function fetchOrderedMovies(): Promise<{ id: string; vote_count: number }[]> {
  const all: { id: string; vote_count: number }[] = [];
  const PAGE_SIZE = 1000;
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from('movies')
      .select('id, vote_count')
      .not('essence_vector', 'is', null)
      .order('vote_count', { ascending: false })
      .order('id', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    all.push(...(page as any[]));
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return all;
}

function isTimeoutError(message: string): boolean {
  return /statement timeout/i.test(message);
}

async function processChunk(rail: RailConfig, ids: string[]): Promise<number> {
  const { data, error } = await supabase.rpc(rail.rpcName, {
    top_k: TOP_K,
    net_size: NET_SIZE,
    movie_limit: null,
    movie_offset: 0,
    after_vote_count: null,
    after_id: null,
    p_specific_ids: ids,
    ...rail.extraArgs,
  });

  if (!error) return data as number;

  if (isTimeoutError(error.message) && ids.length > MIN_CHUNK_SIZE) {
    const mid = Math.ceil(ids.length / 2);
    console.log(`  timeout on chunk of ${ids.length} -- splitting into ${mid} + ${ids.length - mid}`);
    const first = await processChunk(rail, ids.slice(0, mid));
    const second = await processChunk(rail, ids.slice(mid));
    return first + second;
  }

  if (isTimeoutError(error.message)) {
    console.log(`  SKIP: chunk of ${ids.length} still timing out at the floor -- ids: ${ids.slice(0, 3).join(', ')}...`);
    return 0;
  }

  throw new Error(`${rail.rpcName} failed on a non-timeout error: ${error.message}`);
}

async function main() {
  const startTime = Date.now();

  const progress = await loadProgress();

  if (!progress.isRunning && EVENT_NAME === 'schedule') {
    console.log(
      'Nothing in progress (is_running=false) and this run was triggered by the schedule, not workflow_dispatch. ' +
        'The cron only resumes an in-progress run; it does not start a new full recompute on its own. ' +
        'Exiting without doing anything. Trigger workflow_dispatch manually to start a fresh run.',
    );
    return;
  }

  console.log('Fetching ordered movie list...');
  const orderedMovies = await fetchOrderedMovies();
  console.log(`${orderedMovies.length} scored movies to process per rail.`);

  let railStartIndex: number;
  let resumeFromIndex: number;

  if (progress.isRunning) {
    railStartIndex = RAILS.findIndex((r) => r.name === progress.currentType);
    if (railStartIndex === -1) railStartIndex = 0;
    if (progress.cursorId) {
      const idx = orderedMovies.findIndex(
        (m) => m.vote_count === progress.cursorVoteCount && m.id === progress.cursorId,
      );
      resumeFromIndex = idx >= 0 ? idx + 1 : 0;
    } else {
      resumeFromIndex = 0;
    }
    console.log(`Resuming an interrupted run: rail "${progress.currentType}", movie index ${resumeFromIndex}.`);
  } else {
    railStartIndex = 0;
    resumeFromIndex = 0;
    console.log('Starting a fresh full run (manual trigger).');
  }

  for (let railIdx = railStartIndex; railIdx < RAILS.length; railIdx++) {
    const rail = RAILS[railIdx];
    const startIdx = railIdx === railStartIndex ? resumeFromIndex : 0;

    console.log(
      `\n=== ${rail.name} (resuming at movie ${startIdx}/${orderedMovies.length}, chunks of ${INITIAL_CHUNK_SIZE}) ===`,
    );
    let totalProcessed = startIdx;

    for (let i = startIdx; i < orderedMovies.length; i += INITIAL_CHUNK_SIZE) {
      if (Date.now() - startTime > TIME_BUDGET_MS) {
        const lastDone = orderedMovies[i - 1];
        await saveProgress(rail.name, true, i, orderedMovies.length, lastDone?.vote_count ?? null, lastDone?.id ?? null);
        console.log(
          `\nTime budget reached at ${rail.name} movie ${i}/${orderedMovies.length}. Checkpoint saved -- next run resumes here.`,
        );
        return;
      }

      const chunkMovies = orderedMovies.slice(i, i + INITIAL_CHUNK_SIZE);
      const processed = await processChunk(rail, chunkMovies.map((m) => m.id));
      totalProcessed = i + chunkMovies.length;

      const lastInChunk = chunkMovies[chunkMovies.length - 1];
      await saveProgress(rail.name, true, totalProcessed, orderedMovies.length, lastInChunk.vote_count, lastInChunk.id);

      console.log(`  chunk starting at ${i}: +${processed} (total ${totalProcessed}/${orderedMovies.length})`);
    }

    console.log(`${rail.name} done: ${totalProcessed} processed.`);
  }

  await saveProgress('DONE', false, 0, orderedMovies.length, null, null);
  console.log('\nAll rails fully refreshed.');
}

main().catch(async (err) => {
  console.error('Fatal error:', err);
  try {
    await supabase
      .from('bulk_compute_progress')
      .update({ last_error: String(err?.message ?? err), is_running: false })
      .eq('id', PROGRESS_ROW_ID);
  } catch {
    // Best effort -- don't mask the original error if this also fails.
  }
  process.exit(1);
});
