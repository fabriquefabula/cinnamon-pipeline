// Refreshes all 5 TV recommendation rails (tv_neighbors) across the full
// TV catalogue. Port of refresh-recommendations.ts against the TV silo,
// carrying over every operational lesson that script records.
//
// Checkpointing: a full catalogue x 5 rails run takes far longer than a
// single GitHub Actions job may run (platform ceiling 6h; this job's
// timeout-minutes is 350). Progress is written to
// tv_bulk_compute_progress after every chunk, and the script self-stops
// with a clean checkpoint at TIME_BUDGET_MS so graceful stop-and-resume
// is the normal case rather than a hard kill. Trigger repeatedly until
// current_type reaches 'DONE'.
//
// EVENT_NAME gates whether an idle run may start fresh. The schedule
// exists to RESUME; without this check a completed run sitting at
// is_running=false would have the next tick restart the whole job from
// scratch, forever.
//
// RAIL ORDER IS FIXED and must not be reordered: closest_match,
// same_mood, darker_pick, more_accessible, hidden_gem. Each rail
// excludes titles already taken by the ones before it (the v_excl logic
// in each compute_tv_* function), so running them out of order silently
// produces duplicate recommendations across rails rather than failing.
//
// Thresholds differ from the movie script and are NOT typos. Both were
// measured against the real TV vote distribution, because each encodes a
// PROPORTION of catalogue rather than a raw count:
//   visibility_floor  250 -> 180   (24.4% of films / 24.7% of shows)
//   vote_ceiling     5000 -> 2000  (excludes top 2.11% / top 2.26%)
// At 5000 the TV ceiling would exclude almost nothing and hidden_gem
// would stop meaning "hidden".
//
// Chunk sizes are inherited from the movie run's hard-won numbers: the
// valence rails (darker_pick, more_accessible) do strictly more work per
// candidate and timed out on EVERY chunk at 30 there, so they start at
// 15. TV's catalogue is ~5x smaller, so these may prove conservative --
// but a too-small chunk costs extra round trips while a too-large one
// costs a failed attempt plus two recovered halves on every chunk for
// the whole run. Starting conservative is the cheaper error.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: EVENT_NAME (github.event_name from the workflow)

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');
const EVENT_NAME = process.env.EVENT_NAME ?? null;

const TOP_K = 10;
const NET_SIZE = 100;
const INITIAL_CHUNK_SIZE = 30;
const VALENCE_CHUNK_SIZE = 15;
const MIN_CHUNK_SIZE = 10;
const TIME_BUDGET_MS = 320 * 60 * 1000;
const PROGRESS_ROW_ID = 1;

// Measured on the scored TV catalogue -- see the header note.
const TV_VISIBILITY_FLOOR = 180;
const TV_VOTE_CEILING = 2000;

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
  chunkSize?: number;
}

const RAILS: RailConfig[] = [
  { name: 'closest_match', rpcName: 'compute_tv_closest_match', extraArgs: { visibility_floor: TV_VISIBILITY_FLOOR } },
  { name: 'same_mood', rpcName: 'compute_tv_same_mood', extraArgs: { visibility_floor: TV_VISIBILITY_FLOOR, top_n_dims: 5 } },
  { name: 'darker_pick', rpcName: 'compute_tv_darker_pick', extraArgs: { visibility_floor: TV_VISIBILITY_FLOOR, gap_threshold: 15 }, chunkSize: VALENCE_CHUNK_SIZE },
  { name: 'more_accessible', rpcName: 'compute_tv_more_accessible', extraArgs: { visibility_floor: TV_VISIBILITY_FLOOR, gap_threshold: 15 }, chunkSize: VALENCE_CHUNK_SIZE },
  { name: 'hidden_gem', rpcName: 'compute_tv_hidden_gem', extraArgs: { vote_floor: TV_VISIBILITY_FLOOR, vote_ceiling: TV_VOTE_CEILING } },
];

interface Progress {
  currentType: string;
  isRunning: boolean;
  cursorVoteCount: number | null;
  cursorId: string | null;
}

async function loadProgress(): Promise<Progress> {
  const { data, error } = await supabase
    .from('tv_bulk_compute_progress')
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
  totalShows: number,
  cursorVoteCount: number | null,
  cursorId: string | null,
  lastError: string | null = null,
) {
  const { error } = await supabase
    .from('tv_bulk_compute_progress')
    .update({
      current_type: currentType,
      current_offset: processedSoFar,
      total_shows: totalShows,
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

// Filters on essence_vector_ext_z, not essence_vector. On the movie side
// filtering the raw column caused two pipeline crashes at an identical
// offset with "null value in similarity_score violates not-null
// constraint": rows had the raw vector but never had the derived ones
// computed, so they entered a batch before they were genuinely ready.
// The derived column is the one every rail function actually requires.
async function fetchOrderedShows(): Promise<{ id: string; vote_count: number }[]> {
  const all: { id: string; vote_count: number }[] = [];
  const PAGE_SIZE = 1000;
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from('tv_shows')
      .select('id, vote_count')
      .not('essence_vector_ext_z', 'is', null)
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
    show_limit: null,
    show_offset: 0,
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

  console.log('Fetching ordered show list...');
  const orderedShows = await fetchOrderedShows();
  console.log(`${orderedShows.length} scored shows to process per rail.`);

  let railStartIndex: number;
  let resumeFromIndex: number;

  if (progress.isRunning) {
    railStartIndex = RAILS.findIndex((r) => r.name === progress.currentType);
    if (railStartIndex === -1) railStartIndex = 0;
    if (progress.cursorId) {
      const idx = orderedShows.findIndex(
        (m) => m.vote_count === progress.cursorVoteCount && m.id === progress.cursorId,
      );
      resumeFromIndex = idx >= 0 ? idx + 1 : 0;
    } else {
      resumeFromIndex = 0;
    }
    console.log(`Resuming an interrupted run: rail "${progress.currentType}", show index ${resumeFromIndex}.`);
  } else {
    railStartIndex = 0;
    resumeFromIndex = 0;
    console.log('Starting a fresh full run (manual trigger).');
  }

  for (let railIdx = railStartIndex; railIdx < RAILS.length; railIdx++) {
    const rail = RAILS[railIdx];
    const chunkSize = rail.chunkSize ?? INITIAL_CHUNK_SIZE;
    const startIdx = railIdx === railStartIndex ? resumeFromIndex : 0;

    console.log(
      `\n=== ${rail.name} (resuming at show ${startIdx}/${orderedShows.length}, chunks of ${chunkSize}) ===`,
    );
    let totalProcessed = startIdx;

    for (let i = startIdx; i < orderedShows.length; i += chunkSize) {
      if (Date.now() - startTime > TIME_BUDGET_MS) {
        const lastDone = orderedShows[i - 1];
        await saveProgress(rail.name, true, i, orderedShows.length, lastDone?.vote_count ?? null, lastDone?.id ?? null);
        console.log(
          `\nTime budget reached at ${rail.name} show ${i}/${orderedShows.length}. Checkpoint saved -- next run resumes here.`,
        );
        return;
      }

      const chunkShows = orderedShows.slice(i, i + chunkSize);
      const processed = await processChunk(rail, chunkShows.map((m) => m.id));
      totalProcessed = i + chunkShows.length;

      const lastInChunk = chunkShows[chunkShows.length - 1];
      await saveProgress(rail.name, true, totalProcessed, orderedShows.length, lastInChunk.vote_count, lastInChunk.id);

      console.log(`  chunk starting at ${i}: +${processed} (total ${totalProcessed}/${orderedShows.length})`);
    }

    console.log(`${rail.name} done: ${totalProcessed} processed.`);
  }

  await saveProgress('DONE', false, 0, orderedShows.length, null, null);
  console.log('\nAll TV rails fully refreshed.');
}

main().catch(async (err) => {
  console.error('Fatal error:', err);
  try {
    await supabase
      .from('tv_bulk_compute_progress')
      .update({ last_error: String(err?.message ?? err), is_running: false })
      .eq('id', PROGRESS_ROW_ID);
  } catch {
    // Best effort -- don't mask the original error if this also fails.
  }
  process.exit(1);
});
