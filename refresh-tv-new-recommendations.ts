// TV counterpart to refresh-new-recommendations.ts: computes the
// recommendation rails for scored shows that are missing any of them --
// newly scored arrivals from the weekly ingest -> submit -> collect chain,
// after assign-tv-clusters.ts has placed them, and shows that only got
// part of their rails.
//
// This closes the last gap in the TV chain. refresh-tv-recommendations.ts
// exists, but its scheduled trigger only RESUMES an in-progress full run
// and exits immediately when nothing is in progress -- by design, so a
// completed catalogue run can't be restarted by the next tick. That means
// nothing on a schedule ever picked up a newly scored show. The movie side
// has had this covered since refresh-new-recommendations.ts; TV did not.
//
// Order is fixed and matters: closest_match, same_mood, darker_pick,
// more_accessible, hidden_gem -- each excludes picks already used by the
// ones before it. So a show is recomputed from its FIRST missing rail
// onward: the rails before that one are still valid, the ones after it
// were picked without knowing what it will hold.
//
// Finding the shows is one database call, tv_shows_missing_rails(), as on
// the movie side. It used to read every row of tv_neighbors into this
// process -- some 480,000 rows, about 485 requests, each further into the
// table than the last -- to find a few dozen shows, and on 2026-10-19 one
// of those requests met a 502 from the gateway and ended the run. It also
// only asked whether a show had any rows at all, so a show with some of
// its rails was done for good: 27 shows were missing same_mood onward
// when this changed, beside the 40 with none.
//
// Cluster assignment must already exist for these shows before this runs:
// cluster_score inside the rail functions reads tv_cluster_assignments, so
// a show with no assignment scores as if it belonged to nothing. The
// workflow runs assign-tv-clusters.ts first in the same job for exactly
// this reason.
//
// Deliberately passes NO tuning parameters -- not top_k, net_size,
// visibility_floor, vote_floor/ceiling or gap_threshold. Every one of those
// already has a TV-specific default baked into the compute_tv_* functions
// (visibility_floor 180 rather than the movie side's 250, vote_ceiling
// 2000 rather than 5000), and those defaults were tuned against the TV
// catalogue. Restating them here would fork the tuning: a later change to a
// function's default would silently not apply to newly ingested shows, and
// new shows would be railed on different parameters from the rest of the
// catalogue. Only the paging arguments and p_specific_ids are passed.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';
import { GATEWAY_STATUSES, resilientFetch } from './resilient-fetch';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

// Same adaptive-chunking values as the movie side. A single chunk timeout
// must not abort the run: halve and retry, skip and log at the floor, keep
// going. That pattern is why the movie backlog stopped compounding.
const INITIAL_CHUNK_SIZE = 20;
const MIN_CHUNK_SIZE = 10;
const PAGE_SIZE = 1000;

// tv_shows_missing_rails() measured 0.5s. The pause-and-retry on a
// statement timeout is the movie side's, kept so the two jobs fail the
// same way under load.
const DISCOVERY_MAX_ATTEMPTS = 4;
const DISCOVERY_BACKOFF_MS = 15000;

// A rail call the gateway drops may still be running in the database, and
// a second call for the same shows while the first is still writing would
// trip over its rows (the compute_tv_* functions clear a show's rail and
// write it again, with no ON CONFLICT). So a dropped rail call is sent
// again only after the 60s statement timeout has run out on the first,
// and at most twice.
const GATEWAY_RETRIES = 2;
const GATEWAY_PAUSE_MS = 65000;

// The discovery call only reads, so resilientFetch may send it again
// straight away; the rail calls are left to processChunk, below.
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  global: {
    fetch: resilientFetch({
      label: 'database',
      retryOn: GATEWAY_STATUSES,
      repeatRpc: (name) => name === 'tv_shows_missing_rails',
    }),
  },
});

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Position in this array + 1 is the first_missing value
// tv_shows_missing_rails() returns -- keep the two orders identical.
const RAILS: { name: string; rpcName: string }[] = [
  { name: 'closest_match', rpcName: 'compute_tv_closest_match' },
  { name: 'same_mood', rpcName: 'compute_tv_same_mood' },
  { name: 'darker_pick', rpcName: 'compute_tv_darker_pick' },
  { name: 'more_accessible', rpcName: 'compute_tv_more_accessible' },
  { name: 'hidden_gem', rpcName: 'compute_tv_hidden_gem' },
];

interface MissingShow {
  id: string;
  firstMissing: number; // 1-based position in RAILS
}

function isTimeoutError(message: string): boolean {
  return /statement timeout/i.test(message);
}

// No answer from behind the gateway: a gateway status, or no answer at
// all (status 0 is how the client reports a request that never got one).
function isGatewayFailure(status: number): boolean {
  return status === 0 || GATEWAY_STATUSES.has(status);
}

// Every scored show (essence_vector_ext_z present -- a show without it
// would crash the rails on a NULL similarity score) that is missing at
// least one rail. Paged because PostgREST caps a response at 1,000 rows;
// the function orders by id so the pages are stable.
async function fetchShowsMissingRails(): Promise<MissingShow[]> {
  const out: MissingShow[] = [];
  let from = 0;
  while (true) {
    let page: any[] | null = null;
    for (let attempt = 1; attempt <= DISCOVERY_MAX_ATTEMPTS; attempt++) {
      const { data, error } = await supabase.rpc('tv_shows_missing_rails').range(from, from + PAGE_SIZE - 1);
      if (!error) {
        page = data ?? [];
        break;
      }
      if (!isTimeoutError(error.message) || attempt === DISCOVERY_MAX_ATTEMPTS) throw error;
      const waitMs = DISCOVERY_BACKOFF_MS * attempt;
      console.log(`  discovery query timed out (attempt ${attempt}/${DISCOVERY_MAX_ATTEMPTS}) -- retrying in ${waitMs / 1000}s`);
      await sleep(waitMs);
    }
    const rows = page ?? [];
    for (const row of rows) {
      const firstMissing = Number(row.first_missing);
      if (firstMissing >= 1 && firstMissing <= RAILS.length) out.push({ id: row.id, firstMissing });
    }
    if (rows.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return out;
}

// Recursively processes a chunk of show ids for one rail. On a timeout,
// halves the chunk and retries both halves instead of failing the whole
// run. At MIN_CHUNK_SIZE, logs and skips rather than retrying forever --
// a skipped show keeps its gap and is picked up again by the next run.
async function processChunk(rpcName: string, ids: string[], gatewayRetries = 0): Promise<number> {
  const { data, error, status } = await supabase.rpc(rpcName, {
    show_limit: null,
    show_offset: 0,
    after_vote_count: null,
    after_id: null,
    p_specific_ids: ids,
  });

  if (!error) return data as number;

  if (isGatewayFailure(status) && gatewayRetries < GATEWAY_RETRIES) {
    console.log(
      `  no answer from the database for a chunk of ${ids.length} (status ${status}) -- ` +
        `sending it again in ${GATEWAY_PAUSE_MS / 1000}s`,
    );
    await sleep(GATEWAY_PAUSE_MS);
    return processChunk(rpcName, ids, gatewayRetries + 1);
  }

  if (isTimeoutError(error.message) && ids.length > MIN_CHUNK_SIZE) {
    const mid = Math.ceil(ids.length / 2);
    console.log(`  timeout on chunk of ${ids.length} -- splitting into ${mid} + ${ids.length - mid}`);
    const first = await processChunk(rpcName, ids.slice(0, mid));
    const second = await processChunk(rpcName, ids.slice(mid));
    return first + second;
  }

  if (isTimeoutError(error.message)) {
    console.log(`  SKIP: chunk of ${ids.length} still timing out at the floor -- ids: ${ids.slice(0, 3).join(', ')}...`);
    return 0;
  }

  throw new Error(`${rpcName} failed on a non-timeout error: ${error.message}`);
}

async function main() {
  console.log('Finding scored TV shows missing recommendation rails...');
  const missing = await fetchShowsMissingRails();
  const byStart = RAILS.map((r, i) => `${r.name} ${missing.filter((m) => m.firstMissing === i + 1).length}`);
  console.log(`${missing.length} shows need recommendations (first missing rail: ${byStart.join(', ')}).`);

  if (missing.length === 0) {
    console.log('Nothing to do.');
    return;
  }

  for (let r = 0; r < RAILS.length; r++) {
    const rail = RAILS[r];
    // A show is recomputed from its first missing rail onward.
    const showIds = missing.filter((m) => m.firstMissing <= r + 1).map((m) => m.id);
    if (showIds.length === 0) {
      console.log(`\n=== ${rail.rpcName}: no shows need it ===`);
      continue;
    }
    console.log(`\n=== ${rail.rpcName} (${showIds.length} shows, starting chunks of ${INITIAL_CHUNK_SIZE}) ===`);
    let totalProcessed = 0;
    for (let i = 0; i < showIds.length; i += INITIAL_CHUNK_SIZE) {
      const chunk = showIds.slice(i, i + INITIAL_CHUNK_SIZE);
      const processed = await processChunk(rail.rpcName, chunk);
      totalProcessed += processed;
      console.log(`  chunk starting at ${i}: +${processed} (total ${totalProcessed}/${showIds.length})`);
    }
    console.log(`${rail.rpcName} done: ${totalProcessed} processed.`);
  }

  console.log('\nDone.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
