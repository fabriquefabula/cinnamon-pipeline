// Incremental companion to cluster-keywords.ts: embeds any keyword not
// yet in keyword_group_members and assigns it to its nearest EXISTING
// group by distance -- no LLM labeling, no re-clustering, no group
// taxonomy changes. Same relationship cluster-movies.ts has to
// assign-movie-clusters.ts.
//
// No min-count threshold here (unlike cluster-keywords.ts's
// MIN_KEYWORD_COUNT=15) -- that threshold exists to decide whether a
// keyword deserves to help SHAPE a new group during full clustering.
// This script never creates groups, only routes a keyword to the best
// EXISTING one, so a brand-new keyword that has appeared on only one
// movie so far can still get a home immediately rather than waiting to
// accumulate 15 occurrences first.
//
// ANTHROPIC_API_KEY is NOT needed here (no labeling).
// Required env vars: VOYAGE_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { createClient } from '@supabase/supabase-js';

const VOYAGE_API_KEY = requireEnv('VOYAGE_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const EMBED_MODEL = 'voyage-4';
const EMBED_DIMENSION = 1024;
const EMBED_BATCH_SIZE = 128;
const PAGE_SIZE = 1000;

// Same blocklist as cluster-keywords.ts -- a new keyword still has to
// pass the same format/meta and sensitive-content filters before it's
// eligible to be embedded and assigned at all.
const FORMAT_BLOCKLIST = [
  'based on', 'sequel', 'prequel', 'remake', 'reboot', 'spin off', 'spin-off',
  'duringcreditsstinger', 'aftercreditsstinger', '3d animation', 'stop motion',
  'live action remake', 'silent film', 'black and white', 'archive footage',
  'making of', 'behind the scenes', 'interview', 'film in film', 'cinema on cinema',
  'movie business', 'compilation', 'edited from tv series', 'narration',
  'pov (point of view)', 'anthology', 'low budget', 'b movie', 'video nasty',
  'exploitation', 'pseudo-documentary', 'mockumentary style',
];

const SENSITIVE_CONTENT_BLOCKLIST = [
  'holocaust', 'concentration camp', 'genocide', 'slavery',
  'human trafficking', 'sex trafficking', 'organ trafficking', 'child trafficking',
  'war crimes', 'mass murder', 'hate crime', 'terrorism',
  'child abuse', 'child murder', 'pedophil', 'underage sex',
  'sexual abuse', 'sexual assault', 'sexual violence', 'rape',
  'domestic violence', 'suicide', 'incest',
];

const BLOCKLIST_PATTERNS = [...FORMAT_BLOCKLIST, ...SENSITIVE_CONTENT_BLOCKLIST];

function isQualityKeyword(keyword: string): boolean {
  const k = keyword.toLowerCase();
  return !BLOCKLIST_PATTERNS.some((p) =>
    p.includes(' ') ? k.includes(p) : new RegExp(`\\b${p}`).test(k),
  );
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

function normalize(v: number[]): number[] {
  const mag = Math.sqrt(v.reduce((s, x) => s + x * x, 0));
  return mag === 0 ? v.slice() : v.map((x) => x / mag);
}

function dist(a: number[], b: number[]): number {
  let s = 0;
  for (let i = 0; i < a.length; i++) {
    const d = a[i] - b[i];
    s += d * d;
  }
  return Math.sqrt(s);
}

async function embedBatchWithRetry(batch: string[], attempt = 1): Promise<{ embedding: number[]; index: number }[]> {
  const MAX_ATTEMPTS = 8;
  const res = await fetch('https://api.voyageai.com/v1/embeddings', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${VOYAGE_API_KEY}` },
    body: JSON.stringify({ input: batch, model: EMBED_MODEL, input_type: 'document', output_dimension: EMBED_DIMENSION }),
  });
  if (res.status === 429) {
    if (attempt >= MAX_ATTEMPTS) throw new Error(`Voyage still rate-limiting after ${MAX_ATTEMPTS} attempts.`);
    const waitMs = 21_000 * attempt;
    console.log(`Rate limited, waiting ${Math.round(waitMs / 1000)}s...`);
    await new Promise((r) => setTimeout(r, waitMs));
    return embedBatchWithRetry(batch, attempt + 1);
  }
  if (!res.ok) throw new Error(`Voyage embeddings request failed: ${res.status} ${await res.text()}`);
  const json = (await res.json()) as { data: { embedding: number[]; index: number }[] };
  return json.data.sort((a, b) => a.index - b.index);
}

async function fetchUngroupedKeywords(): Promise<string[]> {
  const counts = new Map<string, number>();
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from('movies')
      .select('keywords')
      .eq('scoring_status', 'scored')
      .not('keywords', 'is', null)
      .order('id', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    for (const row of page as any[]) {
      for (const k of row.keywords ?? []) counts.set(k, (counts.get(k) ?? 0) + 1);
    }
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }

  // Bug fixed here: this coverage check previously had no .range(), so
  // it silently capped at Supabase's default 1000-row limit against a
  // table that already has 3,600+ rows -- found and fixed alongside the
  // identical mistake in refresh-new-recommendations.ts and
  // assign-movie-clusters.ts (same root cause, three files). This script
  // hadn't run in production yet, but would have hit the same failure
  // mode on its first scheduled run.
  const already = new Set<string>();
  let efrom = 0;
  while (true) {
    const { data, error } = await supabase
      .from('keyword_group_members')
      .select('keyword')
      .range(efrom, efrom + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    for (const row of page as any[]) already.add(row.keyword);
    if (page.length < PAGE_SIZE) break;
    efrom += PAGE_SIZE;
  }

  return Array.from(counts.keys()).filter((kw) => !already.has(kw) && isQualityKeyword(kw));
}

async function main() {
  console.log('Finding ungrouped keywords...');
  const ungrouped = await fetchUngroupedKeywords();
  console.log(`${ungrouped.length} keywords need a group.`);
  if (ungrouped.length === 0) {
    console.log('Nothing to do.');
    return;
  }

  const { data: groups, error: groupsErr } = await supabase.from('keyword_groups').select('id, centroid');
  if (groupsErr) throw groupsErr;
  if (!groups || groups.length === 0) {
    console.log('No keyword groups exist yet -- run cluster-keywords.ts first. Nothing to do.');
    return;
  }
  const groupCentroids = groups.map((g: any) => ({ id: g.id, centroid: normalize(JSON.parse(g.centroid)) }));

  console.log('Embedding ungrouped keywords...');
  const embeddings: number[][] = [];
  for (let i = 0; i < ungrouped.length; i += EMBED_BATCH_SIZE) {
    const batch = ungrouped.slice(i, i + EMBED_BATCH_SIZE);
    const sorted = await embedBatchWithRetry(batch);
    for (const d of sorted) embeddings.push(d.embedding);
    console.log(`Embedded ${Math.min(i + EMBED_BATCH_SIZE, ungrouped.length)}/${ungrouped.length}`);
  }

  console.log('Assigning to nearest existing group...');
  const rows: { keyword: string; group_id: string; distance: number }[] = [];
  for (let i = 0; i < ungrouped.length; i++) {
    const v = normalize(embeddings[i]);
    let best = { id: groupCentroids[0].id, d: dist(v, groupCentroids[0].centroid) };
    for (const g of groupCentroids.slice(1)) {
      const d = dist(v, g.centroid);
      if (d < best.d) best = { id: g.id, d };
    }
    rows.push({ keyword: ungrouped[i], group_id: best.id, distance: best.d });
  }

  console.log(`Writing ${rows.length} assignments...`);
  const WRITE_CHUNK = 500;
  for (let i = 0; i < rows.length; i += WRITE_CHUNK) {
    const { error } = await supabase.from('keyword_group_members').insert(rows.slice(i, i + WRITE_CHUNK));
    if (error) throw error;
    console.log(`  ${Math.min(i + WRITE_CHUNK, rows.length)}/${rows.length}`);
  }

  // keyword_count on each affected group is now stale -- refresh it.
  const affectedGroupIds = Array.from(new Set(rows.map((r) => r.group_id)));
  for (const groupId of affectedGroupIds) {
    const { count, error } = await supabase
      .from('keyword_group_members')
      .select('keyword', { count: 'exact', head: true })
      .eq('group_id', groupId);
    if (!error) await supabase.from('keyword_groups').update({ keyword_count: count }).eq('id', groupId);
  }

  console.log('Done.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
