// Keyword-group discovery: embeds every distinct keyword in the catalog
// via Voyage AI, k-means clusters those embeddings so synonyms land
// together ("virtual reality" / "simulated reality" / "artificial
// reality" converge into one group), then one Claude call per group to
// write a real label from what's actually in it. Same shape as
// cluster-movies.ts, applied to the keyword vocabulary instead of movies.
//
// This is what makes a chip like "Simulated Reality" possible: it's a
// group label written from real member keywords, not a titlecased
// keyword string. A flat titlecase-the-keyword approach was tried and
// explicitly rejected -- it's the same shape as the "Cozy Christmas"
// mistake, just relocated to the movie page.
//
// Run via workflow_dispatch, not scheduled -- like cluster-movies.ts,
// this can reshape the whole group taxonomy. Re-run by hand as the
// keyword vocabulary grows.
//
// Required env vars: ANTHROPIC_API_KEY, VOYAGE_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=true, K=<int> (default 200), MIN_KEYWORD_COUNT=<int> (default 15)

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const ANTHROPIC_API_KEY = requireEnv('ANTHROPIC_API_KEY');
const VOYAGE_API_KEY = requireEnv('VOYAGE_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const DRY_RUN = process.env.DRY_RUN === 'true';
const K = process.env.K ? parseInt(process.env.K, 10) : 200;
const MIN_KEYWORD_COUNT = process.env.MIN_KEYWORD_COUNT ? parseInt(process.env.MIN_KEYWORD_COUNT, 10) : 15;
const KMEANS_MAX_ITERATIONS = 100;
const EMBED_MODEL = 'voyage-4';
const EMBED_DIMENSION = 1024;
const EMBED_BATCH_SIZE = 128; // well under Voyage's 1000-input cap; keeps individual requests small
const LABEL_MODEL = 'claude-sonnet-5';
const LABEL_SAMPLE_SIZE = 20;

// Production/format/meta tags -- real TMDB keywords, but not a theme
// worth clustering. Filtered out before embedding, so no group ever
// forms around them.
const FORMAT_BLOCKLIST = [
  'based on', 'sequel', 'prequel', 'remake', 'reboot', 'spin off', 'spin-off',
  'duringcreditsstinger', 'aftercreditsstinger', '3d animation', 'stop motion',
  'live action remake', 'silent film', 'black and white', 'archive footage',
  'making of', 'behind the scenes', 'interview', 'film in film', 'cinema on cinema',
  'movie business', 'compilation', 'edited from tv series', 'narration',
  'pov (point of view)', 'anthology', 'low budget', 'b movie', 'video nasty',
  'exploitation', 'pseudo-documentary', 'mockumentary style',
];

// Real-world trauma/crime topics -- keywords in this category are
// genuinely semantically related to each other (the clustering isn't
// wrong to group them), which is exactly the problem: a real first run
// produced a group labeled "Sexual Violence and Abuse" from rape and
// revenge, human trafficking, sexual assault, domestic violence,
// attempted rape, organ trafficking. Same judgment already applied by
// hand in the earlier fingerprint-collection work (Holocaust,
// trafficking, and similar never became a browsable "vibe" there either)
// -- this just codifies it here too, so no group can form around this
// content regardless of how tight the embedding cluster is. Deliberately
// narrower than a general profanity/content filter: fictional genre
// intensity (violence, gore, torture as a horror descriptor) stays,
// since horror/action audiences reasonably expect that framing; this
// blocks real-world atrocity and abuse topics specifically.
const SENSITIVE_CONTENT_BLOCKLIST = [
  'holocaust', 'concentration camp', 'genocide', 'slavery', 'trafficking',
  'war crimes', 'mass murder', 'hate crime', 'terrorism',
  'child abuse', 'child murder', 'pedophilia',
  'sexual abuse', 'sexual assault', 'sexual violence', 'rape',
  'domestic violence', 'suicide', 'incest',
];

const BLOCKLIST_PATTERNS = [...FORMAT_BLOCKLIST, ...SENSITIVE_CONTENT_BLOCKLIST];

function isQualityKeyword(keyword: string): boolean {
  const k = keyword.toLowerCase();
  return !BLOCKLIST_PATTERNS.some((p) => k.includes(p));
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

// Distinct keywords across the whole catalog, with counts -- keeps only
// ones that appear on enough movies to be a real vocabulary term (not a
// one-off tag), and drops the format/meta blocklist before spending any
// embedding calls on them.
async function fetchKeywordVocabulary(): Promise<string[]> {
  const counts = new Map<string, number>();
  const PAGE_SIZE = 1000;
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
      for (const k of row.keywords ?? []) {
        counts.set(k, (counts.get(k) ?? 0) + 1);
      }
    }
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }

  return Array.from(counts.entries())
    .filter(([kw, n]) => n >= MIN_KEYWORD_COUNT && isQualityKeyword(kw))
    .map(([kw]) => kw);
}

async function embedKeywords(keywords: string[]): Promise<number[][]> {
  const embeddings: number[][] = [];
  for (let i = 0; i < keywords.length; i += EMBED_BATCH_SIZE) {
    const batch = keywords.slice(i, i + EMBED_BATCH_SIZE);
    const sorted = await embedBatchWithRetry(batch);
    for (const d of sorted) embeddings.push(d.embedding);
    console.log(`Embedded ${Math.min(i + EMBED_BATCH_SIZE, keywords.length)}/${keywords.length} keywords`);
  }
  return embeddings;
}

// Retries on 429 with backoff instead of assuming the account has
// standard rate limits -- confirmed via a real run that accounts without
// a payment method on file get capped at 3 requests/minute, which a
// tight back-to-back loop blows through in seconds. This makes the
// script work correctly regardless of which tier the account is on,
// rather than requiring the payment method to be added first.
async function embedBatchWithRetry(
  batch: string[],
  attempt = 1,
): Promise<{ embedding: number[]; index: number }[]> {
  const MAX_ATTEMPTS = 8;
  const res = await fetch('https://api.voyageai.com/v1/embeddings', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${VOYAGE_API_KEY}`,
    },
    body: JSON.stringify({
      input: batch,
      model: EMBED_MODEL,
      input_type: 'document',
      output_dimension: EMBED_DIMENSION,
    }),
  });

  if (res.status === 429) {
    if (attempt >= MAX_ATTEMPTS) throw new Error(`Voyage still rate-limiting after ${MAX_ATTEMPTS} attempts, giving up.`);
    // No payment method on file = 3 RPM, so a flat 21s wait clears one
    // request-slot; exponential backoff on top in case the real limit is
    // tighter than documented or something else is throttling too.
    const waitMs = 21_000 * attempt;
    console.log(`Rate limited (429), waiting ${Math.round(waitMs / 1000)}s before retry ${attempt + 1}/${MAX_ATTEMPTS}...`);
    await new Promise((resolve) => setTimeout(resolve, waitMs));
    return embedBatchWithRetry(batch, attempt + 1);
  }

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Voyage embeddings request failed: ${res.status} ${text}`);
  }

  const json = (await res.json()) as { data: { embedding: number[]; index: number }[] };
  return json.data.sort((a, b) => a.index - b.index);
}

function normalize(v: number[]): number[] {
  const mag = Math.sqrt(v.reduce((s, x) => s + x * x, 0));
  return mag === 0 ? v.slice() : v.map((x) => x / mag);
}

function squaredDist(a: number[], b: number[]): number {
  let s = 0;
  for (let i = 0; i < a.length; i++) {
    const d = a[i] - b[i];
    s += d * d;
  }
  return s;
}

function kmeansPlusPlusInit(vectors: number[][], k: number): number[][] {
  const centroids: number[][] = [vectors[Math.floor(Math.random() * vectors.length)].slice()];
  while (centroids.length < k) {
    const dists = vectors.map((v) => Math.min(...centroids.map((c) => squaredDist(v, c))));
    const sum = dists.reduce((a, b) => a + b, 0);
    let r = Math.random() * sum;
    let idx = 0;
    for (; idx < dists.length - 1; idx++) {
      r -= dists[idx];
      if (r <= 0) break;
    }
    centroids.push(vectors[idx].slice());
  }
  return centroids;
}

// Same Lloyd's-algorithm k-means as cluster-movies.ts, on normalized
// vectors so it's effectively cosine-based -- just a different input
// space (keyword embeddings, 1024-D) and a different K in practice.
function kmeans(vectors: number[][], k: number): { centroids: number[][]; assignments: number[] } {
  const n = vectors.length;
  const dims = vectors[0].length;
  let centroids = kmeansPlusPlusInit(vectors, k);
  let assignments = new Array(n).fill(-1);

  for (let iter = 0; iter < KMEANS_MAX_ITERATIONS; iter++) {
    let changed = false;
    for (let i = 0; i < n; i++) {
      let best = 0;
      let bestDist = Infinity;
      for (let c = 0; c < k; c++) {
        const d = squaredDist(vectors[i], centroids[c]);
        if (d < bestDist) {
          bestDist = d;
          best = c;
        }
      }
      if (assignments[i] !== best) {
        assignments[i] = best;
        changed = true;
      }
    }
    const sums = Array.from({ length: k }, () => new Array(dims).fill(0));
    const counts = new Array(k).fill(0);
    for (let i = 0; i < n; i++) {
      counts[assignments[i]]++;
      for (let d = 0; d < dims; d++) sums[assignments[i]][d] += vectors[i][d];
    }
    for (let c = 0; c < k; c++) {
      if (counts[c] === 0) continue;
      centroids[c] = sums[c].map((s) => s / counts[c]);
    }
    console.log(`k-means iteration ${iter + 1}: ${changed ? 'still moving' : 'converged'}`);
    if (!changed) break;
  }
  return { centroids, assignments };
}

async function labelGroup(keywords: string[]): Promise<string> {
  const sample = keywords.sort(() => Math.random() - 0.5).slice(0, LABEL_SAMPLE_SIZE);
  const response = await anthropic.messages.create({
    model: LABEL_MODEL,
    max_tokens: 20,
    system:
      "You name real groups of movie keywords for a recommendation site's browse categories. These groupings came from clustering actual keyword embeddings, not a rule -- synonyms and closely related terms landed together. Your job is to name what the group actually represents, in a form a moviegoer would recognize -- e.g. if the group contains \"virtual reality\", \"simulated reality\", \"artificial reality\", write something like \"Simulated Reality\", not a list of the terms. Write ONE short, specific label, 2-4 words, title case. Respond with only the label text: no quotes, no punctuation at the end, no explanation.",
    messages: [{ role: 'user', content: keywords.join(', ') }],
  });
  const text = response.content.find((b) => b.type === 'text');
  return text && 'text' in text ? text.text.trim() : 'Untitled Group';
}

function slugify(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
}

async function main() {
  console.log(`Starting keyword clustering${DRY_RUN ? ' (DRY RUN)' : ''}, K=${K}, min count=${MIN_KEYWORD_COUNT}...`);

  const vocabulary = await fetchKeywordVocabulary();
  console.log(`${vocabulary.length} keywords to cluster (after blocklist + min-count filter).`);
  if (vocabulary.length < K) throw new Error(`Fewer keywords (${vocabulary.length}) than K (${K}) -- lower K.`);

  console.log('Embedding keywords via Voyage AI...');
  const rawEmbeddings = await embedKeywords(vocabulary);
  const normalized = rawEmbeddings.map(normalize);

  console.log('Running k-means...');
  const { centroids, assignments } = kmeans(normalized, K);

  const memberIndices: number[][] = Array.from({ length: K }, () => []);
  for (let i = 0; i < assignments.length; i++) memberIndices[assignments[i]].push(i);

  console.log('Labeling groups via Claude (one call per group)...');
  const usedSlugs = new Set<string>();
  const groupRows: { keywords: string[]; label: string; centroid: number[] }[] = [];

  for (let c = 0; c < K; c++) {
    const members = memberIndices[c].map((i) => vocabulary[i]);
    if (members.length === 0) continue;
    const label = DRY_RUN ? `[dry-run group ${c}]` : await labelGroup(members);
    console.log(`Group ${c}: "${label}" (${members.length} keywords) -- e.g. ${members.slice(0, 5).join(', ')}`);
    groupRows.push({ keywords: members, label, centroid: centroids[c] });
  }

  if (DRY_RUN) {
    console.log('Dry run: no groups or memberships written.');
    return;
  }

  const vocabularyIndex = new Map(vocabulary.map((kw, i) => [kw, i]));

  // This is a full re-cluster -- a new run with a different K (or just a
  // different random init) produces a different grouping of the same
  // keywords, so old rows can't coexist with new ones. Confirmed via a
  // real run: re-running at a new K without this hit a primary-key
  // violation ("hit-and-run" already existed from the prior K=200 run
  // when this K=250 run tried to write it into a different group).
  // keyword_group_members cascades from keyword_groups (on delete
  // cascade), so deleting the groups is enough.
  console.log("Clearing previous run's groups...");
  const { error: clearError } = await supabase.from('keyword_groups').delete().not('id', 'is', null);
  if (clearError) throw clearError;

  console.log('Writing groups and memberships...');
  for (const group of groupRows) {
    let slug = slugify(group.label);
    let suffix = 1;
    while (usedSlugs.has(slug)) {
      slug = `${slugify(group.label)}-${++suffix}`;
    }
    usedSlugs.add(slug);

    const { data, error } = await supabase
      .from('keyword_groups')
      .insert({ slug, label: group.label, centroid: `[${group.centroid.join(',')}]`, keyword_count: group.keywords.length })
      .select('id')
      .single();
    if (error) throw error;

    const memberRows = group.keywords.map((kw) => ({
      keyword: kw,
      group_id: data.id,
      distance: Math.sqrt(squaredDist(normalized[vocabularyIndex.get(kw)!], group.centroid)),
    }));
    const { error: memberError } = await supabase.from('keyword_group_members').insert(memberRows);
    if (memberError) throw memberError;
  }

  console.log(`Done. ${groupRows.length} groups written.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
