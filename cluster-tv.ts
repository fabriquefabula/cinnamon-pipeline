// Full TV theme-cluster discovery: k-means over every scored show's
// essence_vector, then one LLM call per cluster to name it from its
// actual members' essence summaries. A show can land in up to 4
// clusters, not just its nearest.
//
// Mirrors cluster-movies.ts against the TV silo. The clusters are a
// separate space entirely -- computed over TV only, served under
// /tv/collection/[slug] -- so a TV cluster can never contain a film and
// slugs cannot collide across the two taxonomies.
//
// Run via workflow_dispatch, not scheduled: this redefines the whole TV
// cluster taxonomy, which is a bigger decision than an unattended job
// should make.
//
// Required env vars: ANTHROPIC_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=true, K=<int>

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const ANTHROPIC_API_KEY = requireEnv('ANTHROPIC_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const DRY_RUN = process.env.DRY_RUN === 'true';
// Chosen to preserve the movie side's cluster GRAIN rather than its
// cluster COUNT. Movies run K=200 over 48,708 films, about 244 films per
// cluster; 50 over ~10,166 shows gives about 203 each. Copying K=200
// straight across would have produced ~51-member clusters -- a different
// and much narrower kind of category than the movie ones, which would
// then look inconsistent sitting next to them in the same UI.
const K = process.env.K ? parseInt(process.env.K, 10) : 50;
const KMEANS_MAX_ITERATIONS = 100;
const SECONDARY_MULTIPLIER = 1.4; // carried over from movies; retune against real TV spread if the 1/2/3/4-cluster distribution looks degenerate
const MAX_CLUSTERS_PER_SHOW = 4;
const LABEL_SAMPLE_SIZE = 25;
const LABEL_MODEL = 'claude-sonnet-5';
const PAGE_SIZE = 1000;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

interface ShowRow {
  id: string;
  title: string;
  essence_summary: string | null;
  vote_count: number | null;
  vector: number[];
}

async function fetchAllScoredShows(): Promise<ShowRow[]> {
  const all: ShowRow[] = [];
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from('tv_shows')
      .select('id, title, essence_summary, vote_count, essence_vector')
      .eq('scoring_status', 'scored')
      .not('essence_vector', 'is', null)
      .order('id', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    for (const row of page as any[]) {
      // essence_vector comes back as a pgvector string literal, "[1,2,3,...]"
      const vector = JSON.parse(row.essence_vector) as number[];
      all.push({
        id: row.id,
        title: row.title,
        essence_summary: row.essence_summary,
        vote_count: row.vote_count,
        vector,
      });
    }
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return all;
}

function normalize(v: number[]): number[] {
  const mag = Math.sqrt(v.reduce((s, x) => s + x * x, 0));
  return mag === 0 ? v.slice() : v.map((x) => x / mag);
}

// Squared Euclidean on normalized vectors is monotonic in cosine
// distance, so plain Lloyd's k-means here produces cosine-based clusters
// -- consistent with the cosine similarity used everywhere else rather
// than introducing a second notion of "close".
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

function kmeans(vectors: number[][], k: number): { centroids: number[][]; assignments: number[] } {
  const n = vectors.length;
  const dims = vectors[0].length;
  const centroids = kmeansPlusPlusInit(vectors, k);
  const assignments = new Array(n).fill(-1);

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
      if (counts[c] === 0) continue; // leave an empty centroid rather than divide by zero
      centroids[c] = sums[c].map((s) => s / counts[c]);
    }

    console.log(`k-means iteration ${iter + 1}: ${changed ? 'still moving' : 'converged'}`);
    if (!changed) break;
  }

  return { centroids, assignments };
}

// tv_theme_clusters.slug is UNIQUE and is the /tv/collection/[slug] path
// segment, so it has to be generated here rather than backfilled later:
// a null slug means an unreachable cluster page. The movie table gets
// its slugs from a separate later script, which is why movie clusters
// existed for a while with no browsable URL.
function slugify(label: string, taken: Set<string>): string {
  const base =
    label
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, '')
      .trim()
      .replace(/\s+/g, '-')
      .slice(0, 60) || 'cluster';
  let slug = base;
  let n = 2;
  while (taken.has(slug)) slug = `${base}-${n++}`;
  taken.add(slug);
  return slug;
}

async function labelCluster(members: ShowRow[]): Promise<string> {
  const sample = members
    .filter((m) => m.essence_summary)
    .sort(() => Math.random() - 0.5)
    .slice(0, LABEL_SAMPLE_SIZE);

  const listing = sample.map((m) => `- ${m.title}: ${m.essence_summary}`).join('\n');

  const response = await anthropic.messages.create({
    model: LABEL_MODEL,
    max_tokens: 30,
    // No temperature param -- claude-sonnet-5 rejects it outright.
    system:
      "You name real clusters of television series for a recommendation site's browse categories. These groupings came from clustering actual emotional/tonal data, not from a rule -- your job is only to describe what genuinely unites this specific group, in the group's own terms. Write ONE short, specific, evocative label, 2-5 words. Not a generic genre name, and not a mechanical adjective-plus-noun template applied the same way every time -- look at what these particular descriptions actually share and name that. Respond with only the label text: no quotes, no punctuation at the end, no explanation.",
    messages: [{ role: 'user', content: listing }],
  });

  const text = response.content.find((b) => b.type === 'text');
  return text && 'text' in text ? text.text.trim() : 'Untitled Cluster';
}

async function main() {
  console.log(`Starting TV clustering${DRY_RUN ? ' (DRY RUN)' : ''}, K=${K}...`);

  console.log('Fetching all scored shows with essence_vector...');
  const shows = await fetchAllScoredShows();
  console.log(`${shows.length} shows to cluster (~${Math.round(shows.length / K)} per cluster).`);
  if (shows.length < K) throw new Error(`Fewer shows (${shows.length}) than K (${K}) -- lower K.`);

  const normalized = shows.map((m) => normalize(m.vector));

  console.log('Running k-means...');
  const { centroids, assignments } = kmeans(normalized, K);

  const nativeIndices: number[][] = Array.from({ length: K }, () => []);
  for (let i = 0; i < assignments.length; i++) nativeIndices[assignments[i]].push(i);

  console.log('Labeling clusters via Claude (one call per cluster)...');
  const clusterIds: string[] = [];
  const takenSlugs = new Set<string>();

  for (let c = 0; c < K; c++) {
    const memberIdx = nativeIndices[c];
    const members = memberIdx.map((i) => shows[i]);
    if (members.length === 0) {
      clusterIds.push('');
      continue;
    }
    const label = DRY_RUN ? `[dry-run cluster ${c}]` : await labelCluster(members);
    console.log(`Cluster ${c}: "${label}" (${members.length} native members)`);

    if (DRY_RUN) {
      clusterIds.push('');
      continue;
    }

    // radius is NOT NULL on tv_theme_clusters (it is nullable on the
    // movie table). Mean native-member distance is the honest reading of
    // "how tight is this cluster" and is what the column is for.
    const dists = memberIdx.map((i) => Math.sqrt(squaredDist(normalized[i], centroids[c])));
    const radius = dists.reduce((a, b) => a + b, 0) / dists.length;

    const voteCounts = members.map((m) => m.vote_count ?? 0);
    const avgVoteCount = voteCounts.reduce((a, b) => a + b, 0) / voteCounts.length;

    const { data, error } = await supabase
      .from('tv_theme_clusters')
      .insert({
        label,
        slug: slugify(label, takenSlugs),
        centroid: `[${centroids[c].join(',')}]`,
        radius,
        show_count: members.length,
        avg_vote_count: avgVoteCount,
      })
      .select('id')
      .single();
    if (error) throw error;
    clusterIds.push(data.id);
  }

  if (DRY_RUN) {
    console.log('Dry run: no clusters or assignments written.');
    return;
  }

  console.log(
    `Computing assignments (native + up to ${MAX_CLUSTERS_PER_SHOW - 1} secondary, within ${SECONDARY_MULTIPLIER}x native distance)...`,
  );
  const assignmentRows: { show_id: string; cluster_id: string; distance: number }[] = [];
  for (let i = 0; i < shows.length; i++) {
    const distances: { c: number; d: number }[] = [];
    for (let c = 0; c < K; c++) {
      if (!clusterIds[c]) continue;
      distances.push({ c, d: Math.sqrt(squaredDist(normalized[i], centroids[c])) });
    }
    distances.sort((a, b) => a.d - b.d);
    if (distances.length === 0) continue;

    const nativeDistance = distances[0].d;
    const qualifying = distances
      .slice(0, MAX_CLUSTERS_PER_SHOW)
      .filter((x, idx) => idx === 0 || x.d <= nativeDistance * SECONDARY_MULTIPLIER);

    for (const q of qualifying) {
      assignmentRows.push({ show_id: shows[i].id, cluster_id: clusterIds[q.c], distance: q.d });
    }
  }

  // Report the spread before writing. On the movie side a bad
  // SECONDARY_MULTIPLIER silently produced either ~77% of titles stuck at
  // one cluster or 100% maxed at four, and neither is visible from the
  // row count alone.
  const perShow = new Map<string, number>();
  for (const r of assignmentRows) perShow.set(r.show_id, (perShow.get(r.show_id) ?? 0) + 1);
  const spread = [0, 0, 0, 0];
  for (const n of perShow.values()) spread[Math.min(n, 4) - 1]++;
  const total = perShow.size;
  console.log(
    `Cluster-count spread: ${spread.map((s, i) => `${i + 1}:${Math.round((100 * s) / total)}%`).join('  ')}`,
  );

  console.log(`Writing ${assignmentRows.length} assignments...`);
  const WRITE_CHUNK = 1000;
  for (let i = 0; i < assignmentRows.length; i += WRITE_CHUNK) {
    const chunk = assignmentRows.slice(i, i + WRITE_CHUNK);
    const { error } = await supabase.from('tv_cluster_assignments').insert(chunk);
    if (error) throw error;
    console.log(`  ${Math.min(i + WRITE_CHUNK, assignmentRows.length)}/${assignmentRows.length}`);
  }

  console.log('Done.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
