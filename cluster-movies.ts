// Full theme-cluster discovery: k-means over every scored movie's
// essence_vector, then one LLM call per cluster to name it from its
// actual members' essence summaries. A movie can land in up to 4
// clusters, not just its nearest.
//
// Run via workflow_dispatch, not scheduled -- this redefines the whole
// cluster taxonomy (centroids can shift, cluster count/identity can
// change), which is a bigger decision than the daily incremental script
// (assign-movie-clusters.ts) should make unattended. Re-run by hand as
// the catalog grows enough to warrant it.
//
// Secondary-cluster eligibility (v2, replacing the original radius
// approach): a movie's 2nd/3rd/4th-nearest cluster qualifies if its
// distance is within SECONDARY_MULTIPLIER times THIS MOVIE'S OWN best
// (native) distance -- movie-relative, not tied to a per-cluster
// percentile. The original version used each cluster's own internal
// spread as the radius, which turned out to be unstable here: the 200
// clusters sit close enough together in this space that a small
// percentile change flipped results from ~77% of movies stuck at a
// single cluster to 100% of movies maxed out at 4. Tested several
// multiplier values against the live data before choosing 1.4 (real
// spread: ~46%/25%/14%/16% across 1/2/3/4 clusters) -- see the
// migration that first corrected this live (fix_cluster_assignments_
// relative_distance) for the actual comparison.
//
// Verified before the original version of this script was written:
// k-means/radius logic smoke-tested against synthetic well-separated
// data (100% correct recovery of 3 known groups), since this sandbox has
// no network to test end-to-end against Supabase directly; dimension
// order for essence_vector confirmed against cinnamon-scoring/prompt.ts
// (not needed by this script itself, since it clusters on raw vector
// distance and labels from essence_summary rather than individual
// dimension values).
//
// Required env vars: ANTHROPIC_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=true, K=<int> (default 200)

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const ANTHROPIC_API_KEY = requireEnv('ANTHROPIC_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const DRY_RUN = process.env.DRY_RUN === 'true';
const K = process.env.K ? parseInt(process.env.K, 10) : 200;
const KMEANS_MAX_ITERATIONS = 100;
const SECONDARY_MULTIPLIER = 1.4; // a secondary cluster qualifies if within this multiple of the movie's own native distance
const MAX_CLUSTERS_PER_MOVIE = 4; // native + up to 3 secondary
const LABEL_SAMPLE_SIZE = 25; // movies sampled per cluster for the naming call
const LABEL_MODEL = 'claude-sonnet-5';
const PAGE_SIZE = 1000;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

interface MovieRow {
  id: string;
  title: string;
  essence_summary: string | null;
  vector: number[];
}

async function fetchAllScoredMovies(): Promise<MovieRow[]> {
  const all: MovieRow[] = [];
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from('movies')
      .select('id, title, essence_summary, essence_vector')
      .eq('scoring_status', 'scored')
      .not('essence_vector', 'is', null)
      .order('id', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    for (const row of page as any[]) {
      // essence_vector comes back as a pgvector string literal, "[1,2,3,...]"
      const vector = JSON.parse(row.essence_vector) as number[];
      all.push({ id: row.id, title: row.title, essence_summary: row.essence_summary, vector });
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

// Squared Euclidean on normalized vectors == monotonic in cosine distance,
// so ordinary Lloyd's-algorithm k-means on these produces cosine-based
// clusters -- consistent with the cosine similarity used everywhere else
// in this project (movie_neighbors, director/actor/studio similarity),
// rather than introducing a different notion of "close" just here.
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
      if (counts[c] === 0) continue; // leave an empty centroid in place rather than divide by zero
      centroids[c] = sums[c].map((s) => s / counts[c]);
    }

    console.log(`k-means iteration ${iter + 1}: ${changed ? 'still moving' : 'converged'}`);
    if (!changed) break;
  }

  return { centroids, assignments };
}

async function labelCluster(members: MovieRow[]): Promise<string> {
  const sample = members
    .filter((m) => m.essence_summary)
    .sort(() => Math.random() - 0.5)
    .slice(0, LABEL_SAMPLE_SIZE);

  const listing = sample.map((m) => `- ${m.title}: ${m.essence_summary}`).join('\n');

  const response = await anthropic.messages.create({
    model: LABEL_MODEL,
    max_tokens: 30,
    // No temperature param -- claude-sonnet-5 rejects it outright
    // ("temperature is deprecated for this model").
    system:
      "You name real clusters of movies for a recommendation site's browse categories. These groupings came from clustering actual emotional/tonal data, not from a rule -- your job is only to describe what genuinely unites this specific group, in the group's own terms. Write ONE short, specific, evocative label, 2-5 words. Not a generic genre name, and not a mechanical adjective-plus-noun template applied the same way every time -- look at what these particular descriptions actually share and name that. Respond with only the label text: no quotes, no punctuation at the end, no explanation.",
    messages: [{ role: 'user', content: listing }],
  });

  const text = response.content.find((b) => b.type === 'text');
  return text && 'text' in text ? text.text.trim() : 'Untitled Cluster';
}

async function main() {
  console.log(`Starting movie clustering${DRY_RUN ? ' (DRY RUN)' : ''}, K=${K}...`);

  console.log('Fetching all scored movies with essence_vector...');
  const movies = await fetchAllScoredMovies();
  console.log(`${movies.length} movies to cluster.`);
  if (movies.length < K) throw new Error(`Fewer movies (${movies.length}) than K (${K}) -- lower K.`);

  const normalized = movies.map((m) => normalize(m.vector));

  console.log('Running k-means...');
  const { centroids, assignments } = kmeans(normalized, K);

  const nativeIndices: number[][] = Array.from({ length: K }, () => []);
  for (let i = 0; i < assignments.length; i++) nativeIndices[assignments[i]].push(i);

  console.log('Labeling clusters via Claude (one call per cluster)...');
  const clusterIds: string[] = [];
  for (let c = 0; c < K; c++) {
    const members = nativeIndices[c].map((i) => movies[i]);
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

    const { data, error } = await supabase
      .from('movie_theme_clusters')
      .insert({
        label,
        centroid: `[${centroids[c].join(',')}]`,
        film_count: members.length,
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

  console.log(`Computing assignments (native + up to ${MAX_CLUSTERS_PER_MOVIE - 1} secondary, within ${SECONDARY_MULTIPLIER}x native distance)...`);
  const assignmentRows: { movie_id: string; cluster_id: string; distance: number }[] = [];
  for (let i = 0; i < movies.length; i++) {
    const distances: { c: number; d: number }[] = [];
    for (let c = 0; c < K; c++) {
      if (!clusterIds[c]) continue;
      distances.push({ c, d: Math.sqrt(squaredDist(normalized[i], centroids[c])) });
    }
    distances.sort((a, b) => a.d - b.d);
    if (distances.length === 0) continue;

    const nativeDistance = distances[0].d;
    const qualifying = distances
      .slice(0, MAX_CLUSTERS_PER_MOVIE)
      .filter((x, idx) => idx === 0 || x.d <= nativeDistance * SECONDARY_MULTIPLIER);

    for (const q of qualifying) {
      assignmentRows.push({ movie_id: movies[i].id, cluster_id: clusterIds[q.c], distance: q.d });
    }
  }

  console.log(`Writing ${assignmentRows.length} assignments...`);
  const WRITE_CHUNK = 1000;
  for (let i = 0; i < assignmentRows.length; i += WRITE_CHUNK) {
    const chunk = assignmentRows.slice(i, i + WRITE_CHUNK);
    const { error } = await supabase.from('movie_cluster_assignments').insert(chunk);
    if (error) throw error;
    console.log(`  ${Math.min(i + WRITE_CHUNK, assignmentRows.length)}/${assignmentRows.length}`);
  }

  console.log('Done.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
