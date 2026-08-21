// Incremental companion to cluster-movies.ts: assigns any scored movie
// that doesn't have a cluster assignment yet to the EXISTING clusters
// (native + up to 3 secondary, same movie-relative-distance rule as the
// full pipeline -- see cluster-movies.ts header). No LLM calls, no
// re-clustering, no centroid changes -- just distance math against
// clusters that already exist, which is why this is safe to run on a
// schedule while cluster-movies.ts (which can reshape the whole
// taxonomy) stays a manual, deliberate trigger.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=true

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');
const DRY_RUN = process.env.DRY_RUN === 'true';

const SECONDARY_MULTIPLIER = 1.4; // must match cluster-movies.ts
const MAX_CLUSTERS_PER_MOVIE = 4; // must match cluster-movies.ts
const PAGE_SIZE = 1000;

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

interface Cluster {
  id: string;
  centroid: number[];
}

async function fetchClusters(): Promise<Cluster[]> {
  const { data, error } = await supabase.from('movie_theme_clusters').select('id, centroid');
  if (error) throw error;
  return (data ?? []).map((c: any) => ({ id: c.id, centroid: normalize(JSON.parse(c.centroid)) }));
}

async function fetchUnassignedMovies(): Promise<{ id: string; vector: number[] }[]> {
  // Movies scored but not present in movie_cluster_assignments at all --
  // newly scored since the last run of either script.
  const { data: assignedRows, error: e1 } = await supabase
    .from('movie_cluster_assignments')
    .select('movie_id');
  if (e1) throw e1;
  const assignedIds = new Set((assignedRows ?? []).map((r: any) => r.movie_id));

  const all: { id: string; vector: number[] }[] = [];
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from('movies')
      .select('id, essence_vector')
      .eq('scoring_status', 'scored')
      .not('essence_vector', 'is', null)
      .order('id', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    for (const row of page as any[]) {
      if (assignedIds.has(row.id)) continue;
      all.push({ id: row.id, vector: JSON.parse(row.essence_vector) });
    }
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return all;
}

async function main() {
  console.log(`Starting incremental cluster assignment${DRY_RUN ? ' (DRY RUN)' : ''}...`);

  const clusters = await fetchClusters();
  console.log(`${clusters.length} existing clusters loaded.`);
  if (clusters.length === 0) {
    console.log('No clusters exist yet -- run cluster-movies.ts first. Nothing to do.');
    return;
  }

  const unassigned = await fetchUnassignedMovies();
  console.log(`${unassigned.length} scored movies without a cluster assignment.`);
  if (unassigned.length === 0) {
    console.log('Nothing to do.');
    return;
  }

  const rows: { movie_id: string; cluster_id: string; distance: number }[] = [];
  for (const movie of unassigned) {
    const v = normalize(movie.vector);
    const distances = clusters
      .map((c) => ({ cluster: c, d: dist(v, c.centroid) }))
      .sort((a, b) => a.d - b.d);

    const nativeDistance = distances[0].d;
    const qualifying = distances
      .slice(0, MAX_CLUSTERS_PER_MOVIE)
      .filter((x, idx) => idx === 0 || x.d <= nativeDistance * SECONDARY_MULTIPLIER);

    for (const q of qualifying) {
      rows.push({ movie_id: movie.id, cluster_id: q.cluster.id, distance: q.d });
    }
  }

  console.log(`${rows.length} assignment rows to write (${unassigned.length} movies).`);
  if (DRY_RUN) {
    console.log('Dry run: nothing written.');
    return;
  }

  const WRITE_CHUNK = 1000;
  for (let i = 0; i < rows.length; i += WRITE_CHUNK) {
    const chunk = rows.slice(i, i + WRITE_CHUNK);
    const { error } = await supabase.from('movie_cluster_assignments').insert(chunk);
    if (error) throw error;
    console.log(`  ${Math.min(i + WRITE_CHUNK, rows.length)}/${rows.length}`);
  }

  console.log('Done.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
