// Recomputes movie_cluster_assignments against the EXISTING 200 clusters
// with a new SECONDARY_MULTIPLIER -- deliberately does NOT re-run
// k-means or re-label clusters. A full cluster-movies.ts re-run would
// also shift centroids and rename clusters via fresh LLM calls, which is
// a much bigger disruption than what this change actually calls for:
// "some movies should qualify for a 2nd/3rd cluster that don't
// currently," not "redefine what the 200 clusters are." Same technique
// as assign-movie-clusters.ts's distance math, applied to every movie
// (not just unassigned ones) against the current, unchanged centroids.
//
// Multiplier moved from 1.4x to 1.6x -- checked directly first: keyword-
// group coverage is already 99.1% of the vocabulary (assign-keyword-
// groups.ts already closed that gap), so the remaining shortfall against
// the "5 categories" target is specifically about movies capped at 1
// tonal cluster (46% of the catalog under the old multiplier), not a
// keyword problem. A conservative loosening, not an aggressive one --
// re-run and check the real distribution before considering going
// further.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=true, MULTIPLIER=<number> (default 1.6)

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');
const DRY_RUN = process.env.DRY_RUN === 'true';
const SECONDARY_MULTIPLIER = process.env.MULTIPLIER ? parseFloat(process.env.MULTIPLIER) : 1.6;
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

function squaredDist(a: number[], b: number[]): number {
  let s = 0;
  for (let i = 0; i < a.length; i++) {
    const d = a[i] - b[i];
    s += d * d;
  }
  return s;
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

async function fetchAllScoredMovies(): Promise<{ id: string; vector: number[] }[]> {
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
    for (const row of page as any[]) all.push({ id: row.id, vector: JSON.parse(row.essence_vector) });
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return all;
}

async function main() {
  console.log(`Recomputing cluster assignments${DRY_RUN ? ' (DRY RUN)' : ''}, multiplier=${SECONDARY_MULTIPLIER}...`);

  const clusters = await fetchClusters();
  console.log(`${clusters.length} existing clusters (centroids untouched).`);

  const movies = await fetchAllScoredMovies();
  console.log(`${movies.length} scored movies to reassign.`);

  const rows: { movie_id: string; cluster_id: string; distance: number }[] = [];
  for (const movie of movies) {
    const v = normalize(movie.vector);
    const distances = clusters
      .map((c) => ({ cluster: c, d: Math.sqrt(squaredDist(v, c.centroid)) }))
      .sort((a, b) => a.d - b.d);

    const nativeDistance = distances[0].d;
    const qualifying = distances
      .slice(0, MAX_CLUSTERS_PER_MOVIE)
      .filter((x, idx) => idx === 0 || x.d <= nativeDistance * SECONDARY_MULTIPLIER);

    for (const q of qualifying) {
      rows.push({ movie_id: movie.id, cluster_id: q.cluster.id, distance: q.d });
    }
  }

  const perMovieCounts = new Map<string, number>();
  for (const r of rows) perMovieCounts.set(r.movie_id, (perMovieCounts.get(r.movie_id) ?? 0) + 1);
  const dist = [1, 2, 3, 4].map((n) => Array.from(perMovieCounts.values()).filter((c) => c === n).length);
  console.log(`Projected distribution (1/2/3/4 clusters): ${dist.join('/')} (total ${rows.length} assignment rows for ${movies.length} movies)`);

  if (DRY_RUN) {
    console.log('Dry run: nothing written.');
    return;
  }

  console.log('Clearing existing assignments...');
  const { error: clearError } = await supabase.from('movie_cluster_assignments').delete().not('movie_id', 'is', null);
  if (clearError) throw clearError;

  console.log(`Writing ${rows.length} assignments...`);
  const WRITE_CHUNK = 1000;
  for (let i = 0; i < rows.length; i += WRITE_CHUNK) {
    const chunk = rows.slice(i, i + WRITE_CHUNK);
    const { error } = await supabase.from('movie_cluster_assignments').insert(chunk);
    if (error) throw error;
    console.log(`  ${Math.min(i + WRITE_CHUNK, rows.length)}/${rows.length}`);
  }

  console.log('Refreshing cluster popularity...');
  const { error: refreshError } = await supabase.rpc('refresh_cluster_popularity');
  if (refreshError) throw refreshError;

  console.log('Done.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
