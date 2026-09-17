// Incremental companion to cluster-tv.ts: assigns any scored TV show
// that doesn't have a cluster assignment yet to the EXISTING clusters
// (native + up to 3 secondary), by distance math alone. No LLM calls, no
// re-clustering, no centroid changes.
//
// That distinction is the whole reason this file exists. cluster-tv.ts
// refits k-means from scratch, which reassigns every show and rewrites
// every /tv/collection/ slug -- it is a deliberate, manual act and must
// never be put on a schedule. Until now there was no safe automated way
// to place a newly scored show, so the weekly ingest -> submit -> collect
// chain ended with shows that were scored but permanently unclustered,
// and therefore unrailed and invisible on the site.
//
// SECONDARY_MULTIPLIER is 1.4 here and this is NOT a copy of the movie
// side's value. The TV threshold was derived empirically against the TV
// distance distribution; movie and TV cluster parameters differ and
// carrying one over silently changes how many collections a show appears
// in. If cluster-tv.ts ever changes its multiplier, change it here too.
//
// Also refreshes avg_vote_count/show_count on every cluster each run, the
// same way assign-movie-clusters.ts does for films -- vote_count drifts
// constantly and this script adds shows to clusters, so the counters go
// stale otherwise.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=true

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');
const DRY_RUN = process.env.DRY_RUN === 'true';

const SECONDARY_MULTIPLIER = 1.4; // must match cluster-tv.ts -- see header
const MAX_CLUSTERS_PER_SHOW = 4; // must match cluster-tv.ts
const PAGE_SIZE = 1000;
const WRITE_CHUNK = 1000;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

// Both tv_shows.essence_vector and tv_theme_clusters.centroid are pgvector
// columns. PostgREST serialises those as the string "[0.1,0.2,...]", but
// tolerate a real array too rather than depending on that representation.
function parseVec(raw: unknown): number[] {
  if (Array.isArray(raw)) return raw as number[];
  if (typeof raw === 'string') return JSON.parse(raw) as number[];
  throw new Error(`Unexpected vector representation: ${typeof raw}`);
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
  const { data, error } = await supabase.from('tv_theme_clusters').select('id, centroid');
  if (error) throw error;
  return (data ?? []).map((c: any) => ({ id: c.id, centroid: normalize(parseVec(c.centroid)) }));
}

async function fetchUnassignedShows(): Promise<{ id: string; vector: number[] }[]> {
  // Shows scored but not present in tv_cluster_assignments at all --
  // newly scored since the last run of either script.
  const assignedIds = new Set<string>();
  let afrom = 0;
  while (true) {
    const { data, error } = await supabase
      .from('tv_cluster_assignments')
      .select('show_id')
      .range(afrom, afrom + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    for (const row of page as any[]) assignedIds.add(row.show_id);
    if (page.length < PAGE_SIZE) break;
    afrom += PAGE_SIZE;
  }

  // Explicit ordered paging. A plain .select() silently truncates at
  // Supabase's 1000-row default, and OFFSET paging without a
  // deterministic sort can repeat or skip rows.
  const all: { id: string; vector: number[] }[] = [];
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from('tv_shows')
      .select('id, essence_vector')
      .eq('scoring_status', 'scored')
      .not('essence_vector', 'is', null)
      .order('id', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = data ?? [];
    for (const row of page as any[]) {
      if (assignedIds.has(row.id)) continue;
      all.push({ id: row.id, vector: parseVec(row.essence_vector) });
    }
    if (page.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return all;
}

async function main() {
  console.log(`Starting incremental TV cluster assignment${DRY_RUN ? ' (DRY RUN)' : ''}...`);

  const clusters = await fetchClusters();
  console.log(`${clusters.length} existing TV clusters loaded.`);
  if (clusters.length === 0) {
    console.log('No TV clusters exist yet -- run cluster-tv.ts first. Nothing to do.');
    return;
  }

  const unassigned = await fetchUnassignedShows();
  console.log(`${unassigned.length} scored shows without a cluster assignment.`);

  if (unassigned.length > 0) {
    const rows: { show_id: string; cluster_id: string; distance: number }[] = [];
    for (const show of unassigned) {
      const v = normalize(show.vector);
      const distances = clusters
        .map((c) => ({ cluster: c, d: dist(v, c.centroid) }))
        .sort((a, b) => a.d - b.d);

      const nativeDistance = distances[0].d;
      const qualifying = distances
        .slice(0, MAX_CLUSTERS_PER_SHOW)
        .filter((x, idx) => idx === 0 || x.d <= nativeDistance * SECONDARY_MULTIPLIER);

      for (const q of qualifying) {
        rows.push({ show_id: show.id, cluster_id: q.cluster.id, distance: q.d });
      }
    }

    console.log(`${rows.length} assignment rows to write.`);
    if (!DRY_RUN) {
      for (let i = 0; i < rows.length; i += WRITE_CHUNK) {
        const chunk = rows.slice(i, i + WRITE_CHUNK);
        const { error } = await supabase.from('tv_cluster_assignments').insert(chunk);
        if (error) throw error;
        console.log(`  ${Math.min(i + WRITE_CHUNK, rows.length)}/${rows.length}`);
      }
    } else {
      console.log('Dry run: nothing written.');
    }
  } else {
    console.log('No new assignments needed.');
  }

  if (DRY_RUN) {
    console.log('Dry run: skipping cluster popularity refresh.');
    return;
  }

  console.log('Refreshing TV cluster popularity (avg_vote_count, show_count)...');
  const { data: refreshed, error: refreshError } = await supabase.rpc('refresh_tv_cluster_popularity');
  if (refreshError) throw refreshError;
  console.log(`Popularity refreshed for ${refreshed} clusters.`);

  console.log('Done.');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
