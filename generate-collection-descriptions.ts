// Generates 2-3 sentence intro copy for collection pages (tonal
// clusters + keyword groups, across BOTH catalogues) -- grounded in real
// member titles, same sampling approach already validated for labeling
// (cluster-movies.ts / cluster-tv.ts / cluster-keywords.ts), extended to
// also write a description. A new, separate incremental script rather
// than folding into the full re-cluster pipelines: the clusters/groups
// already exist and don't need to be redone, just enriched with one new
// field. Re-running a full k-means clustering just to add copy would be
// the wrong tool for the job.
//
// TV was missing entirely until now. Measured against live data: all 200
// movie clusters and 245 of 246 keyword groups had descriptions, while
// 0 of 50 TV clusters did -- the TV taxonomy was built and shipped, but
// this pass was never extended to cover it, so every /tv/collection page
// and every TV cluster chip appeared with a bare label and no
// explanation of what the grouping actually means.
//
// DRY_RUN defaults true on purpose -- this writes new public-facing
// copy, and generic/templated output here would be worse than no copy at
// all (the exact failure mode "helpful content" search systems are built
// to detect). Review the dry-run output before running for real.
//
// Required env vars: ANTHROPIC_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=false to actually write (defaults to true), LIMIT=<int> to cap
//           how many are processed, MEDIA=movie|tv|all to restrict scope (defaults all)

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const ANTHROPIC_API_KEY = requireEnv('ANTHROPIC_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const DRY_RUN = process.env.DRY_RUN !== 'false'; // defaults to true -- must explicitly opt out
const LIMIT = process.env.LIMIT ? parseInt(process.env.LIMIT, 10) : undefined;
// Scope control, mainly so the TV backfill can be run and reviewed on
// its own without re-walking the movie side that is already complete.
const MEDIA = (process.env.MEDIA ?? 'all').toLowerCase() as 'movie' | 'tv' | 'all';
const SAMPLE_SIZE = 20;
const DESCRIPTION_MODEL = 'claude-sonnet-5';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

interface CollectionTarget {
  table: 'movie_theme_clusters' | 'tv_theme_clusters' | 'keyword_groups';
  id: string;
  label: string;
  // Drives the wording of the prompt. Keyword groups are movie-derived,
  // so they carry 'movie'.
  media: 'movie' | 'tv';
}

async function fetchTonalClustersNeedingDescription(): Promise<CollectionTarget[]> {
  let query = supabase.from('movie_theme_clusters').select('id, label').is('description', null);
  if (LIMIT) query = query.limit(LIMIT);
  const { data, error } = await query;
  if (error) throw error;
  return (data ?? []).map((c: any) => ({
    table: 'movie_theme_clusters' as const,
    id: c.id,
    label: c.label,
    media: 'movie' as const,
  }));
}

// TV clusters are a completely separate taxonomy from the movie ones --
// independently fitted centroids, independently generated labels and
// slugs. They are not a view onto the movie clusters and must be
// described from their own members.
async function fetchTvClustersNeedingDescription(): Promise<CollectionTarget[]> {
  let query = supabase.from('tv_theme_clusters').select('id, label').is('description', null);
  if (LIMIT) query = query.limit(LIMIT);
  const { data, error } = await query;
  if (error) throw error;
  return (data ?? []).map((c: any) => ({
    table: 'tv_theme_clusters' as const,
    id: c.id,
    label: c.label,
    media: 'tv' as const,
  }));
}

async function fetchKeywordGroupsNeedingDescription(): Promise<CollectionTarget[]> {
  let query = supabase.from('keyword_groups').select('id, label').is('description', null);
  if (LIMIT) query = query.limit(LIMIT);
  const { data, error } = await query;
  if (error) throw error;
  return (data ?? []).map((g: any) => ({
    table: 'keyword_groups' as const,
    id: g.id,
    label: g.label,
    media: 'movie' as const,
  }));
}

async function sampleTonalClusterMovies(clusterId: string): Promise<string[]> {
  const { data, error } = await supabase
    .from('movie_cluster_assignments')
    .select('movie:movie_id(title, essence_summary)')
    .eq('cluster_id', clusterId)
    .order('distance', { ascending: true })
    .limit(SAMPLE_SIZE);
  if (error) throw error;
  return ((data ?? []) as any[])
    .map((row) => row.movie)
    .filter((m) => m?.essence_summary)
    .map((m) => `- ${m.title}: ${m.essence_summary}`);
}

// Same shape as the movie version, against the TV silo: the join column
// is show_id rather than movie_id, and the table is
// tv_cluster_assignments. Ordered by distance so the sample is the
// cluster's most central shows -- the ones that best characterise what
// the grouping actually is.
async function sampleTvClusterShows(clusterId: string): Promise<string[]> {
  const { data, error } = await supabase
    .from('tv_cluster_assignments')
    .select('show:show_id(title, essence_summary)')
    .eq('cluster_id', clusterId)
    .order('distance', { ascending: true })
    .limit(SAMPLE_SIZE);
  if (error) throw error;
  return ((data ?? []) as any[])
    .map((row) => row.show)
    .filter((s) => s?.essence_summary)
    .map((s) => `- ${s.title}: ${s.essence_summary}`);
}

// Real production failure fixed here: this used to call supabase-js's
// .overlaps('keywords', keywords), which serializes the JS array into a
// raw Postgres array-literal string client-side ("{a,b,c}") without
// properly quoting elements containing special characters. A keyword
// group containing "rock 'n' roll" broke on the unescaped apostrophes:
// "malformed array literal... Incorrectly quoted array element." Using
// an RPC instead -- the array goes through as a real parameter, which
// Postgres handles correctly regardless of what's inside the strings.
async function sampleKeywordGroupMovies(groupId: string): Promise<string[]> {
  const { data: members, error: membersErr } = await supabase
    .from('keyword_group_members')
    .select('keyword')
    .eq('group_id', groupId);
  if (membersErr) throw membersErr;
  const keywords = (members ?? []).map((m: any) => m.keyword);
  if (keywords.length === 0) return [];

  const { data: movies, error: moviesErr } = await supabase.rpc('movies_by_keywords_sample', {
    p_keywords: keywords,
    p_limit: SAMPLE_SIZE,
  });
  if (moviesErr) throw moviesErr;
  return ((movies ?? []) as any[]).map((m) => `- ${m.title}: ${m.essence_summary}`);
}

async function generateDescription(
  label: string,
  sample: string[],
  media: 'movie' | 'tv',
): Promise<string | null> {
  if (sample.length < 3) return null; // too few real examples to ground anything in

  // The nouns are swapped rather than left generic. Asking for copy
  // about a "movie collection" while showing it a sample of series
  // invites language that does not fit television -- runtime, a single
  // viewing, a plot that resolves once -- when the things that
  // characterise a TV grouping are seasons, arcs and how a show sustains
  // or shifts over time.
  const noun = media === 'tv' ? 'TV' : 'movie';
  const plural = media === 'tv' ? 'shows' : 'movies';

  const response = await anthropic.messages.create({
    model: DESCRIPTION_MODEL,
    // Was 150 -- confirmed via a real production check that this was
    // systematically too tight: 422 of 445 live descriptions (94.8%)
    // ended up truncated mid-sentence, not an occasional edge case. The
    // prompt asks for specific, grounded detail rather than generic
    // filler, and a genuinely specific 2-3 sentence description
    // routinely runs past what 150 tokens covers. 300 gives real
    // headroom without inviting rambling past 2-3 sentences.
    max_tokens: 300,
    system:
      `You write a short intro (2-3 sentences) for a ${noun} collection page on a recommendation site. The collection was discovered by clustering real ${plural} -- its members share something real, not just a label. Ground what you write in the ACTUAL ${plural} shown below, not in the label alone. Be specific: name a real pattern in tone, subject, or feeling that genuinely recurs across these particular ${plural}. Do not restate the label as a sentence, do not use generic filler like "if you enjoy X, you'll love these," and do not use bullet points. Respond with only the description text: no preamble, no quotes, no label repeated verbatim at the start.`,
    messages: [{ role: 'user', content: `Collection label: ${label}\n\nSample ${plural}:\n${sample.join('\n')}` }],
  });

  const text = response.content.find((b) => b.type === 'text');
  return text && 'text' in text ? text.text.trim() : null;
}

async function sampleFor(target: CollectionTarget): Promise<string[]> {
  switch (target.table) {
    case 'movie_theme_clusters':
      return sampleTonalClusterMovies(target.id);
    case 'tv_theme_clusters':
      return sampleTvClusterShows(target.id);
    case 'keyword_groups':
      return sampleKeywordGroupMovies(target.id);
  }
}

async function main() {
  console.log(
    `Starting collection description generation${DRY_RUN ? ' (DRY RUN)' : ''}, media=${MEDIA}...`,
  );

  const wantMovie = MEDIA === 'all' || MEDIA === 'movie';
  const wantTv = MEDIA === 'all' || MEDIA === 'tv';

  const [tonalTargets, tvTargets, keywordTargets] = await Promise.all([
    wantMovie ? fetchTonalClustersNeedingDescription() : Promise.resolve([]),
    wantTv ? fetchTvClustersNeedingDescription() : Promise.resolve([]),
    // Keyword groups are built from movie keywords, so they belong to
    // the movie side and are skipped when only TV is requested.
    wantMovie ? fetchKeywordGroupsNeedingDescription() : Promise.resolve([]),
  ]);
  const targets = [...tonalTargets, ...tvTargets, ...keywordTargets];
  console.log(
    `${tonalTargets.length} movie clusters, ${tvTargets.length} TV clusters, ${keywordTargets.length} keyword groups need a description (${targets.length} total).`,
  );

  let written = 0;
  let skipped = 0;

  for (const target of targets) {
    const sample = await sampleFor(target);

    const description = await generateDescription(target.label, sample, target.media);
    if (!description) {
      console.log(`SKIP "${target.label}" -- fewer than 3 real sample titles with essence_summary.`);
      skipped++;
      continue;
    }

    console.log(`\n[${target.media}] "${target.label}"\n  -> ${description}`);

    if (!DRY_RUN) {
      const { error } = await supabase.from(target.table).update({ description }).eq('id', target.id);
      if (error) throw error;
    }
    written++;
  }

  console.log(
    `\n${DRY_RUN ? 'Dry run complete' : 'Done'}: ${written} descriptions ${DRY_RUN ? 'generated (not written)' : 'written'}, ${skipped} skipped.`,
  );
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
