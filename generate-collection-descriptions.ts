// Generates 2-3 sentence intro copy for collection pages (tonal
// clusters + keyword groups) -- grounded in real member movies, same
// sampling approach already validated for labeling (cluster-movies.ts /
// cluster-keywords.ts), extended to also write a description. A new,
// separate incremental script rather than folding into the full
// re-cluster pipelines: the clusters/groups already exist and don't
// need to be redone, just enriched with one new field. Re-running a
// full k-means clustering just to add copy would be the wrong tool for
// the job.
//
// DRY_RUN defaults true on purpose -- this writes new public-facing
// copy across ~450 pages, and generic/templated output here would be
// worse than no copy at all (the exact failure mode "helpful content"
// search systems are built to detect). Review the dry-run output before
// running for real.
//
// Required env vars: ANTHROPIC_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=false to actually write (defaults to true), LIMIT=<int> to cap how many are processed

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const ANTHROPIC_API_KEY = requireEnv('ANTHROPIC_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const DRY_RUN = process.env.DRY_RUN !== 'false'; // defaults to true -- must explicitly opt out
const LIMIT = process.env.LIMIT ? parseInt(process.env.LIMIT, 10) : undefined;
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
  table: 'movie_theme_clusters' | 'keyword_groups';
  id: string;
  label: string;
}

async function fetchTonalClustersNeedingDescription(): Promise<CollectionTarget[]> {
  let query = supabase.from('movie_theme_clusters').select('id, label').is('description', null);
  if (LIMIT) query = query.limit(LIMIT);
  const { data, error } = await query;
  if (error) throw error;
  return (data ?? []).map((c: any) => ({ table: 'movie_theme_clusters' as const, id: c.id, label: c.label }));
}

async function fetchKeywordGroupsNeedingDescription(): Promise<CollectionTarget[]> {
  let query = supabase.from('keyword_groups').select('id, label').is('description', null);
  if (LIMIT) query = query.limit(LIMIT);
  const { data, error } = await query;
  if (error) throw error;
  return (data ?? []).map((g: any) => ({ table: 'keyword_groups' as const, id: g.id, label: g.label }));
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

async function generateDescription(label: string, sample: string[]): Promise<string | null> {
  if (sample.length < 3) return null; // too few real examples to ground anything in

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
      "You write a short intro (2-3 sentences) for a movie collection page on a recommendation site. The collection was discovered by clustering real movies -- its members share something real, not just a label. Ground what you write in the ACTUAL movies shown below, not in the label alone. Be specific: name a real pattern in tone, subject, or feeling that genuinely recurs across these particular movies. Do not restate the label as a sentence, do not use generic filler like \"if you enjoy X, you'll love these,\" and do not use bullet points. Respond with only the description text: no preamble, no quotes, no label repeated verbatim at the start.",
    messages: [{ role: 'user', content: `Collection label: ${label}\n\nSample movies:\n${sample.join('\n')}` }],
  });

  const text = response.content.find((b) => b.type === 'text');
  return text && 'text' in text ? text.text.trim() : null;
}

async function main() {
  console.log(`Starting collection description generation${DRY_RUN ? ' (DRY RUN)' : ''}...`);

  const [tonalTargets, keywordTargets] = await Promise.all([
    fetchTonalClustersNeedingDescription(),
    fetchKeywordGroupsNeedingDescription(),
  ]);
  const targets = [...tonalTargets, ...keywordTargets];
  console.log(`${tonalTargets.length} tonal clusters, ${keywordTargets.length} keyword groups need a description (${targets.length} total).`);

  let written = 0;
  let skipped = 0;

  for (const target of targets) {
    const sample =
      target.table === 'movie_theme_clusters'
        ? await sampleTonalClusterMovies(target.id)
        : await sampleKeywordGroupMovies(target.id);

    const description = await generateDescription(target.label, sample);
    if (!description) {
      console.log(`SKIP "${target.label}" -- fewer than 3 real sample movies with essence_summary.`);
      skipped++;
      continue;
    }

    console.log(`\n"${target.label}"\n  -> ${description}`);

    if (!DRY_RUN) {
      const { error } = await supabase.from(target.table).update({ description }).eq('id', target.id);
      if (error) throw error;
    }
    written++;
  }

  console.log(`\n${DRY_RUN ? 'Dry run complete' : 'Done'}: ${written} descriptions ${DRY_RUN ? 'generated (not written)' : 'written'}, ${skipped} skipped.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
