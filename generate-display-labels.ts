// Generates a simpler, frontend-facing display_label alongside the
// existing sophisticated label -- the clustering, centroids, slugs, and
// original label are all untouched. Real user feedback: labels like
// "Paranoid Sci-Fi Conspiracies" and "Destiny-Bound Fellowships Against
// Epic Threats" are too literary for a visitor to parse at a glance,
// even though that same richness is exactly what makes the description-
// generation prompt work well. Rather than dumbing down the original
// (and losing that grounding value), this adds a second, simpler name
// and leaves the original in place for internal use.
//
// Slugs are NOT regenerated from display_label -- they're already live
// in the sitemap submitted to Search Console; remapping them would
// break real indexed URLs for zero benefit.
//
// DRY_RUN defaults true, same discipline as generate-collection-
// descriptions.ts -- this is new copy across ~450 pages' primary
// heading, worth reviewing a sample before it's live everywhere.
//
// Required env vars: ANTHROPIC_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=false to actually write (defaults true), LIMIT=<int>

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const ANTHROPIC_API_KEY = requireEnv('ANTHROPIC_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const DRY_RUN = process.env.DRY_RUN !== 'false';
const LIMIT = process.env.LIMIT ? parseInt(process.env.LIMIT, 10) : undefined;
const LABEL_MODEL = 'claude-sonnet-5';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

interface Target {
  table: 'movie_theme_clusters' | 'keyword_groups';
  id: string;
  label: string;
}

async function fetchTargets(table: 'movie_theme_clusters' | 'keyword_groups'): Promise<Target[]> {
  let query = supabase.from(table).select('id, label').is('display_label', null);
  if (LIMIT) query = query.limit(LIMIT);
  const { data, error } = await query;
  if (error) throw error;
  return (data ?? []).map((r: any) => ({ table, id: r.id, label: r.label }));
}

async function simplifyLabel(label: string): Promise<string | null> {
  const response = await anthropic.messages.create({
    model: LABEL_MODEL,
    max_tokens: 20,
    system:
      "You simplify movie-collection category labels for a general audience browsing a recommendation site. The label you're given is deliberately literary/evocative -- that's intentional and used elsewhere, not a mistake. Your job is a DIFFERENT, simpler version for people scanning quickly: swap poetic or unusual phrasing for concrete, familiar words, but keep it JUST AS SPECIFIC -- never genericize into a broad bucket like \"Action Movies\" or \"Drama.\" If the original is already simple and direct, output it unchanged rather than forcing a change. 2-4 words, title case. Respond with only the simplified label: no quotes, no explanation, no punctuation at the end.",
    messages: [{ role: 'user', content: label }],
  });
  const text = response.content.find((b) => b.type === 'text');
  return text && 'text' in text ? text.text.trim() : null;
}

async function main() {
  console.log(`Starting display label generation${DRY_RUN ? ' (DRY RUN)' : ''}...`);

  const [tonalTargets, keywordTargets] = await Promise.all([
    fetchTargets('movie_theme_clusters'),
    fetchTargets('keyword_groups'),
  ]);
  const targets = [...tonalTargets, ...keywordTargets];
  console.log(`${tonalTargets.length} tonal clusters, ${keywordTargets.length} keyword groups need a display label (${targets.length} total).`);

  let written = 0;

  for (const target of targets) {
    const displayLabel = await simplifyLabel(target.label);
    if (!displayLabel) {
      console.log(`SKIP "${target.label}" -- no response.`);
      continue;
    }

    const changed = displayLabel.toLowerCase() !== target.label.toLowerCase();
    console.log(`"${target.label}" -> "${displayLabel}"${changed ? '' : '  (unchanged, already simple)'}`);

    if (!DRY_RUN) {
      const { error } = await supabase.from(target.table).update({ display_label: displayLabel }).eq('id', target.id);
      if (error) throw error;
    }
    written++;
  }

  console.log(`\n${DRY_RUN ? 'Dry run complete' : 'Done'}: ${written} display labels ${DRY_RUN ? 'generated (not written)' : 'written'}.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
