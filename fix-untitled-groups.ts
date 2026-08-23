// Fixes "Untitled Group" -- the fallback label written when the
// original cluster-keywords.ts labeling call failed for that group.
// Confirmed via direct inspection: most of the 6 affected groups are
// genuinely coherent (one is Middle East/ancient-world cities --
// Istanbul, Cairo, Jerusalem, Mecca, Damascus; another is British Isles
// history -- Irish history, House of Stuart, British royal family) and
// just never got a real label. One (678 keywords) is genuine k-means
// noise with no real theme (mole, password, hashtag, matrix, cube --
// no discernible pattern). The prompt gives the model an honest way to
// say so rather than forcing a fake label onto incoherent noise --
// groups that come back NO_COHERENT_THEME get deleted (their member
// keywords become ungrouped rather than showing a meaningless chip on
// movie pages, which is what's happening right now).
//
// DRY_RUN defaults true, same discipline as every other content-writing
// script in this pipeline.
//
// Required env vars: ANTHROPIC_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=false to actually write (defaults to true)

import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const ANTHROPIC_API_KEY = requireEnv('ANTHROPIC_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');

const DRY_RUN = process.env.DRY_RUN !== 'false';
const SAMPLE_SIZE = 40; // larger than the usual 15-20 -- these groups are bigger and a small sample risks missing the real pattern
const LABEL_MODEL = 'claude-sonnet-5';
const NO_THEME_SENTINEL = 'NO_COHERENT_THEME';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

async function fetchUntitledGroups() {
  const { data, error } = await supabase
    .from('keyword_groups')
    .select('id, keyword_count')
    .eq('label', 'Untitled Group');
  if (error) throw error;
  return data ?? [];
}

async function sampleKeywords(groupId: string): Promise<string[]> {
  const { data, error } = await supabase
    .from('keyword_group_members')
    .select('keyword')
    .eq('group_id', groupId)
    .limit(SAMPLE_SIZE);
  if (error) throw error;
  return (data ?? []).map((m: any) => m.keyword);
}

async function attemptLabel(keywords: string[]): Promise<{ label: string; simpleLabel: string } | null> {
  const response = await anthropic.messages.create({
    model: LABEL_MODEL,
    max_tokens: 100,
    system:
      `You label a cluster of movie keywords for a recommendation site. You're given a sample of keywords that were grouped together by embedding similarity. Sometimes the group is genuinely coherent (a real theme connects the keywords); sometimes it's k-means noise -- rare or hard-to-place keywords that got dumped together with no real connection. Be honest about which this is.\n\nIf coherent: respond with two lines, no labels or preamble --\nLine 1: an evocative 2-4 word label (matching a literary, distinctive style)\nLine 2: a plain-English 2-4 word version of the same label, immediately clear to anyone\n\nIf NOT coherent (the keywords don't share a real theme): respond with exactly "${NO_THEME_SENTINEL}" and nothing else.`,
    messages: [{ role: 'user', content: `Keywords:\n${keywords.join(', ')}` }],
  });

  const text = response.content.find((b) => b.type === 'text');
  const raw = text && 'text' in text ? text.text.trim() : '';

  if (raw === NO_THEME_SENTINEL || raw.length === 0) return null;

  const lines = raw.split('\n').map((l) => l.trim()).filter(Boolean);
  if (lines.length < 2) return null;
  return { label: lines[0], simpleLabel: lines[1] };
}

async function main() {
  console.log(`Starting Untitled Group cleanup${DRY_RUN ? ' (DRY RUN)' : ''}...`);

  const groups = await fetchUntitledGroups();
  console.log(`${groups.length} "Untitled Group" instances found.`);

  let relabeled = 0;
  let deleted = 0;

  for (const group of groups) {
    const keywords = await sampleKeywords(group.id);
    const result = await attemptLabel(keywords);

    if (result) {
      console.log(`RELABEL (${group.keyword_count} keywords) -> "${result.label}" / "${result.simpleLabel}"`);
      if (!DRY_RUN) {
        const { error } = await supabase
          .from('keyword_groups')
          .update({ label: result.label, display_label: result.simpleLabel })
          .eq('id', group.id);
        if (error) throw error;
      }
      relabeled++;
    } else {
      console.log(`DELETE (${group.keyword_count} keywords, no coherent theme) -- sample: ${keywords.slice(0, 10).join(', ')}`);
      if (!DRY_RUN) {
        const { error: memberErr } = await supabase.from('keyword_group_members').delete().eq('group_id', group.id);
        if (memberErr) throw memberErr;
        const { error: groupErr } = await supabase.from('keyword_groups').delete().eq('id', group.id);
        if (groupErr) throw groupErr;
      }
      deleted++;
    }
  }

  console.log(`\n${DRY_RUN ? 'Dry run complete' : 'Done'}: ${relabeled} relabeled, ${deleted} ${DRY_RUN ? 'would be deleted' : 'deleted'} (no coherent theme).`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
