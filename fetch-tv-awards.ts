// Major awards and nominations for the catalogue's series, from Wikipedia.
//
// Series have almost no critics' scores on record, and an audience rating
// alone lets fan favourites in: anime and children's cartoons rate as
// highly on IMDb as The Sopranos. What critics and the industry think of a
// series shows up instead in the big awards.
//
// Wikidata's award statements were tried first (9 Nov 2026) and are too
// patchy to rank by: Friends had 13 wins on record, Mad Men one, and
// Seinfeld, Frasier and The Wire none. So this reads the winners and
// nominees on Wikipedia's pages for the series awards themselves: the
// Primetime and International Emmys, the Golden Globes, the BAFTAs, the
// Critics' Choice and TCA awards, and the Peabodys.
//
// Each table row or list entry that links to a series counts once for it:
// as a win when the row is marked as the winner (the gold row, or bold
// type), as a nomination otherwise. A table laid out a year to a row, with
// a Winner column and a Nominees column (the TCA awards), is read by
// column. Every Peabody entry is a win: those pages list the winners alone.
// Sections that recount the same awards (most wins, by network) and the
// reference lists are skipped. Links are resolved to Wikidata items, items
// to IMDb ids -- a season or an episode to its series -- and IMDb ids to
// the catalogue.
//
// Upserts tv_awards for every scored series with an IMDb id: a series with
// no major awards gets a row of zeros, and detail holds the wins and
// nominations per awarding body. Rows this run did not write are then
// removed. What each page yielded is logged in pipeline_runs.error_message.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { pathToFileURL } from 'node:url';
import { createClient } from '@supabase/supabase-js';
import { API_STATUSES, GATEWAY_STATUSES, resilientFetch } from './resilient-fetch';

const WIKI_API = 'https://en.wikipedia.org/w/api.php';
const SPARQL = 'https://query.wikidata.org/sparql';
const USER_AGENT = 'CinnamonDB-pipeline/1.0 (https://www.cinnamondb.com)';

const TITLE_BATCH = 50;
const TITLE_URL_MAX = 5000;
const ITEM_BATCH = 150;
const PAUSE_MS = 300;
const TIMEOUT_MS = 60 * 1000;
const UPSERT_BATCH = 500;
const PAGE = 1000;
// Fewer series rows than this means the pages could not be read, not that
// the awards went away: nothing is written.
const MIN_MATCHED_ROWS = 200;

export interface AwardPage {
  title: string;
  /** Every entry on the page is an honouree (the Peabodys). */
  allWins?: boolean;
}

// The series awards. A title that has moved is followed through its
// redirect; two titles that land on one page are read once.
export const FIXED_PAGES: AwardPage[] = [
  { title: 'Primetime Emmy Award for Outstanding Drama Series' },
  { title: 'Primetime Emmy Award for Outstanding Comedy Series' },
  { title: 'Primetime Emmy Award for Outstanding Limited or Anthology Series' },
  { title: 'Golden Globe Award for Best Television Series – Drama' },
  { title: 'Golden Globe Award for Best Television Series – Musical or Comedy' },
  { title: 'Golden Globe Award for Best Limited or Anthology Series or Television Film' },
  { title: 'British Academy Television Award for Best Drama Series' },
  { title: 'British Academy Television Award for Best Mini-Series' },
  { title: 'British Academy Television Award for Best Drama Serial' },
  { title: 'British Academy Television Award for Best Scripted Comedy' },
  { title: 'British Academy Television Award for Best Comedy (Programme or Series)' },
  { title: 'British Academy Television Award for Best International Programme' },
  { title: 'International Emmy Award for Best Drama Series' },
  { title: 'International Emmy Award for Drama' },
  { title: 'International Emmy Award for Best Comedy Series' },
  { title: "Critics' Choice Television Award for Best Drama Series" },
  { title: "Critics' Choice Television Award for Best Comedy Series" },
  { title: "Critics' Choice Television Award for Best Limited Series" },
  { title: "Critics' Choice Television Award for Best Movie/Miniseries" },
  { title: "Critics' Choice Television Award for Best Foreign Language Series" },
  { title: 'TCA Award for Program of the Year' },
  { title: 'TCA Award for Outstanding Achievement in Drama' },
  { title: 'TCA Award for Outstanding Achievement in Comedy' },
  { title: 'TCA Award for Outstanding Achievement in Movies, Miniseries and Specials' },
  { title: 'TCA Award for Outstanding New Program' },
];

// The body behind each page, for the per-body detail.
const BODIES: [RegExp, string][] = [
  [/^Primetime Emmy/, 'emmy'],
  [/^International Emmy/, 'intl_emmy'],
  [/^Golden Globe/, 'globe'],
  [/^British Academy Television/, 'bafta'],
  [/^Critics' Choice/, 'critics_choice'],
  [/^TCA Award/, 'tca'],
  [/Peabody/, 'peabody'],
];

export function bodyOf(page: string): string {
  for (const [re, body] of BODIES) if (re.test(page)) return body;
  return 'other';
}

// Pages found by their title's start, one per decade.
export const PAGE_PREFIXES: { prefix: string; allWins: boolean }[] = [
  { prefix: 'List of Peabody Award winners (', allWins: true },
];

// ---------------------------------------------------------------------------
// Reading a page

export interface AwardRow {
  /** Pages the row links to, as linked. */
  targets: string[];
  won: boolean;
}

// Sections that recount awards listed elsewhere on the page, or that are
// not about the award at all.
const SKIP_SECTION =
  /superlative|record|multiple|\bmost\b|statistic|network|channel|broadcaster|see also|reference|\bnotes?\b|footnote|external link|further reading|bibliograph|\bsources\b|citation/i;
// How award tables mark the winner: the gold row, a double dagger, the
// won template.
const WINNER_MARK = /faeb86|b0c4de|‡|\{\{\s*won\b|\(winner\)/i;
// Or bold type on the title.
const BOLD_LINK = /'''+\s*\[\[/;
const NAMESPACE =
  /^(file|image|category|template|wikipedia|wp|help|portal|special|media|draft|user|wikt|wiktionary|commons|meta|mos)\s*:/i;
const HEADING = /^(={2,6})\s*(.+?)\s*\1$/;
// The ceremonies and the years in each row: never a series, and a
// ceremony's telecast must not collect every award handed out at it.
const NOT_A_SERIES = /\bawards?$|\bawards? ceremony$|^\d{4} in |^\d+(st|nd|rd|th) /i;

function decodeEntities(s: string): string {
  return s
    .replace(/&nbsp;|&#160;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#0?39;|&apos;/g, "'")
    .replace(/&ndash;/g, '–')
    .replace(/&mdash;/g, '—');
}

/** The pages a stretch of wikitext links to, in the API's title form. */
export function linkTargets(text: string): string[] {
  const out = new Set<string>();
  for (const m of text.matchAll(/\[\[([^[\]|]+?)(?:\|[^[\]]*)?\]\]/g)) {
    let t = m[1];
    if (t.includes('{')) continue;
    const hash = t.indexOf('#');
    if (hash === 0) continue;
    if (hash > 0) t = t.slice(0, hash);
    t = decodeEntities(t).replace(/_/g, ' ').replace(/\s+/g, ' ').trim();
    if (t.startsWith(':')) t = t.slice(1).trim();
    if (!t || NAMESPACE.test(t) || NOT_A_SERIES.test(t)) continue;
    out.add(t.charAt(0).toUpperCase() + t.slice(1));
  }
  return [...out];
}

// A cell's attributes ("style=... | content") are not its content; the
// pipes inside a link are.
function stripAttributes(cell: string): string {
  const m = /^([^[\]{}|]*=[^[\]{}|]*)\|(?!\|)([\s\S]*)$/.exec(cell);
  return (m ? m[2] : cell).trim();
}

/**
 * The cells of one table row, in order: "a || b" on one line or a cell to a
 * line, with lines that start with neither | nor ! (list entries, run-on
 * text) belonging to the cell before them.
 */
export function rowCells(rowText: string): string[] {
  const cells: string[] = [];
  for (const line of rowText.split('\n')) {
    if (line.startsWith('|-') || line.startsWith('|+') || line.startsWith('|}')) continue;
    if (line.startsWith('|') || line.startsWith('!')) {
      const sep = line.startsWith('!') ? /\s*(?:!!|\|\|)\s*/ : /\s*\|\|\s*/;
      for (const part of line.slice(1).split(sep)) cells.push(stripAttributes(part));
    } else if (cells.length > 0) {
      cells[cells.length - 1] += '\n' + line;
    }
  }
  return cells;
}

const WINNER_COLUMN = /\bwinners?\b|\brecipients?\b/i;
const NOMINEE_COLUMN = /nominee/i;

// A header row: every line a ! cell.
function headerCells(rowText: string): string[] | null {
  const lines = rowText.split('\n').filter((l) => !l.startsWith('|-'));
  if (lines.length === 0 || !lines.every((l) => l.startsWith('!'))) return null;
  return rowCells(rowText);
}

// A table with a Winner column and a Nominees column, a year to a row: the
// links in the one are wins, in the other nominations. A row under a
// rowspan has fewer cells; they line up from the right. Null when the
// table is not laid out that way.
function judgeByColumn(group: string[]): AwardRow[] | null {
  const at = group.findIndex((t) => headerCells(t) !== null);
  if (at < 0) return null;
  const header = headerCells(group[at])!;
  const w = header.findIndex((h) => WINNER_COLUMN.test(h) && !NOMINEE_COLUMN.test(h));
  const n = header.findIndex((h) => NOMINEE_COLUMN.test(h));
  if (w < 0 || n < 0) return null;
  const out: AwardRow[] = [];
  for (const t of group.slice(at + 1)) {
    const cells = rowCells(t);
    const shift = header.length - cells.length;
    const cell = (col: number) => (col - shift >= 0 ? cells[col - shift] : undefined);
    const winners = linkTargets(cell(w) ?? '');
    const nominees = linkTargets(cell(n) ?? '').filter((x) => !winners.includes(x));
    if (winners.length > 0) out.push({ targets: winners, won: true });
    if (nominees.length > 0) out.push({ targets: nominees, won: false });
  }
  return out;
}

// Which rows of one table (or one run of list entries) won. A table with
// a Winner and a Nominees column is read by column. Otherwise the marks
// are trusted in order: the gold row or dagger; bold type, unless nearly
// every row is bold and it marks nothing; and failing both, the first row
// of each year, which carries the year's rowspan and is the winner by
// convention.
function judge(group: string[], allWins: boolean): AwardRow[] {
  if (!allWins) {
    const byColumn = judgeByColumn(group);
    if (byColumn) return byColumn;
  }
  const rows = group.map((t) => ({ t, targets: linkTargets(t) })).filter((r) => r.targets.length > 0);
  if (rows.length === 0) return [];
  if (allWins) return rows.map((r) => ({ targets: r.targets, won: true }));
  const count = (xs: boolean[]) => xs.filter(Boolean).length;
  const marked = rows.map((r) => WINNER_MARK.test(r.t));
  const bold = rows.map((r) => BOLD_LINK.test(r.t));
  const spans = rows.map((r) => /rowspan/i.test(r.t));
  let won = rows.map(() => false);
  if (count(marked) > 0) won = marked;
  else if (count(bold) > 0 && count(bold) <= 0.6 * rows.length) won = bold;
  else if (count(spans) > 0 && count(spans) < rows.length) won = spans;
  return rows.map((r, i) => ({ targets: r.targets, won: won[i] }));
}

/**
 * The winner and nominee rows of an award page: one per table row or list
 * entry that links anywhere, outside the skipped sections.
 */
export function parseAwardPage(wikitext: string, allWins = false): AwardRow[] {
  const text = wikitext
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/<ref[^>]*\/>/gi, '')
    .replace(/<ref[^>]*>[\s\S]*?<\/ref>/gi, '');
  const out: AwardRow[] = [];
  let group: string[] = [];
  let row: string[] = [];
  let inTable = false;
  let skipLevel = 0;

  const endRow = () => {
    if (row.length) group.push(row.join('\n'));
    row = [];
  };
  const endGroup = () => {
    endRow();
    if (group.length) out.push(...judge(group, allWins));
    group = [];
  };

  for (const raw of text.split('\n')) {
    const line = raw.trim();
    const h = HEADING.exec(line);
    if (h) {
      endGroup();
      inTable = false;
      const level = h[1].length;
      if (skipLevel && level > skipLevel) continue;
      skipLevel = SKIP_SECTION.test(h[2]) ? level : 0;
      continue;
    }
    if (skipLevel) continue;
    if (line.startsWith('{|')) {
      endGroup();
      inTable = true;
      continue;
    }
    if (line.startsWith('|}')) {
      endGroup();
      inTable = false;
      continue;
    }
    if (line.startsWith('|-')) {
      // A table opened by a template has no {| of its own.
      if (inTable) endRow();
      else endGroup();
      inTable = true;
      row.push(line);
      continue;
    }
    if (line.startsWith('|+')) continue;
    if (inTable) {
      if (/^\{\{\s*(end|s-end|table end|award table end)\s*\}\}/i.test(line)) {
        endGroup();
        inTable = false;
        continue;
      }
      row.push(line);
      continue;
    }
    if (line.startsWith('|') || line.startsWith('!')) {
      endGroup();
      inTable = true;
      row.push(line);
      continue;
    }
    if (/^[*#]/.test(line)) {
      // One entry per row; a run of entries is judged together.
      endRow();
      row.push(line);
      endRow();
      continue;
    }
    // Prose, or a blank line, ends a run of entries.
    endGroup();
  }
  endGroup();
  return out;
}

// ---------------------------------------------------------------------------
// Counting

export interface Tally {
  wins: number;
  nominations: number;
  /** Per awarding body: wins and nominations. */
  detail: Record<string, { w: number; n: number }>;
}

export interface ParsedPage {
  page: string;
  rows: AwardRow[];
}

/**
 * Wins and nominations per catalogue series. A row counts once for each
 * series it links to, however many of its links lead there (the series and
 * its season, say).
 */
export function tallyPages(
  pages: ParsedPage[],
  itemOf: Map<string, string>,
  imdbOf: Map<string, Set<string>>,
  byImdb: Map<string, string[]>,
): { tallies: Map<string, Tally>; stats: { page: string; rows: number; winners: number; matched: number }[] } {
  const tallies = new Map<string, Tally>();
  const stats: { page: string; rows: number; winners: number; matched: number }[] = [];
  for (const p of pages) {
    let winners = 0;
    let matched = 0;
    for (const row of p.rows) {
      if (row.won) winners++;
      const ids = new Set<string>();
      for (const target of row.targets) {
        const item = itemOf.get(target);
        if (!item) continue;
        for (const tt of imdbOf.get(item) ?? []) for (const id of byImdb.get(tt) ?? []) ids.add(id);
      }
      if (ids.size > 0) matched++;
      const body = bodyOf(p.page);
      for (const id of ids) {
        const t = tallies.get(id) ?? { wins: 0, nominations: 0, detail: {} };
        const d = (t.detail[body] ??= { w: 0, n: 0 });
        if (row.won) {
          t.wins++;
          d.w++;
        } else {
          t.nominations++;
          d.n++;
        }
        tallies.set(id, t);
      }
    }
    stats.push({ page: p.page, rows: p.rows.length, winners, matched });
  }
  return { tallies, stats };
}

// ---------------------------------------------------------------------------
// Asking Wikipedia and Wikidata

type GetJson = (url: string) => Promise<any>;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function jsonGetter(label: string, accept: string): GetJson {
  const f = resilientFetch({ label, retryOn: API_STATUSES, attempts: 4 });
  return async (url: string) => {
    for (let attempt = 1; ; attempt++) {
      try {
        const res = await f(url, {
          headers: { Accept: accept, 'User-Agent': USER_AGENT },
          signal: AbortSignal.timeout(TIMEOUT_MS),
        });
        if (!res.ok) throw new Error(`answered ${res.status}`);
        return await res.json();
      } catch (err) {
        if (attempt >= 3) throw err;
        console.log(`  ${label}: ${(err as Error).message} -- asking again`);
        await sleep(5000 * attempt);
      }
    }
  };
}

const wikiUrl = (params: Record<string, string>) =>
  `${WIKI_API}?${new URLSearchParams({ action: 'query', format: 'json', formatversion: '2', ...params }).toString()}`;

/** Article titles that start with a prefix, redirects left out. */
export async function titlesWithPrefix(getJson: GetJson, prefix: string): Promise<string[]> {
  const out: string[] = [];
  let from: string | undefined;
  for (;;) {
    const params: Record<string, string> = {
      list: 'allpages',
      apprefix: prefix,
      apnamespace: '0',
      apfilterredir: 'nonredirects',
      aplimit: '500',
    };
    if (from) params.apcontinue = from;
    const json = await getJson(wikiUrl(params));
    for (const p of json.query?.allpages ?? []) if (typeof p.title === 'string') out.push(p.title);
    from = json.continue?.apcontinue;
    if (!from) break;
    await sleep(PAUSE_MS);
  }
  return out;
}

/** A page's wikitext and its title after redirects; null when missing. */
export async function pageText(getJson: GetJson, title: string): Promise<{ title: string; text: string } | null> {
  const json = await getJson(
    wikiUrl({ redirects: '1', prop: 'revisions', rvprop: 'content', rvslots: 'main', titles: title }),
  );
  const page = json.query?.pages?.[0];
  if (!page || page.missing || page.invalid) return null;
  const text = page.revisions?.[0]?.slots?.main?.content;
  return typeof text === 'string' ? { title: page.title, text } : null;
}

/** Wikidata item per linked title, following the API's normalising and redirects. */
export async function itemsForTitles(getJson: GetJson, titles: string[]): Promise<Map<string, string>> {
  const out = new Map<string, string>();
  let i = 0;
  while (i < titles.length) {
    const batch: string[] = [];
    let length = 0;
    while (i < titles.length && batch.length < TITLE_BATCH) {
      const size = encodeURIComponent(titles[i]).length + 3;
      if (batch.length > 0 && length + size > TITLE_URL_MAX) break;
      batch.push(titles[i]);
      length += size;
      i++;
    }
    const json = await getJson(
      wikiUrl({ redirects: '1', prop: 'pageprops', ppprop: 'wikibase_item', titles: batch.join('|') }),
    );
    const q = json.query ?? {};
    const step = new Map<string, string>();
    for (const n of q.normalized ?? []) step.set(n.from, n.to);
    const redirect = new Map<string, string>();
    for (const r of q.redirects ?? []) redirect.set(r.from, r.to);
    const item = new Map<string, string>();
    for (const p of q.pages ?? []) {
      const id = p.pageprops?.wikibase_item;
      if (typeof id === 'string' && /^Q\d+$/.test(id)) item.set(p.title, id);
    }
    for (const t of batch) {
      let x = step.get(t) ?? t;
      for (let hops = 0; hops < 3 && redirect.has(x); hops++) x = redirect.get(x)!;
      const id = item.get(x);
      if (id) out.set(t, id);
    }
    await sleep(PAUSE_MS);
  }
  return out;
}

export function directImdbQuery(items: string[]): string {
  return `SELECT ?item ?imdb WHERE {
  hint:Query hint:optimizer "None" .
  VALUES ?item { ${items.map((q) => `wd:${q}`).join(' ')} }
  ?item wdt:P345 ?imdb .
}`;
}

// A season or an episode carries its series' IMDb id on the series.
export function seriesImdbQuery(items: string[]): string {
  return `SELECT ?item ?imdb WHERE {
  hint:Query hint:optimizer "None" .
  VALUES ?item { ${items.map((q) => `wd:${q}`).join(' ')} }
  ?item wdt:P179|wdt:P361 ?series .
  ?series wdt:P345 ?imdb .
}`;
}

/** IMDb title ids per item: its own, and its series' for a season or episode. */
export async function imdbForItems(getJson: GetJson, items: string[]): Promise<Map<string, Set<string>>> {
  const out = new Map<string, Set<string>>();
  const add = (bindings: any[]) => {
    for (const b of bindings) {
      const item = /Q\d+$/.exec(b.item?.value ?? '')?.[0];
      const imdb = b.imdb?.value;
      if (!item || typeof imdb !== 'string' || !/^tt\d{5,10}$/.test(imdb)) continue;
      const set = out.get(item) ?? new Set<string>();
      set.add(imdb);
      out.set(item, set);
    }
  };
  for (let i = 0; i < items.length; i += ITEM_BATCH) {
    const batch = items.slice(i, i + ITEM_BATCH);
    for (const q of [directImdbQuery(batch), seriesImdbQuery(batch)]) {
      const json = await getJson(`${SPARQL}?format=json&query=${encodeURIComponent(q)}`);
      add(json.results?.bindings ?? []);
      await sleep(PAUSE_MS);
    }
  }
  return out;
}

const SHORT: [RegExp, string][] = [
  [/^Primetime Emmy Award for Outstanding /, 'Emmy '],
  [/^Golden Globe Award for Best (Television Series – )?/, 'Globe '],
  [/^British Academy Television Award for Best /, 'BAFTA '],
  [/^International Emmy Award for (Best )?/, 'Intl Emmy '],
  [/^Critics' Choice Television Award for Best /, 'Critics Choice '],
  [/^TCA Award for (Outstanding Achievement in )?/, 'TCA '],
  [/^List of Peabody Award winners /, 'Peabody '],
];

function shortName(page: string): string {
  for (const [re, to] of SHORT) if (re.test(page)) return page.replace(re, to);
  return page;
}

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_ROLE_KEY'), {
    global: { fetch: resilientFetch({ label: 'database', retryOn: GATEWAY_STATUSES }) },
  });
  const wiki = jsonGetter('Wikipedia', 'application/json');
  const wikidata = jsonGetter('Wikidata', 'application/sparql-results+json');
  const startedAt = new Date().toISOString();

  const byImdb = new Map<string, string[]>();
  const series: { id: string; imdb: string }[] = [];
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await supabase
      .from('tv_shows')
      .select('id, imdb_id')
      .eq('scoring_status', 'scored')
      .not('imdb_id', 'is', null)
      .order('id', { ascending: true })
      .range(from, from + PAGE - 1);
    if (error) throw new Error(`Reading series failed: ${error.message}`);
    const rows = (data ?? []) as { id: string; imdb_id: string }[];
    for (const r of rows) {
      if (!/^tt\d{5,10}$/.test(r.imdb_id)) continue;
      series.push({ id: r.id, imdb: r.imdb_id });
      const ids = byImdb.get(r.imdb_id) ?? [];
      ids.push(r.id);
      byImdb.set(r.imdb_id, ids);
    }
    if (rows.length < PAGE) break;
  }
  console.log(`${series.length} series in the catalogue with an IMDb id.`);

  const pages: AwardPage[] = [...FIXED_PAGES];
  for (const { prefix, allWins } of PAGE_PREFIXES) {
    const titles = await titlesWithPrefix(wiki, prefix);
    console.log(`${titles.length} pages start "${prefix}".`);
    for (const title of titles) pages.push({ title, allWins });
    await sleep(PAUSE_MS);
  }

  const read = new Set<string>();
  const missing: string[] = [];
  const parsed: ParsedPage[] = [];
  for (const p of pages) {
    const got = await pageText(wiki, p.title);
    await sleep(PAUSE_MS);
    if (!got) {
      missing.push(p.title);
      console.log(`  Missing: ${p.title}`);
      continue;
    }
    if (read.has(got.title)) continue;
    read.add(got.title);
    const rows = parseAwardPage(got.text, p.allWins);
    parsed.push({ page: got.title, rows });
    console.log(`  ${got.title}: ${rows.length} rows, ${rows.filter((r) => r.won).length} winners.`);
  }

  const targets = [...new Set(parsed.flatMap((p) => p.rows.flatMap((r) => r.targets)))];
  console.log(`${parsed.length} pages read; ${targets.length} linked pages to look up.`);
  const itemOf = await itemsForTitles(wiki, targets);
  const items = [...new Set(itemOf.values())];
  console.log(`${items.length} Wikidata items; asking for their IMDb ids.`);
  const imdbOf = await imdbForItems(wikidata, items);

  const { tallies, stats } = tallyPages(parsed, itemOf, imdbOf, byImdb);
  const matchedRows = stats.reduce((n, s) => n + s.matched, 0);
  const report =
    `${parsed.length} pages, ${missing.length} missing` +
    (missing.length ? ` (${missing.map(shortName).join('; ')})` : '') +
    `; ${tallies.size} series with awards; ${matchedRows} series rows. Per page rows/winners/series rows: ` +
    stats.map((s) => `${shortName(s.page)} ${s.rows}/${s.winners}/${s.matched}`).join('; ');
  console.log(report);

  if (matchedRows < MIN_MATCHED_ROWS) {
    await supabase.from('pipeline_runs').insert({
      run_type: 'tv_awards_refresh',
      started_at: startedAt,
      finished_at: new Date().toISOString(),
      rows_processed: 0,
      rows_failed: 0,
      status: 'failed',
      error_message: `Too few series rows matched; nothing written. ${report}`.slice(0, 8000),
    });
    console.error('Too few series rows matched; nothing written.');
    process.exit(1);
  }

  const top = [...tallies.entries()]
    .sort((a, b) => 2 * b[1].wins + b[1].nominations - (2 * a[1].wins + a[1].nominations))
    .slice(0, 25);
  console.log('Most recognised: ' + top.map(([id, t]) => `${id} ${t.wins}w/${t.nominations}n`).join(', '));

  const stamp = new Date().toISOString();
  const rows = series.map((s) => {
    const t = tallies.get(s.id) ?? { wins: 0, nominations: 0, detail: {} };
    return {
      tv_id: s.id,
      imdb_id: s.imdb,
      wins: t.wins,
      nominations: t.nominations,
      detail: t.detail,
      fetched_at: stamp,
    };
  });

  let written = 0;
  let failedRows = 0;
  for (let i = 0; i < rows.length; i += UPSERT_BATCH) {
    const chunk = rows.slice(i, i + UPSERT_BATCH);
    const { error } = await supabase.from('tv_awards').upsert(chunk, { onConflict: 'tv_id' });
    if (error) {
      failedRows += chunk.length;
      console.error(`Writing rows ${i}-${i + chunk.length} failed: ${error.message}`);
    } else {
      written += chunk.length;
    }
  }

  // What an earlier run wrote and this one did not (a series no longer
  // scored, or the Wikidata counts this replaces) goes, once every row has
  // landed.
  if (failedRows === 0) {
    const { error } = await supabase.from('tv_awards').delete().lt('fetched_at', stamp);
    if (error) console.error(`Removing older rows failed (non-fatal): ${error.message}`);
  }

  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'tv_awards_refresh',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: written,
    rows_failed: failedRows,
    status: written === 0 ? 'failed' : 'success',
    error_message: report.slice(0, 8000),
  });
  if (logError) console.error(`pipeline_runs logging failed (non-fatal): ${logError.message}`);

  console.log(`Done. ${tallies.size} series with major awards; ${written} rows written, ${failedRows} failed.`);
  if (written === 0) process.exit(1);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}
