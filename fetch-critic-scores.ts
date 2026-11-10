// Critics' scores for the catalogue's films and series, from Wikidata and,
// for films Wikidata has nothing on, from the film's English Wikipedia
// article.
//
// The Cinnamon score's only measure of how good a title is used to be its
// TMDB user rating, and TMDB's users rate like fans: Avengers: Infinity War
// (8.2) and Sound of Freedom (8.0) outrated The Lighthouse (7.5), and the
// Cinnamon Hundred filled up with crowd favourites. The television Hundred
// had the same problem, with Ghost Whisperer, Gossip Girl and Redo of
// Healer in it. Critics are the other half of "is it brilliant", and
// Wikidata carries their scores openly (CC0): Metacritic's Metascore, and
// Rotten Tomatoes' Tomatometer and critics' average rating, as
// review-score statements (P444) qualified by who gave them (P447).
// Titles are matched on their IMDb id (P345).
//
// Only critics' scores are kept. A statement whose determination method
// (P459) names an audience or user score is skipped, as are deprecated
// statements and any value whose shape does not fit its source (Metascore
// "NN/100", Tomatometer "NN%", average "N.N/10"). Where a source has
// several values over time, the latest by point in time (P585) wins.
//
// Series are scored season by season on both sites, and Wikidata keeps
// them that way: on the season's own item (a television series season,
// part of the series by P179), or on the series qualified by the season
// (P4908). A series' score is the mean of its seasons', the way both sites
// sum a series up; a value for the series as a whole counts only when no
// season has one.
//
// Wikidata has scores mostly for well-known films: on 10 November 2026 two
// films in three had none -- 98% of films with 2,000+ votes were covered,
// 57% of those with 100-500 and none under 50, which were never looked
// up. Every scored film is now looked up, and a film Wikidata has nothing
// on is read from its English Wikipedia article (the wikipediaPass below),
// which usually quotes both sites in its reception section.
//
// Upserts critic_scores by movie_id and tv_critic_scores by tv_id, with
// critic_scores.source saying where a film's scores came from. A title no
// source has anything on gets no row; a title whose scores vanish keeps
// its last ones, and a film with Wikidata scores is never overwritten from
// Wikipedia. Series run first: they are the shorter pass, and a failure in
// one pass does not stop the others.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import { pathToFileURL } from 'node:url';
import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import { API_STATUSES, GATEWAY_STATUSES, resilientFetch } from './resilient-fetch';

// Films: every scored film, however few votes it has. Below 40 votes a
// series almost never has a Metascore or a Tomatometer, and the series
// score does not use them.
const VOTE_FLOOR: number | null = null;
const TV_VOTE_FLOOR = 40;
// IMDb ids per query. Sent as a GET (so a failed one can safely be sent
// again), and 150 keeps the address well inside what the service accepts.
// Series ask for their seasons too, so fewer per query.
const BATCH = 150;
const TV_BATCH = 50;
// The query service asks clients to go gently: one query at a time, with
// a pause between.
const PAUSE_MS = 1000;
// The series pass gives up at this point and writes what it has, so the
// film passes always get their turn inside the job's time limit.
const TV_TIME_LIMIT_MS = 20 * 60 * 1000;
// The Wikipedia pass stops reading at this point and writes what it has.
const WIKI_TIME_LIMIT_MS = 55 * 60 * 1000;
// The service stops a query at 60 seconds; this leaves room for one more
// try, and stops a request that never answers from holding up the run.
const ASK_TIMEOUT_MS = 130 * 1000;
const UPSERT_BATCH = 500;
const PAGE = 1000;

const ENDPOINT = 'https://query.wikidata.org/sparql';
const WIKI_API = 'https://en.wikipedia.org/w/api.php';
// Wikimedia asks every client to say who it is.
const USER_AGENT = 'CinnamonDB-pipeline/1.0 (https://www.cinnamondb.com)';
// Articles per MediaWiki request (the API's limit), and the most the
// titles may add to the address.
const WIKI_BATCH = 50;
const WIKI_URL_MAX = 5000;
const WIKI_PAUSE_MS = 300;
const WIKI_TIMEOUT_MS = 90 * 1000;
// Films with Wikidata scores read from Wikipedia as well each run, to
// report how often the two sources agree.
const CHECK_SAMPLE = 300;

export const METACRITIC = 'Q150248';
export const ROTTEN_TOMATOES = 'Q105584';
// "television series season"
export const SEASON = 'Q3464665';

// Audience and user scores are what TMDB already gives us; only critics'
// scores belong here.
const AUDIENCE = /audience|user|popcorn|verified|fan/i;

export interface Binding {
  imdb?: { value: string };
  item?: { value: string };
  value?: { value: string };
  by?: { value: string };
  methodLabel?: { value: string };
  date?: { value: string };
}

export interface TvBinding extends Binding {
  // The statement, so one that comes back twice is counted once.
  st?: { value: string };
  // The season a value belongs to; absent for the series as a whole.
  part?: { value: string };
}

interface Pick {
  value: number;
  // ISO date of the value, or '' when the statement has none.
  date: string;
}

type Kind = 'metacritic' | 'tomatometer' | 'average';

export interface Found {
  wikidata: string | null;
  metacritic: Pick | null;
  tomatometer: Pick | null;
  average: Pick | null;
}

export interface TvFound {
  wikidata: string | null;
  // How many seasons the most complete of the three scores was averaged
  // over; 0 when it is a value for the series as a whole.
  seasons: number;
  metacritic: number | null;
  tomatometer: number | null;
  average: number | null;
}

// The latest dated value wins; an undated one only fills a gap.
function better(current: Pick | null, next: Pick): Pick {
  if (!current) return next;
  if (next.date && (!current.date || next.date > current.date)) return next;
  return current;
}

function lastSegment(uri: string): string {
  return uri.slice(uri.lastIndexOf('/') + 1);
}

// Which of the three scores a statement is, and its value; null when it is
// none of them or its value does not fit its source.
export function reading(by: string, raw: string): { kind: Kind; value: number } | null {
  if (by === METACRITIC) {
    const m = raw.match(/^(\d{1,3})(?:\s*\/\s*100)?$/);
    const v = m ? Number(m[1]) : NaN;
    return v >= 0 && v <= 100 ? { kind: 'metacritic', value: v } : null;
  }
  if (by === ROTTEN_TOMATOES) {
    const pct = raw.match(/^(\d{1,3})\s*%$/);
    if (pct) {
      const v = Number(pct[1]);
      return v >= 0 && v <= 100 ? { kind: 'tomatometer', value: v } : null;
    }
    const avg = raw.match(/^(\d{1,2}(?:\.\d+)?)\s*\/\s*10$/);
    if (avg) {
      const v = Math.round(Number(avg[1]) * 10) / 10;
      return v >= 0 && v <= 10 ? { kind: 'average', value: v } : null;
    }
  }
  return null;
}

export function parse(bindings: Binding[]): Map<string, Found> {
  const out = new Map<string, Found>();
  for (const b of bindings) {
    const imdb = b.imdb?.value;
    const raw = b.value?.value?.trim();
    const by = b.by ? lastSegment(b.by.value) : '';
    if (!imdb || !raw || !by) continue;
    if (AUDIENCE.test(b.methodLabel?.value ?? '')) continue;

    const found: Found = out.get(imdb) ?? { wikidata: null, metacritic: null, tomatometer: null, average: null };
    if (!found.wikidata && b.item) found.wikidata = lastSegment(b.item.value);
    const r = reading(by, raw);
    if (r) found[r.kind] = better(found[r.kind], { value: r.value, date: b.date?.value ?? '' });
    out.set(imdb, found);
  }
  return out;
}

const round1 = (x: number) => Math.round(x * 10) / 10;

// The mean of a series' seasons, or its whole-series value when no season
// has one.
function seriesValue(byPart: Map<string, Pick>): { value: number | null; seasons: number } {
  const seasons = [...byPart.entries()].filter(([part]) => part !== '').map(([, p]) => p.value);
  if (seasons.length) return { value: round1(seasons.reduce((a, b) => a + b, 0) / seasons.length), seasons: seasons.length };
  const whole = byPart.get('');
  return { value: whole ? whole.value : null, seasons: 0 };
}

export function parseTv(bindings: TvBinding[]): Map<string, TvFound> {
  const acc = new Map<string, { wikidata: string | null; seen: Set<string>; parts: Record<Kind, Map<string, Pick>> }>();
  for (const b of bindings) {
    const imdb = b.imdb?.value;
    const raw = b.value?.value?.trim();
    const by = b.by ? lastSegment(b.by.value) : '';
    if (!imdb || !raw || !by) continue;
    if (AUDIENCE.test(b.methodLabel?.value ?? '')) continue;

    let entry = acc.get(imdb);
    if (!entry) {
      entry = { wikidata: null, seen: new Set(), parts: { metacritic: new Map(), tomatometer: new Map(), average: new Map() } };
      acc.set(imdb, entry);
    }
    if (!entry.wikidata && b.item) entry.wikidata = lastSegment(b.item.value);

    const r = reading(by, raw);
    if (!r) continue;
    // A statement qualified by both a season and a part comes back once
    // for each; it counts once.
    const st = b.st?.value;
    if (st) {
      if (entry.seen.has(st)) continue;
      entry.seen.add(st);
    }
    const part = b.part ? lastSegment(b.part.value) : '';
    const byPart = entry.parts[r.kind];
    byPart.set(part, better(byPart.get(part) ?? null, { value: r.value, date: b.date?.value ?? '' }));
  }

  const out = new Map<string, TvFound>();
  for (const [imdb, e] of acc) {
    const mc = seriesValue(e.parts.metacritic);
    const tm = seriesValue(e.parts.tomatometer);
    const avg = seriesValue(e.parts.average);
    out.set(imdb, {
      wikidata: e.wikidata,
      seasons: Math.max(mc.seasons, tm.seasons, avg.seasons),
      metacritic: mc.value,
      tomatometer: tm.value,
      average: avg.value,
    });
  }
  return out;
}

export function sparql(imdbIds: string[]): string {
  const values = imdbIds.map((id) => `"${id}"`).join(' ');
  return `SELECT ?imdb ?item ?value ?by ?methodLabel ?date WHERE {
  VALUES ?imdb { ${values} }
  VALUES ?by { wd:${METACRITIC} wd:${ROTTEN_TOMATOES} }
  ?item wdt:P345 ?imdb ;
        p:P444 ?st .
  ?st ps:P444 ?value ;
      pq:P447 ?by ;
      wikibase:rank ?rank .
  FILTER(?rank != wikibase:DeprecatedRank)
  OPTIONAL { ?st pq:P459 ?method . ?method rdfs:label ?methodLabel . FILTER(LANG(?methodLabel) = "en") }
  OPTIONAL { ?st pq:P585 ?date . }
}`;
}

// Two questions per batch, each anchored on the series the way the film
// query is: its own review scores, then its seasons'. Asked as one query
// with a UNION, the first series pass had not written anything after half
// an hour: with the series' statements inside the UNION, the query service
// is free to gather every Metacritic and Rotten Tomatoes statement on
// Wikidata before it knows which series it wants. The written order is
// kept (hint:optimizer "None") so each question starts from the IMDb ids.
export function sparqlTvSeries(imdbIds: string[]): string {
  const values = imdbIds.map((id) => `"${id}"`).join(' ');
  return `SELECT ?imdb ?item ?part ?st ?value ?by ?methodLabel ?date WHERE {
  hint:Query hint:optimizer "None" .
  VALUES ?imdb { ${values} }
  ?item wdt:P345 ?imdb .
  ?item p:P444 ?st .
  ?st pq:P447 ?by .
  FILTER(?by IN (wd:${METACRITIC}, wd:${ROTTEN_TOMATOES}))
  ?st ps:P444 ?value ;
      wikibase:rank ?rank .
  FILTER(?rank != wikibase:DeprecatedRank)
  OPTIONAL { ?st pq:P4908|pq:P518 ?part . }
  OPTIONAL { ?st pq:P459 ?method . ?method rdfs:label ?methodLabel . FILTER(LANG(?methodLabel) = "en") }
  OPTIONAL { ?st pq:P585 ?date . }
}`;
}

export function sparqlTvSeasons(imdbIds: string[]): string {
  const values = imdbIds.map((id) => `"${id}"`).join(' ');
  return `SELECT ?imdb ?item ?part ?st ?value ?by ?methodLabel ?date WHERE {
  hint:Query hint:optimizer "None" .
  VALUES ?imdb { ${values} }
  ?item wdt:P345 ?imdb .
  ?part wdt:P179 ?item .
  ?part wdt:P31 wd:${SEASON} .
  ?part p:P444 ?st .
  ?st pq:P447 ?by .
  FILTER(?by IN (wd:${METACRITIC}, wd:${ROTTEN_TOMATOES}))
  ?st ps:P444 ?value ;
      wikibase:rank ?rank .
  FILTER(?rank != wikibase:DeprecatedRank)
  OPTIONAL { ?st pq:P459 ?method . ?method rdfs:label ?methodLabel . FILTER(LANG(?methodLabel) = "en") }
  OPTIONAL { ?st pq:P585 ?date . }
}`;
}

// The English Wikipedia article of each title, through Wikidata's
// sitelinks.
export function sparqlSitelinks(imdbIds: string[]): string {
  const values = imdbIds.map((id) => `"${id}"`).join(' ');
  return `SELECT ?imdb ?item ?title WHERE {
  VALUES ?imdb { ${values} }
  ?item wdt:P345 ?imdb .
  ?article schema:about ?item ;
           schema:isPartOf <https://en.wikipedia.org/> ;
           schema:name ?title .
}`;
}

// ---------------------------------------------------------------------------
// Reading a film's scores off its Wikipedia article
//
// The reception section of a film article quotes Rotten Tomatoes and
// Metacritic in a handful of set phrasings -- "On the review aggregator
// website Rotten Tomatoes, 85% of 120 critics' reviews are positive, with
// an average rating of 7.2/10"; "Metacritic, which uses a weighted
// average, assigned the film a score of 74 out of 100, based on 40
// critics" -- written out or through {{Rotten Tomatoes prose|85|7.2|120}}
// and {{Metacritic film prose|74|40}}. The first figures of each kind in
// the reception section are taken; failing that, the first in the rest of
// the article, leaving out sections about sequels, remakes and the like,
// which quote other films' scores. {{Rotten Tomatoes data}} reads its
// figures from Wikidata, which the first pass has already asked.

export interface Reception {
  metacritic: number | null;
  tomatometer: number | null;
  average: number | null;
}

const RECEPTION = /\b(critical\s+(response|reception|reaction|reviews?)|reception|reviews)\b/i;
const OFF_TOPIC =
  /\b(sequels?|prequels?|remakes?|reboots?|follow[- ]?ups?|spin[- ]?offs?|franchise|other versions?|adaptations?|legacy|series|soundtrack|home media|video games?)\b/i;

// Comments and references carry numbers of their own (dates, page
// counts), so they go first.
export function stripNoise(text: string): string {
  return text
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/<ref\b[^>]*\/\s*>/gi, '')
    .replace(/<ref\b[^>]*>[\s\S]*?<\/ref\s*>/gi, '');
}

export interface Template {
  name: string;
  params: string[];
  named: Map<string, string>;
}

// Splits a template's inside on the bars that are not inside a nested
// template or link.
function splitTop(s: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let cur = '';
  for (let k = 0; k < s.length; k++) {
    const two = s.slice(k, k + 2);
    if (two === '{{' || two === '[[') {
      depth++;
      cur += two;
      k++;
      continue;
    }
    if ((two === '}}' || two === ']]') && depth > 0) {
      depth--;
      cur += two;
      k++;
      continue;
    }
    if (s[k] === '|' && depth === 0) {
      parts.push(cur);
      cur = '';
      continue;
    }
    cur += s[k];
  }
  parts.push(cur);
  return parts;
}

// Every outermost template in the text, read with nesting, so a parameter
// that holds a template of its own does not end it early.
export function templates(text: string): Template[] {
  const out: Template[] = [];
  let i = 0;
  for (;;) {
    const start = text.indexOf('{{', i);
    if (start < 0) break;
    let depth = 0;
    let j = start;
    let end = -1;
    while (j < text.length - 1) {
      const two = text.slice(j, j + 2);
      if (two === '{{') {
        depth++;
        j += 2;
      } else if (two === '}}') {
        depth--;
        j += 2;
        if (depth === 0) {
          end = j;
          break;
        }
      } else {
        j++;
      }
    }
    if (end < 0) break;
    const parts = splitTop(text.slice(start + 2, end - 2));
    const name = (parts[0] ?? '').replace(/_/g, ' ').replace(/\s+/g, ' ').trim().toLowerCase();
    const params: string[] = [];
    const named = new Map<string, string>();
    for (const p of parts.slice(1)) {
      const eq = p.indexOf('=');
      if (eq > 0 && !/[{}[\]]/.test(p.slice(0, eq))) named.set(p.slice(0, eq).trim().toLowerCase(), p.slice(eq + 1).trim());
      else params.push(p.trim());
    }
    out.push({ name, params, named });
    i = end;
  }
  return out;
}

function param(t: Template, n: number): string | undefined {
  return t.named.get(String(n)) ?? t.params[n - 1];
}

function numberIn(raw: string | undefined): number | null {
  const m = raw?.match(/^\s*(\d{1,3}(?:\.\d+)?)\s*%?\s*$/);
  return m ? Number(m[1]) : null;
}

const pct = (v: number | null) => (v != null && v >= 0 && v <= 100 ? Math.round(v) : null);
const outOf10 = (v: number | null) => (v != null && v > 0 && v <= 10 ? round1(v) : null);
const outOf100 = (v: number | null) => (v != null && v > 0 && v <= 100 ? Math.round(v) : null);

export function fromTemplates(text: string): Reception {
  const out: Reception = { metacritic: null, tomatometer: null, average: null };
  for (const t of templates(text)) {
    if (/^(rotten tomatoes prose|rt prose)$/.test(t.name)) {
      out.tomatometer ??= pct(numberIn(param(t, 1)));
      out.average ??= outOf10(numberIn(param(t, 2)));
    } else if (/^(metacritic film prose|mc film|metacritic film)$/.test(t.name)) {
      out.metacritic ??= outOf100(numberIn(param(t, 1)));
    }
  }
  return out;
}

export interface Section {
  level: number;
  title: string;
  start: number;
  end: number;
}

// Sections by their headings; a section runs to the next heading of the
// same level or higher, its subsections included.
export function sections(text: string): Section[] {
  const heads = [...text.matchAll(/^(={2,6})[ \t]*(.+?)[ \t]*\1[ \t]*$/gm)];
  return heads.map((h, idx) => {
    const level = h[1].length;
    let end = text.length;
    for (let k = idx + 1; k < heads.length; k++) {
      if (heads[k][1].length <= level) {
        end = heads[k].index ?? text.length;
        break;
      }
    }
    const title = h[2].replace(/\[\[(?:[^|\]]*\|)?([^\]]*)\]\]/g, '$1').replace(/'{2,}/g, '').trim();
    return { level, title, start: (h.index ?? 0) + h[0].length, end };
  });
}

// Readable sentences: links become their words, a few formatting templates
// keep theirs, every other template goes.
export function prose(text: string): string {
  let t = text;
  for (let pass = 0; pass < 6 && t.includes('{{'); pass++) {
    t = t.replace(/\{\{([^{}]*)\}\}/g, (_m, inner: string) => {
      const [name, ...rest] = inner.split('|');
      const n = name.replace(/_/g, ' ').trim().toLowerCase();
      if (/^(nowrap|nobr|nobreak|small|sic|nowrap begin)$/.test(n)) return rest.join(' ');
      if (/^(snd|ndash|mdash|spaced ndash|spnd)$/.test(n)) return ' - ';
      if (/^(percent|pct)$/.test(n)) return `${rest[0] ?? ''}%`;
      return '';
    });
  }
  return t
    .replace(/\[\[(?:[^|\]]*\|)?([^\]]*)\]\]/g, '$1')
    .replace(/'{2,}/g, '')
    .replace(/&nbsp;|&#160;|&thinsp;/gi, ' ')
    .replace(/&#37;|&percnt;/gi, '%')
    .replace(/<[^>]+>/g, '')
    .replace(/[ \t]+/g, ' ');
}

export function sentences(text: string): string[] {
  return text
    .split(/\n+|(?<=[.!?])\s+(?=[A-Z0-9"'(“‘[])/)
    .map((s) => s.trim())
    .filter(Boolean);
}

const RT_NAME = /rotten\s*tomatoes/i;
const MC_NAME = /metacritic/i;
// What follows these is an audience's figure, not the critics'.
const AUDIENCE_WORD = /\b(audiences?|popcorn\w*|verified|users?|viewers?|fans?)\b/i;
const PCT = /(\d{1,3}(?:\.\d+)?)\s*(?:%|percent\b|per\s*cent\b)/i;
const AVG = /(\d{1,2}(?:\.\d{1,2})?)\s*(?:\/|out of)\s*10(?!\d|\.\d)/i;
const MC_100 = /(\d{1,3})\s*(?:\/|out of)\s*100(?!\d)/i;
const MC_SCORE = /\bscore of\s*(\d{1,3})\b(?!\s*(?:%|percent|per\s*cent|\.\d|\/\s*10(?!0)|out of 10(?!0)))/i;
const OTHER_SITE = /metacritic|imdb|cinemascore|posttrak|letterboxd/i;

function beforeAudience(s: string): string {
  const m = AUDIENCE_WORD.exec(s);
  return m ? s.slice(0, m.index) : s;
}

export function fromProse(text: string): Reception {
  const out: Reception = { metacritic: null, tomatometer: null, average: null };
  const list = sentences(prose(text));

  for (let i = 0; i < list.length && out.tomatometer == null && out.average == null; i++) {
    if (!RT_NAME.test(list[i])) continue;
    const s = beforeAudience(list[i]);
    if (!RT_NAME.test(s)) continue;
    const p = pct(numberIn(s.match(PCT)?.[1]));
    let a = outOf10(numberIn(s.match(AVG)?.[1]));
    // "... based on 120 reviews. The average rating is 7.2/10."
    const next = list[i + 1];
    if (p != null && a == null && next && /average/i.test(next) && !OTHER_SITE.test(next) && !RT_NAME.test(next)) {
      a = outOf10(numberIn(beforeAudience(next).match(AVG)?.[1]));
    }
    if (p != null || a != null) {
      out.tomatometer = p;
      out.average = a;
    }
  }

  for (const sentence of list) {
    if (out.metacritic != null) break;
    const at = sentence.search(MC_NAME);
    if (at < 0) continue;
    // Its own figures come after its name; failing that, before it.
    const after = beforeAudience(sentence.slice(at));
    const before = beforeAudience(sentence.slice(0, at));
    for (const part of [after, before]) {
      const v = outOf100(numberIn(part.match(MC_100)?.[1])) ?? outOf100(numberIn(part.match(MC_SCORE)?.[1]));
      if (v != null) {
        out.metacritic = v;
        break;
      }
    }
  }
  return out;
}

function fill(out: Reception, add: Reception): void {
  out.metacritic ??= add.metacritic;
  if (out.tomatometer == null && out.average == null) {
    out.tomatometer = add.tomatometer;
    out.average = add.average;
  }
}

// A film's critics' scores as its article gives them.
export function receptionScores(wikitext: string): Reception {
  const text = stripNoise(wikitext);
  const secs = sections(text);
  const regions: string[] = [];

  const reception = secs.find((s) => RECEPTION.test(s.title) && !OFF_TOPIC.test(s.title));
  if (reception) regions.push(text.slice(reception.start, reception.end));

  // The rest of the article, less the sections about other films.
  const cut = secs
    .filter((s) => OFF_TOPIC.test(s.title))
    .map((s) => [s.start, s.end] as [number, number])
    .sort((a, b) => a[0] - b[0]);
  let rest = '';
  let from = 0;
  for (const [s, e] of cut) {
    if (s > from) rest += text.slice(from, s);
    from = Math.max(from, e);
  }
  rest += text.slice(from);
  regions.push(rest);

  const out: Reception = { metacritic: null, tomatometer: null, average: null };
  for (const region of regions) {
    fill(out, fromTemplates(region));
    fill(out, fromProse(region));
    if (out.metacritic != null && (out.tomatometer != null || out.average != null)) break;
  }
  return out;
}

const hasAny = (r: Reception) => r.metacritic != null || r.tomatometer != null || r.average != null;

// ---------------------------------------------------------------------------
// Asking Wikidata and Wikipedia

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface PassResult {
  written: number;
  failedRows: number;
  failedBatches: number;
  batches: number;
}

// Catalogue rows to look up, by IMDb id. Several rows can share one id.
async function idsByImdb(supabase: SupabaseClient, table: 'movies' | 'tv_shows', floor: number | null): Promise<Map<string, string[]>> {
  const byImdb = new Map<string, string[]>();
  for (let from = 0; ; from += PAGE) {
    let q = supabase.from(table).select('id, imdb_id').eq('scoring_status', 'scored').not('imdb_id', 'is', null);
    if (floor != null) q = q.gte('vote_count', floor);
    const { data, error } = await q.order('id', { ascending: true }).range(from, from + PAGE - 1);
    if (error) throw new Error(`Reading ${table} failed: ${error.message}`);
    const rows = (data ?? []) as { id: string; imdb_id: string }[];
    for (const r of rows) {
      if (!/^tt\d{5,10}$/.test(r.imdb_id)) continue;
      const ids = byImdb.get(r.imdb_id) ?? [];
      ids.push(r.id);
      byImdb.set(r.imdb_id, ids);
    }
    if (rows.length < PAGE) break;
  }
  return byImdb;
}

async function ask<B>(wikidataFetch: typeof fetch, query: string): Promise<B[]> {
  const url = `${ENDPOINT}?format=json&query=${encodeURIComponent(query)}`;
  const res = await wikidataFetch(url, {
    headers: { Accept: 'application/sparql-results+json', 'User-Agent': USER_AGENT },
    signal: AbortSignal.timeout(ASK_TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`answered ${res.status}`);
  const json = (await res.json()) as { results?: { bindings?: B[] } };
  return json.results?.bindings ?? [];
}

async function upsert(supabase: SupabaseClient, table: string, key: string, rows: Record<string, unknown>[]) {
  let written = 0;
  let failedRows = 0;
  for (let i = 0; i < rows.length; i += UPSERT_BATCH) {
    const chunk = rows.slice(i, i + UPSERT_BATCH);
    const { error } = await supabase.from(table).upsert(chunk, { onConflict: key });
    if (error) {
      failedRows += chunk.length;
      console.error(`Writing ${table} rows ${i}-${i + chunk.length} failed: ${error.message}`);
    } else {
      written += chunk.length;
    }
  }
  return { written, failedRows };
}

async function seriesPass(supabase: SupabaseClient, wikidataFetch: typeof fetch): Promise<PassResult> {
  const byImdb = await idsByImdb(supabase, 'tv_shows', TV_VOTE_FLOOR);
  const imdbIds = [...byImdb.keys()];
  console.log(`${imdbIds.length} series to look up on Wikidata.`);

  const found = new Map<string, TvFound>();
  let failedBatches = 0;
  const deadline = Date.now() + TV_TIME_LIMIT_MS;
  for (let i = 0; i < imdbIds.length; i += TV_BATCH) {
    if (Date.now() > deadline) {
      const left = Math.ceil((imdbIds.length - i) / TV_BATCH);
      failedBatches += left;
      console.error(`Series pass stopped at its time limit with ${left} queries not asked.`);
      break;
    }
    try {
      const batch = imdbIds.slice(i, i + TV_BATCH);
      const series = await ask<TvBinding>(wikidataFetch, sparqlTvSeries(batch));
      await sleep(PAUSE_MS);
      const seasons = await ask<TvBinding>(wikidataFetch, sparqlTvSeasons(batch));
      for (const [imdb, f] of parseTv([...series, ...seasons])) found.set(imdb, f);
    } catch (err) {
      failedBatches++;
      console.error(`Series batch at ${i} failed: ${(err as Error).message}`);
    }
    if ((i / TV_BATCH) % 20 === 0) {
      console.log(`Series: ${Math.min(i + TV_BATCH, imdbIds.length)}/${imdbIds.length}, ${found.size} with statements.`);
    }
    await sleep(PAUSE_MS);
  }

  const stamp = new Date().toISOString();
  const rows: Record<string, unknown>[] = [];
  for (const [imdb, f] of found) {
    if (f.metacritic == null && f.tomatometer == null && f.average == null) continue;
    for (const tvId of byImdb.get(imdb) ?? []) {
      rows.push({
        tv_id: tvId,
        imdb_id: imdb,
        wikidata_id: f.wikidata,
        metacritic: f.metacritic,
        rt_tomatometer: f.tomatometer,
        rt_average: f.average,
        seasons: f.seasons,
        fetched_at: stamp,
      });
    }
  }
  const { written, failedRows } = await upsert(supabase, 'tv_critic_scores', 'tv_id', rows);

  const count = (pick: (f: TvFound) => unknown) => [...found.values()].filter(pick).length;
  console.log(
    `Series found: ${count((f) => f.metacritic != null)} Metascores, ${count((f) => f.average != null)} Rotten Tomatoes averages, ` +
      `${count((f) => f.tomatometer != null)} Tomatometers (${count((f) => f.seasons > 0)} by season), across ${rows.length} series.`,
  );
  return { written, failedRows, failedBatches, batches: Math.ceil(imdbIds.length / TV_BATCH) };
}

async function filmPass(
  supabase: SupabaseClient,
  wikidataFetch: typeof fetch,
  byImdb: Map<string, string[]>,
): Promise<PassResult & { found: Map<string, Found> }> {
  const imdbIds = [...byImdb.keys()];
  console.log(`${imdbIds.length} films to look up on Wikidata.`);

  const found = new Map<string, Found>();
  const methods = new Map<string, number>();
  let failedBatches = 0;
  for (let i = 0; i < imdbIds.length; i += BATCH) {
    try {
      const bindings = await ask<Binding>(wikidataFetch, sparql(imdbIds.slice(i, i + BATCH)));
      for (const b of bindings) {
        const label = b.methodLabel?.value ?? '(none)';
        methods.set(label, (methods.get(label) ?? 0) + 1);
      }
      for (const [imdb, f] of parse(bindings)) found.set(imdb, f);
    } catch (err) {
      failedBatches++;
      console.error(`Film batch at ${i} failed: ${(err as Error).message}`);
    }
    if ((i / BATCH) % 20 === 0) {
      console.log(`Films: ${Math.min(i + BATCH, imdbIds.length)}/${imdbIds.length}, ${found.size} with statements.`);
    }
    await sleep(PAUSE_MS);
  }

  const stamp = new Date().toISOString();
  const rows: Record<string, unknown>[] = [];
  for (const [imdb, f] of found) {
    if (!f.metacritic && !f.tomatometer && !f.average) continue;
    for (const movieId of byImdb.get(imdb) ?? []) {
      rows.push({
        movie_id: movieId,
        imdb_id: imdb,
        wikidata_id: f.wikidata,
        metacritic: f.metacritic?.value ?? null,
        rt_tomatometer: f.tomatometer?.value ?? null,
        rt_average: f.average?.value ?? null,
        source: 'wikidata',
        fetched_at: stamp,
      });
    }
  }
  const { written, failedRows } = await upsert(supabase, 'critic_scores', 'movie_id', rows);

  const count = (pick: (f: Found) => unknown) => [...found.values()].filter(pick).length;
  console.log('Determination methods seen (film statements):');
  for (const [label, n] of [...methods.entries()].sort((a, b) => b[1] - a[1])) console.log(`  ${n}\t${label}`);
  console.log(
    `Films found: ${count((f) => f.metacritic)} Metascores, ${count((f) => f.average)} Rotten Tomatoes averages, ` +
      `${count((f) => f.tomatometer)} Tomatometers, across ${rows.length} films.`,
  );
  return { written, failedRows, failedBatches, batches: Math.ceil(imdbIds.length / BATCH), found };
}

type GetJson = (url: string) => Promise<any>;

function wikiGetter(): GetJson {
  const f = resilientFetch({ label: 'Wikipedia', retryOn: API_STATUSES, attempts: 4 });
  return async (url: string) => {
    for (let attempt = 1; ; attempt++) {
      try {
        const res = await f(url, {
          headers: { Accept: 'application/json', 'User-Agent': USER_AGENT },
          signal: AbortSignal.timeout(WIKI_TIMEOUT_MS),
        });
        if (!res.ok) throw new Error(`answered ${res.status}`);
        return await res.json();
      } catch (err) {
        if (attempt >= 3) throw err;
        console.log(`  Wikipedia: ${(err as Error).message} -- asking again`);
        await sleep(5000 * attempt);
      }
    }
  };
}

const wikiUrl = (params: Record<string, string>) =>
  `${WIKI_API}?${new URLSearchParams({ action: 'query', format: 'json', formatversion: '2', ...params }).toString()}`;

// The English Wikipedia article of each IMDb id that has one.
async function articlesFor(wikidataFetch: typeof fetch, imdbIds: string[]): Promise<{ titles: Map<string, { item: string; title: string }>; failed: number }> {
  const titles = new Map<string, { item: string; title: string }>();
  let failed = 0;
  for (let i = 0; i < imdbIds.length; i += BATCH) {
    try {
      const rows = await ask<{ imdb?: { value: string }; item?: { value: string }; title?: { value: string } }>(
        wikidataFetch,
        sparqlSitelinks(imdbIds.slice(i, i + BATCH)),
      );
      for (const r of rows) {
        const imdb = r.imdb?.value;
        const title = r.title?.value;
        if (imdb && title && !titles.has(imdb)) titles.set(imdb, { item: lastSegment(r.item?.value ?? ''), title });
      }
    } catch (err) {
      failed++;
      console.error(`Article lookup at ${i} failed: ${(err as Error).message}`);
    }
    await sleep(PAUSE_MS);
  }
  return { titles, failed };
}

// Reads each article and keeps only its scores, a batch at a time, so the
// articles' text is never all held at once.
async function scoresFromArticles(
  getJson: GetJson,
  titles: string[],
  deadline: number,
): Promise<{ scores: Map<string, Reception>; read: number; failed: number; stopped: boolean }> {
  const scores = new Map<string, Reception>();
  let read = 0;
  let failed = 0;
  let i = 0;
  while (i < titles.length) {
    if (Date.now() > deadline) return { scores, read, failed, stopped: true };
    const batch: string[] = [];
    let length = 0;
    while (i < titles.length && batch.length < WIKI_BATCH) {
      const size = encodeURIComponent(titles[i]).length + 3;
      if (batch.length > 0 && length + size > WIKI_URL_MAX) break;
      batch.push(titles[i]);
      length += size;
      i++;
    }
    try {
      const step = new Map<string, string>();
      const redirect = new Map<string, string>();
      const byTitle = new Map<string, Reception>();
      let cont: Record<string, string> | undefined;
      for (let round = 0; round < 25; round++) {
        const json = await getJson(
          wikiUrl({ prop: 'revisions', rvprop: 'content', rvslots: 'main', redirects: '1', titles: batch.join('|'), ...(cont ?? {}) }),
        );
        const q = json.query ?? {};
        for (const n of q.normalized ?? []) step.set(n.from, n.to);
        for (const r of q.redirects ?? []) redirect.set(r.from, r.to);
        for (const p of q.pages ?? []) {
          const text = p.revisions?.[0]?.slots?.main?.content;
          if (typeof text === 'string' && typeof p.title === 'string' && !byTitle.has(p.title)) {
            byTitle.set(p.title, receptionScores(text));
            read++;
          }
        }
        cont = json.continue;
        if (!cont) break;
        await sleep(WIKI_PAUSE_MS);
      }
      for (const t of batch) {
        let x = step.get(t) ?? t;
        for (let hops = 0; hops < 3 && redirect.has(x); hops++) x = redirect.get(x)!;
        const r = byTitle.get(x);
        if (r) scores.set(t, r);
      }
    } catch (err) {
      failed++;
      console.error(`Article batch ending at ${i} failed: ${(err as Error).message}`);
    }
    if (Math.floor(i / WIKI_BATCH) % 40 === 0) console.log(`Wikipedia: ${i}/${titles.length} articles asked for, ${read} read.`);
    await sleep(WIKI_PAUSE_MS);
  }
  return { scores, read, failed, stopped: false };
}

// Which films already have Wikidata scores on record, from this run or an
// earlier one: Wikipedia only fills the gaps.
async function moviesWithWikidataScores(supabase: SupabaseClient): Promise<Set<string>> {
  const out = new Set<string>();
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await supabase
      .from('critic_scores')
      .select('movie_id')
      .eq('source', 'wikidata')
      .order('movie_id', { ascending: true })
      .range(from, from + PAGE - 1);
    if (error) throw new Error(`Reading critic_scores failed: ${error.message}`);
    const rows = (data ?? []) as { movie_id: string }[];
    for (const r of rows) out.add(r.movie_id);
    if (rows.length < PAGE) break;
  }
  return out;
}

const near = (a: number | null, b: number | null | undefined, within: number) => a != null && b != null && Math.abs(a - b) <= within;

async function wikipediaPass(
  supabase: SupabaseClient,
  wikidataFetch: typeof fetch,
  byImdb: Map<string, string[]>,
  wikidataFound: Map<string, Found>,
): Promise<PassResult & { report: string }> {
  const getJson = wikiGetter();
  const deadline = Date.now() + WIKI_TIME_LIMIT_MS;
  const covered = await moviesWithWikidataScores(supabase);
  const candidates = [...byImdb.entries()].filter(([, ids]) => ids.every((id) => !covered.has(id))).map(([imdb]) => imdb);
  console.log(`${candidates.length} films with no Wikidata scores to look up on Wikipedia.`);

  // A sample of films that do have Wikidata scores, read the same way, to
  // say how often the two sources agree.
  const withScores = [...wikidataFound.entries()].filter(([, f]) => f.metacritic || f.tomatometer || f.average).map(([imdb]) => imdb);
  const stride = Math.max(1, Math.floor(withScores.length / CHECK_SAMPLE));
  const sample = withScores.filter((_, k) => k % stride === 0).slice(0, CHECK_SAMPLE);

  const { titles, failed: lookupFailed } = await articlesFor(wikidataFetch, [...candidates, ...sample]);
  console.log(`${titles.size} of them have an English Wikipedia article.`);

  const askFor = [...new Set([...titles.values()].map((t) => t.title))];
  const { scores, read, failed: readFailed, stopped } = await scoresFromArticles(getJson, askFor, deadline);
  if (stopped) console.error('Wikipedia pass stopped at its time limit; writing what it has.');

  // How the two sources compare on the sample.
  let mcBoth = 0, mcAgree = 0, tmBoth = 0, tmAgree = 0, avgBoth = 0, avgAgree = 0;
  for (const imdb of sample) {
    const f = wikidataFound.get(imdb);
    const t = titles.get(imdb);
    const r = t ? scores.get(t.title) : undefined;
    if (!f || !r) continue;
    if (f.metacritic && r.metacritic != null) { mcBoth++; if (near(r.metacritic, f.metacritic.value, 2)) mcAgree++; }
    if (f.tomatometer && r.tomatometer != null) { tmBoth++; if (near(r.tomatometer, f.tomatometer.value, 2)) tmAgree++; }
    if (f.average && r.average != null) { avgBoth++; if (near(r.average, f.average.value, 0.2)) avgAgree++; }
  }

  const stamp = new Date().toISOString();
  const rows: Record<string, unknown>[] = [];
  let mc = 0, tm = 0, avg = 0, films = 0;
  for (const imdb of candidates) {
    const t = titles.get(imdb);
    const r = t ? scores.get(t.title) : undefined;
    if (!t || !r || !hasAny(r)) continue;
    films++;
    if (r.metacritic != null) mc++;
    if (r.tomatometer != null) tm++;
    if (r.average != null) avg++;
    for (const movieId of byImdb.get(imdb) ?? []) {
      rows.push({
        movie_id: movieId,
        imdb_id: imdb,
        wikidata_id: t.item || null,
        metacritic: r.metacritic,
        rt_tomatometer: r.tomatometer,
        rt_average: r.average,
        source: 'wikipedia',
        fetched_at: stamp,
      });
    }
  }
  const { written, failedRows } = await upsert(supabase, 'critic_scores', 'movie_id', rows);

  const candidateSet = new Set(candidates);
  const withArticle = [...titles.keys()].filter((k) => candidateSet.has(k)).length;
  const report =
    `wikipedia: ${candidates.length} films without Wikidata scores, ${withArticle} with an article, ` +
    `${read} articles read, scores for ${films} films (${mc} Metascores, ${tm} Tomatometers, ${avg} averages)` +
    `${stopped ? ', stopped at time limit' : ''}; ` +
    `check against Wikidata on ${sample.length} films: Metascore ${mcAgree}/${mcBoth}, Tomatometer ${tmAgree}/${tmBoth}, average ${avgAgree}/${avgBoth} agree`;
  console.log(report);
  return {
    written,
    failedRows,
    failedBatches: lookupFailed + readFailed,
    batches: Math.ceil((candidates.length + sample.length) / BATCH) + Math.ceil(askFor.length / WIKI_BATCH),
    report,
  };
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_ROLE_KEY'), {
    global: { fetch: resilientFetch({ label: 'database', retryOn: GATEWAY_STATUSES }) },
  });
  // A query is a read, so it is sent again on a dropped connection, a rate
  // limit or a 5xx (see resilient-fetch.ts). A series query that ran out of
  // time on the service's side will run out again, so it gets three tries
  // rather than five.
  const filmFetch = resilientFetch({ label: 'Wikidata', retryOn: API_STATUSES, attempts: 5 });
  const seriesFetch = resilientFetch({ label: 'Wikidata', retryOn: API_STATUSES, attempts: 3 });

  const startedAt = new Date().toISOString();
  const passes: PassResult[] = [];
  const reports: string[] = [];
  let broken = false;

  const record = (name: string, r: PassResult) => {
    passes.push(r);
    console.log(`${name}: ${r.written} written, ${r.failedRows} failed, ${r.failedBatches} of ${r.batches} queries failed.\n`);
    if (r.batches > 0 && r.failedBatches === r.batches) broken = true;
  };

  try {
    record('Series', await seriesPass(supabase, seriesFetch));
  } catch (err) {
    broken = true;
    console.error(`Series pass failed: ${(err as Error).message}\n`);
  }

  try {
    const byImdb = await idsByImdb(supabase, 'movies', VOTE_FLOOR);
    const films = await filmPass(supabase, filmFetch, byImdb);
    record('Films', films);
    // Asked only when the Wikidata pass got answers: without them every
    // film would look uncovered.
    if (films.failedBatches < films.batches) {
      try {
        const wiki = await wikipediaPass(supabase, filmFetch, byImdb, films.found);
        record('Wikipedia', wiki);
        reports.push(wiki.report);
      } catch (err) {
        broken = true;
        console.error(`Wikipedia pass failed: ${(err as Error).message}\n`);
      }
    }
  } catch (err) {
    broken = true;
    console.error(`Films pass failed: ${(err as Error).message}\n`);
  }

  const written = passes.reduce((n, r) => n + r.written, 0);
  const failedRows = passes.reduce((n, r) => n + r.failedRows, 0);

  // Logged next to the other pipeline jobs, with the Wikipedia pass's
  // report. Best-effort: a logging failure must not fail a run that
  // otherwise worked.
  const { error: logError } = await supabase.from('pipeline_runs').insert({
    run_type: 'critic_scores_refresh',
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    rows_processed: written,
    rows_failed: failedRows,
    status: broken || (written === 0 && failedRows > 0) ? 'failed' : 'success',
    error_message: reports.length ? reports.join('\n') : null,
  });
  if (logError) console.error(`pipeline_runs logging failed (non-fatal): ${logError.message}`);

  console.log(`Done. ${written} written, ${failedRows} failed.`);
  if (broken) process.exit(1);
}

// Run when called as a script, not when imported (the parsers are tested
// on their own).
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}
