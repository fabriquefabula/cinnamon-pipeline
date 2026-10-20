// Release alerts: a notification on the day something someone liked or
// put on their watchlist comes out -- a film's release, a new series, or
// a new season of a series already out -- and when it starts streaming on
// a service in their country.
//
// Who gets one: anyone who turned alerts on for a device (a row in
// push_subscriptions, written by the ReleaseAlerts switch on the profile
// page) and liked the title or put it on their watchlist before the day
// it came out. A film added on its release day is already known to be out.
// Liked series matter most here: liking a show is the clearest sign of
// wanting its next season.
//
// The date is checked with TMDB on the day rather than read from the
// catalogue. The catalogue keeps release dates current only for the 300
// most popular upcoming titles (ingest-upcoming.ts), so a smaller film
// postponed in September would otherwise still be announced as out on
// its old date. For a series the season list gives each season's start
// date directly, which does not depend on how far TMDB has moved its
// "next episode" by the time this runs.
//
// Streaming: the database keeps a daily history of where every title
// streams (record_offer_arrivals), and streaming_alert_titles() returns
// the saved titles that arrived on a free or subscription service in the
// person's country -- the country their most recently used device
// reported -- not counting rentals, and not counting a title that was on
// the service already under another of its names. It must have been
// saved before the day the new listing could be seen on the site. Each
// title, service and country is announced once. A title that comes out
// and starts streaming in the same run gets one notification saying
// where. A device that has not reported a country yet gets no streaming
// alerts; it reports one the next time the profile page is opened on it.
//
// Each alert is recorded in release_alerts_sent before it goes out, so a
// second run on the same day sends nothing twice. If every device of that
// person fails for a reason that may pass, the record is removed again
// and the next run retries while the event is still inside LOOKBACK_DAYS.
//
// The sending keys (VAPID) are made by the first run and kept in
// push_config, which only the service role can read. The site reads the
// public half through push_public_key().
//
// What went out is release_alerts_sent (with sent_at); each run's counts
// are in the Actions log. pipeline_runs does not take this job's name.
// The run fails, and GitHub emails the owner, only when every TMDB lookup
// fails, which means TMDB or the key is broken rather than one title.
//
// Required env vars: TMDB_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=true -- prints what would be sent, sends and records nothing

import { createClient } from '@supabase/supabase-js';
import * as webPushModule from 'web-push';
import { API_STATUSES, GATEWAY_STATUSES, resilientFetch } from './resilient-fetch';

// Published web-push 3.x is CommonJS, so from this ES module its functions
// arrive as the default export. The library's main branch has since moved
// to ES-module-only with named exports and no default. This works with
// either, so a new release cannot quietly break the job.
const webpush: typeof webPushModule = (webPushModule as any).default ?? webPushModule;

const TMDB_API_KEY = requireEnv('TMDB_API_KEY');
const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');
const DRY_RUN = process.env.DRY_RUN === 'true';

// Who the push services can contact about these messages.
const VAPID_SUBJECT = 'https://www.cinnamondb.com';

// An event still counts this many days after its date, so a run that
// failed or was skipped is caught up the next day. Each alert still goes
// out once.
const LOOKBACK_DAYS = 2;

// Films whose stored date is older than this are not checked with TMDB.
const FILM_CHECK_DAYS = 14;

// More than this many releases for one person in one run become a single
// notification listing them; the same, separately, for titles that
// started streaming.
const MAX_SEPARATE = 3;

const CONCURRENCY = 10;
const PAGE_SIZE = 1000;

// How long a push service keeps trying to reach a device that is off.
const TTL_SECONDS = 2 * 24 * 60 * 60;

// A device that has failed this many times in a row for reasons other than
// "gone" is dropped.
const MAX_FAILURES = 10;

// A dropped connection or a gateway error is sent again where that is
// safe (see resilient-fetch.ts), instead of ending the run. The claim in
// release_alerts_sent is not: it is an upsert that ignores duplicates and
// asks for its rows back, which resilientFetch never repeats, and a second
// try after a first that landed unanswered would get none of its rows
// back -- those alerts would be taken as sent by an earlier run and never
// go out. The delete that un-records an alert is repeated, which is safe
// because only this job writes release_alerts_sent and two runs of it
// never overlap (the workflow's concurrency group).
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  global: {
    fetch: resilientFetch({
      label: 'database',
      retryOn: GATEWAY_STATUSES,
      // Read-only (STABLE) functions this job calls; nothing else it calls is sent twice.
      repeatRpc: (name) => ['release_alert_titles', 'streaming_alert_titles'].includes(name),
    }),
  },
});

// TMDB calls are sent again on a dropped connection, a rate limit or a
// 5xx (see resilient-fetch.ts).
const tmdbFetch = resilientFetch({ label: 'TMDB', retryOn: API_STATUSES });

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

function isoDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function addDays(iso: string, days: number): string {
  const d = new Date(`${iso}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return isoDate(d);
}

async function tmdbGet(path: string): Promise<any> {
  const res = await tmdbFetch(`https://api.themoviedb.org/3${path}`, {
    headers: { Authorization: `Bearer ${TMDB_API_KEY}` },
  });
  if (!res.ok) return null;
  return res.json();
}

async function runWithConcurrency<T, R>(
  items: T[],
  limit: number,
  worker: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let next = 0;
  async function runner() {
    while (next < items.length) {
      const i = next++;
      results[i] = await worker(items[i]);
    }
  }
  await Promise.all(Array.from({ length: limit }, runner));
  return results;
}

// ---------------------------------------------------------------- keys

async function loadKeys(): Promise<{ publicKey: string; privateKey: string }> {
  const read = () =>
    supabase.from('push_config').select('vapid_public_key, vapid_private_key').eq('id', true).maybeSingle();

  const { data, error } = await read();
  if (error) throw error;
  if (data) return { publicKey: data.vapid_public_key, privateKey: data.vapid_private_key };

  // First run. ignoreDuplicates, then read back: if two first runs race,
  // both end up with the pair that was stored, never one each.
  const keys = webpush.generateVAPIDKeys();
  const { error: insertError } = await supabase
    .from('push_config')
    .upsert(
      { id: true, vapid_public_key: keys.publicKey, vapid_private_key: keys.privateKey },
      { onConflict: 'id', ignoreDuplicates: true },
    );
  if (insertError) throw insertError;

  const { data: stored, error: readError } = await read();
  if (readError) throw readError;
  if (!stored) throw new Error('push_config is still empty after creating the keys');
  console.log('Created the sending keys.');
  return { publicKey: stored.vapid_public_key, privateKey: stored.vapid_private_key };
}

// ------------------------------------------------------------- devices

interface Device {
  id: string;
  user_id: string;
  endpoint: string;
  p256dh: string;
  auth: string;
  failure_count: number;
}

async function fetchDevices(): Promise<Map<string, Device[]>> {
  const byUser = new Map<string, Device[]>();
  for (let from = 0; ; from += PAGE_SIZE) {
    const { data, error } = await supabase
      .from('push_subscriptions')
      .select('id, user_id, endpoint, p256dh, auth, failure_count')
      .order('id', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = (data ?? []) as Device[];
    for (const d of page) {
      const list = byUser.get(d.user_id) ?? [];
      list.push(d);
      byUser.set(d.user_id, list);
    }
    if (page.length < PAGE_SIZE) break;
  }
  return byUser;
}

// ---------------------------------------------------------- candidates

type List = 'watchlist' | 'liked';

interface Candidate {
  user_id: string;
  media: 'movie' | 'tv';
  item_id: string;
  tmdb_id: number;
  slug: string | null;
  title: string;
  backdrop_url: string | null;
  added_at: string;
  list: List; // 'watchlist' when it is on both
}

async function fetchCandidates(filmSince: string): Promise<Candidate[]> {
  const out: Candidate[] = [];
  for (let from = 0; ; from += PAGE_SIZE) {
    const { data, error } = await supabase
      .rpc('release_alert_titles', { p_film_since: filmSince })
      .order('user_id', { ascending: true })
      .order('media', { ascending: true })
      .order('item_id', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = (data ?? []) as Candidate[];
    out.push(...page);
    if (page.length < PAGE_SIZE) break;
  }
  return out;
}

// One row per person, saved title and service it arrived on.
interface StreamingRow {
  user_id: string;
  media: 'movie' | 'tv';
  item_id: string;
  slug: string | null;
  title: string;
  backdrop_url: string | null;
  list: List;
  region: string;
  provider_slug: string;
  provider_label: string;
  first_seen: string;
}

async function fetchStreaming(since: string): Promise<StreamingRow[]> {
  const out: StreamingRow[] = [];
  for (let from = 0; ; from += PAGE_SIZE) {
    const { data, error } = await supabase
      .rpc('streaming_alert_titles', { p_since: since })
      .order('user_id', { ascending: true })
      .order('media', { ascending: true })
      .order('item_id', { ascending: true })
      .order('provider_slug', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = (data ?? []) as StreamingRow[];
    out.push(...page);
    if (page.length < PAGE_SIZE) break;
  }
  return out;
}

// --------------------------------------------------------------- events

interface ReleaseEvent {
  event: string; // 'release' for a film, 'season-N' for a series
  date: string;
  season: number | null;
}

// null when TMDB could not be read, so a failed lookup is told apart from
// "nothing comes out".
async function eventsFor(
  media: 'movie' | 'tv',
  tmdbId: number,
  from: string,
  to: string,
): Promise<ReleaseEvent[] | null> {
  if (media === 'movie') {
    const d = await tmdbGet(`/movie/${tmdbId}`);
    if (!d) return null;
    const date: string | null = d.release_date || null;
    return date && date >= from && date <= to ? [{ event: 'release', date, season: null }] : [];
  }

  const d = await tmdbGet(`/tv/${tmdbId}`);
  if (!d) return null;
  const events: ReleaseEvent[] = [];
  for (const s of Array.isArray(d.seasons) ? d.seasons : []) {
    const n = s?.season_number;
    const date: string | null = s?.air_date || null;
    // Season 0 is specials.
    if (typeof n !== 'number' || n < 1 || !date) continue;
    if (date >= from && date <= to) events.push({ event: `season-${n}`, date, season: n });
  }
  return events;
}

// --------------------------------------------------------------- alerts

interface Alert {
  kind: 'release' | 'streaming';
  user_id: string;
  media: 'movie' | 'tv';
  item_id: string;
  // 'release', 'season-N', or 'streaming:<service>:<country>'
  event: string;
  event_date: string;
  season: number | null;
  title: string;
  slug: string | null;
  backdrop_url: string | null;
  list: List;
  service: string | null; // streaming only: the service's name
}

function alertKey(a: { user_id: string; media: string; item_id: string; event: string }): string {
  return `${a.user_id}|${a.media}|${a.item_id}|${a.event}`;
}

function titleKey(a: Alert): string {
  return `${a.media}:${a.item_id}`;
}

function headline(a: Alert, today: string): string {
  const isToday = a.event_date === today;
  if (a.media === 'movie') return isToday ? `${a.title} is out today` : `${a.title} is out now`;
  if (a.season === 1) return isToday ? `${a.title} starts today` : `${a.title} has started`;
  return isToday ? `${a.title}: season ${a.season} starts today` : `${a.title}: season ${a.season} has started`;
}

// Which list it is on, said the way the rest of the site says it.
function whose(a: Alert): string {
  if (a.list === 'liked') return a.media === 'tv' ? 'You liked this series.' : 'You liked it.';
  return 'It’s on your watchlist.';
}

function body(a: Alert): string {
  if (a.media === 'tv' && a.season === 1) {
    return a.list === 'liked' ? 'A new series you liked.' : 'A new series on your watchlist.';
  }
  return whose(a);
}

// "Netflix", "Netflix and Hulu", "Hulu, Netflix and Tubi".
function services(alerts: Alert[]): string {
  const names = Array.from(new Set(alerts.map((a) => a.service).filter((s): s is string => !!s))).sort((x, y) =>
    x.localeCompare(y),
  );
  if (names.length <= 1) return names[0] ?? '';
  return `${names.slice(0, -1).join(', ')} and ${names[names.length - 1]}`;
}

function titleUrl(a: Alert): string {
  return `/${a.media === 'tv' ? 'tv' : 'movie'}/${a.slug ?? a.item_id}`;
}

function imageOf(a: Alert): string | undefined {
  return a.backdrop_url ? a.backdrop_url.replace('/w1280/', '/w780/') : undefined;
}

// Cut a running list of names at a word boundary so the notification
// stays readable.
function clip(list: string): string {
  return list.length > 160 ? `${list.slice(0, 157).replace(/,?\s*[^,]*$/, '')}, and more` : list;
}

const LIST_PAGE: Record<string, string> = {
  'movie:watchlist': '/watchlist',
  'movie:liked': '/favorites',
  'tv:watchlist': '/tv/watchlist',
  'tv:liked': '/tv/favorites',
};

// One list page when they all come from the same list, otherwise the
// profile page, which shows all four.
function listPage(alerts: Alert[]): string {
  const pages = new Set(alerts.map((a) => LIST_PAGE[`${a.media}:${a.list}`]));
  return pages.size === 1 ? Array.from(pages)[0] : '/profile';
}

interface Message {
  alerts: Alert[];
  payload: string;
}

// A release, with the services it streams on when it starts streaming in
// the same run.
interface ReleaseItem {
  alert: Alert;
  streaming: Alert[];
}

// Paths, not full addresses: the service worker opens them on whichever
// address the device signed up on.
function releaseMessages(items: ReleaseItem[], today: string): Message[] {
  if (items.length === 0) return [];
  if (items.length <= MAX_SEPARATE) {
    return items.map(({ alert: a, streaming }) => ({
      alerts: [a, ...streaming],
      payload: JSON.stringify({
        title: headline(a, today),
        body: streaming.length > 0 ? `Streaming on ${services(streaming)}. ${body(a)}` : body(a),
        url: titleUrl(a),
        tag: `${a.media}-${a.item_id}-${a.event}`,
        image: imageOf(a),
      }),
    }));
  }

  const releases = items.map((i) => i.alert);
  const names = releases.map((a) =>
    a.media === 'tv' && a.season && a.season > 1 ? `${a.title} (season ${a.season})` : a.title,
  );
  const lists = new Set(releases.map((a) => a.list));
  const n = releases.length;
  return [
    {
      alerts: items.flatMap((i) => [i.alert, ...i.streaming]),
      payload: JSON.stringify({
        title:
          lists.size > 1
            ? `${n} titles you saved are out`
            : lists.has('liked')
              ? `${n} titles you liked are out`
              : `${n} from your watchlist are out`,
        body: clip(names.join(', ')),
        url: listPage(releases),
        tag: `release-summary-${today}`,
      }),
    },
  ];
}

// One message per title, naming every service it arrived on.
function streamingMessages(groups: Alert[][], today: string): Message[] {
  if (groups.length === 0) return [];
  if (groups.length <= MAX_SEPARATE) {
    return groups.map((g) => {
      const a = g[0];
      return {
        alerts: g,
        payload: JSON.stringify({
          title: `${a.title} is now streaming`,
          body: `On ${services(g)}. ${whose(a)}`,
          url: titleUrl(a),
          tag: `${a.media}-${a.item_id}-streaming`,
          image: imageOf(a),
        }),
      };
    });
  }

  const firsts = groups.map((g) => g[0]);
  const lists = new Set(firsts.map((a) => a.list));
  const n = groups.length;
  return [
    {
      alerts: groups.flat(),
      payload: JSON.stringify({
        title:
          lists.size > 1
            ? `${n} titles you saved are now streaming`
            : lists.has('liked')
              ? `${n} titles you liked are now streaming`
              : `${n} from your watchlist are now streaming`,
        body: clip(groups.map((g) => `${g[0].title} (${services(g)})`).join(', ')),
        url: listPage(firsts),
        tag: `streaming-summary-${today}`,
      }),
    },
  ];
}

function messagesFor(alerts: Alert[], today: string): Message[] {
  const groups = new Map<string, Alert[]>();
  for (const a of alerts) {
    if (a.kind !== 'streaming') continue;
    groups.set(titleKey(a), [...(groups.get(titleKey(a)) ?? []), a]);
  }
  // A title that comes out and starts streaming in the same run gets one
  // notification, saying where.
  const items: ReleaseItem[] = [];
  for (const a of alerts) {
    if (a.kind !== 'release') continue;
    items.push({ alert: a, streaming: groups.get(titleKey(a)) ?? [] });
    groups.delete(titleKey(a));
  }
  return [...releaseMessages(items, today), ...streamingMessages(Array.from(groups.values()), today)];
}

type Outcome = 'sent' | 'gone' | 'failed' | 'retry';

async function deliver(device: Device, payload: string): Promise<Outcome> {
  try {
    await webpush.sendNotification(
      { endpoint: device.endpoint, keys: { p256dh: device.p256dh, auth: device.auth } },
      payload,
      { TTL: TTL_SECONDS },
    );
    return 'sent';
  } catch (err: any) {
    const status: number | undefined = err?.statusCode;
    // 404 and 410: the browser has dropped this subscription for good.
    if (status === 404 || status === 410) return 'gone';
    console.error(`Push to device ${device.id} failed: ${status ?? ''} ${err?.body ?? err?.message ?? err}`.trim());
    // Rate limits, server errors and no answer at all may pass by tomorrow.
    if (status === undefined || status === 429 || status >= 500) return 'retry';
    return 'failed';
  }
}

// ----------------------------------------------------------------- main

async function main() {
  const today = isoDate(new Date());
  const from = addDays(today, -LOOKBACK_DAYS);
  console.log(`Release alerts for ${from} to ${today}${DRY_RUN ? ' (DRY RUN)' : ''}.`);

  const keys = await loadKeys();
  webpush.setVapidDetails(VAPID_SUBJECT, keys.publicKey, keys.privateKey);

  const devices = await fetchDevices();
  const deviceCount = Array.from(devices.values()).reduce((n, list) => n + list.length, 0);
  console.log(`${deviceCount} devices for ${devices.size} people.`);

  let lookups = 0;
  let lookupFailures = 0;
  let delivered = 0;
  let undelivered = 0;
  let removed = 0;
  let claimedCount = 0;
  let dueCount = 0;
  let retried = 0;

  if (devices.size > 0) {
    const candidates = await fetchCandidates(addDays(today, -FILM_CHECK_DAYS));

    // Each title is looked up once however many people saved it.
    const titles = new Map<string, Candidate>();
    for (const c of candidates) titles.set(`${c.media}:${c.item_id}`, c);
    console.log(`${candidates.length} watchlist entries, ${titles.size} titles to check.`);

    const eventsByTitle = new Map<string, ReleaseEvent[]>();
    await runWithConcurrency(Array.from(titles.entries()), CONCURRENCY, async ([key, c]) => {
      lookups++;
      const events = await eventsFor(c.media, c.tmdb_id, from, today);
      if (events === null) lookupFailures++;
      else if (events.length > 0) eventsByTitle.set(key, events);
    });

    const due: Alert[] = [];
    for (const c of candidates) {
      for (const e of eventsByTitle.get(`${c.media}:${c.item_id}`) ?? []) {
        if (new Date(c.added_at).getTime() >= Date.parse(`${e.date}T00:00:00Z`)) continue;
        due.push({
          kind: 'release',
          user_id: c.user_id,
          media: c.media,
          item_id: c.item_id,
          event: e.event,
          event_date: e.date,
          season: e.season,
          title: c.title,
          slug: c.slug,
          backdrop_url: c.backdrop_url,
          list: c.list,
          service: null,
        });
      }
    }
    const releaseCount = due.length;

    // The database has already applied who, where and since when.
    for (const s of await fetchStreaming(from)) {
      due.push({
        kind: 'streaming',
        user_id: s.user_id,
        media: s.media,
        item_id: s.item_id,
        event: `streaming:${s.provider_slug}:${s.region}`,
        event_date: s.first_seen,
        season: null,
        title: s.title,
        slug: s.slug,
        backdrop_url: s.backdrop_url,
        list: s.list,
        service: s.provider_label,
      });
    }
    dueCount = due.length;
    console.log(`${due.length} alerts due: ${releaseCount} releases, ${due.length - releaseCount} streaming.`);

    if (DRY_RUN) {
      const byUser = new Map<string, Alert[]>();
      for (const a of due) byUser.set(a.user_id, [...(byUser.get(a.user_id) ?? []), a]);
      for (const [userId, alerts] of Array.from(byUser.entries())) {
        for (const m of messagesFor(alerts, today)) console.log(`  ${userId}: ${m.payload}`);
      }
      console.log('Dry run: nothing sent or recorded.');
      return;
    }

    // Claim before sending. Only rows that were not there already come
    // back, so whatever another run has claimed is skipped here.
    const claimed: Alert[] = [];
    const dueByKey = new Map(due.map((a) => [alertKey(a), a]));
    for (let i = 0; i < due.length; i += PAGE_SIZE) {
      const chunk = due.slice(i, i + PAGE_SIZE).map((a) => ({
        user_id: a.user_id,
        media: a.media,
        item_id: a.item_id,
        event: a.event,
        event_date: a.event_date,
      }));
      const { data, error } = await supabase
        .from('release_alerts_sent')
        .upsert(chunk, { onConflict: 'user_id,media,item_id,event', ignoreDuplicates: true })
        .select('user_id, media, item_id, event');
      if (error) throw error;
      for (const row of (data ?? []) as { user_id: string; media: string; item_id: string; event: string }[]) {
        const a = dueByKey.get(alertKey(row));
        if (a) claimed.push(a);
      }
    }
    claimedCount = claimed.length;
    console.log(`${claimed.length} not sent before.`);

    const byUser = new Map<string, Alert[]>();
    for (const a of claimed) byUser.set(a.user_id, [...(byUser.get(a.user_id) ?? []), a]);

    const succeeded = new Set<string>();
    const failedIds = new Map<string, number>();
    const goneIds = new Set<string>();
    const release: Alert[] = [];

    for (const [userId, alerts] of Array.from(byUser.entries())) {
      for (const message of messagesFor(alerts, today)) {
        let sent = false;
        let retry = false;
        for (const device of devices.get(userId) ?? []) {
          if (goneIds.has(device.id)) continue;
          const outcome = await deliver(device, message.payload);
          if (outcome === 'sent') {
            sent = true;
            succeeded.add(device.id);
            failedIds.delete(device.id);
          } else if (outcome === 'gone') {
            goneIds.add(device.id);
          } else {
            if (outcome === 'retry') retry = true;
            if (!succeeded.has(device.id)) failedIds.set(device.id, device.failure_count + 1);
          }
        }
        if (sent) {
          delivered += message.alerts.length;
        } else {
          undelivered += message.alerts.length;
          if (retry) release.push(...message.alerts);
        }
      }
    }

    // Not delivered anywhere, for a reason that may pass: un-record so the
    // next run tries again.
    retried = release.length;
    for (const a of release) {
      const { error } = await supabase
        .from('release_alerts_sent')
        .delete()
        .eq('user_id', a.user_id)
        .eq('media', a.media)
        .eq('item_id', a.item_id)
        .eq('event', a.event);
      if (error) console.error(`Could not un-record ${alertKey(a)}: ${error.message}`);
    }

    // Device bookkeeping.
    const now = new Date().toISOString();
    if (succeeded.size > 0) {
      const { error } = await supabase
        .from('push_subscriptions')
        .update({ last_success_at: now, failure_count: 0 })
        .in('id', Array.from(succeeded));
      if (error) console.error(`Could not mark delivered devices: ${error.message}`);
    }
    for (const [id, count] of Array.from(failedIds.entries())) {
      if (count >= MAX_FAILURES) {
        goneIds.add(id);
        continue;
      }
      const { error } = await supabase.from('push_subscriptions').update({ failure_count: count }).eq('id', id);
      if (error) console.error(`Could not count a failure for ${id}: ${error.message}`);
    }
    if (goneIds.size > 0) {
      const { error } = await supabase.from('push_subscriptions').delete().in('id', Array.from(goneIds));
      if (error) console.error(`Could not remove dropped devices: ${error.message}`);
      else removed = goneIds.size;
    }
  }

  console.log(
    `\nDone. ${dueCount} due, ${claimedCount} new, ${delivered} delivered, ${undelivered} not delivered ` +
      `(${retried} kept for a retry), ${removed} devices removed, ${lookupFailures}/${lookups} lookups failed.`,
  );

  if (lookups > 0 && lookupFailures === lookups) process.exit(1);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
