// fetch with a second try, for the network blips that used to end whole
// pipeline runs.
//
// Two runs in two days were lost to one failed request among hundreds:
// refresh-tv-new-recommendations to a 502 from the gateway in front of the
// database (2026-10-19), and ingest-upcoming to a connection TMDB reset
// part-way through 300 films (2026-10-20). Nothing was wrong with either
// job; one request failed and nothing sent it again.
//
// A third, assign-movie-clusters (2026-10-23), was lost to a 555 on its
// first read: the database gateway's own "Internal server error.", in
// plain text, after ten seconds with no answer from behind it. The same
// read had taken under a second the day before, and every other request
// in that minute was answered in milliseconds. A 555 is a gateway status
// like 502 or 504 and is sent again like one (GATEWAY_STATUSES, below).
//
// resilientFetch() sends a request again, after a growing pause, when it
// fails before an answer arrives (connection reset, refused, timed out, or
// cut off part-way through the body) or when the answer is one of
// `retryOn` -- the gateway and rate-limit statuses. Any other answer comes
// straight back, a 500 included: a 500 from the database is usually a
// statement timeout, which the jobs handle themselves by splitting the
// work, and sending it again here would only make them wait for the same
// answer.
//
// Only a request that is safe to send twice is sent again. One that failed
// in transit may still have been carried out, so a plain insert is never
// repeated -- it could land twice. GET, HEAD, PUT, PATCH and DELETE are
// repeatable by definition, and so is an upsert (Prefer:
// resolution=merge-duplicates), which leaves the same rows however many
// times it lands. An upsert that ignores duplicates leaves the same rows
// too, but a second landing answers with none of the rows the first one
// wrote, so it is repeated only when the job does not ask for those rows
// back (return=representation): a job that reads them to claim work would
// be told it had claimed nothing. A database function (/rest/v1/rpc/<name>)
// is repeated only when the job names it in repeatRpc, because only the
// job knows whether a function reads, recomputes or appends.
//
// The body of an answer is read in full before it is handed back, so a
// connection cut off part-way through a body is caught and retried here
// instead of surfacing later as a broken response.

export interface ResilientFetchOptions {
  /** Who is being called, for the log lines. */
  label: string;
  /** Answer statuses worth sending the request again for. */
  retryOn: ReadonlySet<number>;
  /** Tries in all, the first one included. */
  attempts?: number;
  /** The first pause; each one after it doubles, plus a little jitter. */
  baseDelayMs?: number;
  /** Database functions that may be called twice. */
  repeatRpc?: (name: string) => boolean;
}

/**
 * The request never got a real answer from behind the gateway. 555 is what
 * the database gateway answers, in plain text, when it gives up after ten
 * seconds without one (seen 2026-10-22 and 2026-10-23).
 */
export const GATEWAY_STATUSES: ReadonlySet<number> = new Set([502, 503, 504, 520, 521, 522, 523, 524, 555]);

/** For an API whose rate limits and 500s pass after a moment (TMDB). */
export const API_STATUSES: ReadonlySet<number> = new Set([429, 500, ...GATEWAY_STATUSES]);

type FetchInput = Parameters<typeof fetch>[0];

const NULL_BODY_STATUSES = new Set([101, 204, 205, 304]);
const MAX_DELAY_MS = 60_000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function urlOf(input: FetchInput): URL {
  if (typeof input === 'string') return new URL(input);
  if (input instanceof URL) return input;
  return new URL(input.url);
}

function methodOf(input: FetchInput, init?: RequestInit): string {
  const method = init?.method ?? (input instanceof Request ? input.method : 'GET');
  return method.toUpperCase();
}

function preferOf(input: FetchInput, init?: RequestInit): string {
  const headers = new Headers(init?.headers ?? (input instanceof Request ? input.headers : undefined));
  return headers.get('prefer') ?? '';
}

// A body that can be sent a second time. A stream cannot: the first try
// has already read it.
function bodyIsReusable(init?: RequestInit): boolean {
  const body = init?.body;
  return (
    body === undefined ||
    body === null ||
    typeof body === 'string' ||
    body instanceof URLSearchParams ||
    body instanceof ArrayBuffer ||
    ArrayBuffer.isView(body)
  );
}

function repeatable(input: FetchInput, init: RequestInit | undefined, options: ResilientFetchOptions): boolean {
  if (input instanceof Request && input.body !== null) return false;
  if (!bodyIsReusable(init)) return false;
  const method = methodOf(input, init);
  if (method !== 'POST') return true;
  const rpc = /\/rest\/v1\/rpc\/([^/?]+)/.exec(urlOf(input).pathname);
  if (rpc) return options.repeatRpc?.(decodeURIComponent(rpc[1])) ?? false;
  const prefer = preferOf(input, init);
  if (/resolution=merge-duplicates/i.test(prefer)) return true;
  return /resolution=ignore-duplicates/i.test(prefer) && !/return=representation/i.test(prefer);
}

// Retry-After, as seconds or as a date; null when absent or unreadable.
function retryAfterMs(value: string | null): number | null {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
  const at = Date.parse(value);
  return Number.isNaN(at) ? null : Math.max(0, at - Date.now());
}

function describe(err: TypeError): string {
  const cause = (err as TypeError & { cause?: { code?: string; message?: string } }).cause;
  const detail = cause?.code ?? cause?.message;
  return detail ? `${err.message} (${detail})` : err.message;
}

export function resilientFetch(options: ResilientFetchOptions): typeof fetch {
  const attempts = Math.max(1, options.attempts ?? 5);
  const baseDelayMs = options.baseDelayMs ?? 2000;

  const send = async (input: FetchInput, init?: RequestInit): Promise<Response> => {
    const canRepeat = repeatable(input, init, options);
    const where = `${methodOf(input, init)} ${urlOf(input).pathname}`;

    for (let attempt = 1; ; attempt++) {
      const last = !canRepeat || attempt >= attempts;
      let wait = Math.min(MAX_DELAY_MS, baseDelayMs * 2 ** (attempt - 1) + Math.random() * baseDelayMs * 0.5);
      let reason: string;

      try {
        const res = await fetch(input, init);
        // Read in full here, so a body cut off part-way is retried like
        // any other dropped connection.
        const body = NULL_BODY_STATUSES.has(res.status) ? null : await res.arrayBuffer();
        if (last || !options.retryOn.has(res.status)) {
          const headers = new Headers(res.headers);
          // The body is already decoded and its length may differ.
          headers.delete('content-encoding');
          headers.delete('content-length');
          return new Response(body, { status: res.status, statusText: res.statusText, headers });
        }
        reason = `answered ${res.status}`;
        const asked = retryAfterMs(res.headers.get('retry-after'));
        if (asked !== null) wait = Math.min(MAX_DELAY_MS, asked);
      } catch (err) {
        // fetch reports every network failure as a TypeError; anything
        // else (an abort, a bug) is not ours to retry.
        if (last || !(err instanceof TypeError)) throw err;
        reason = describe(err);
      }

      console.log(
        `  ${options.label}: ${where} ${reason} -- sending it again in ${(wait / 1000).toFixed(1)}s ` +
          `(retry ${attempt} of ${attempts - 1})`,
      );
      await sleep(wait);
    }
  };

  return send as typeof fetch;
}
