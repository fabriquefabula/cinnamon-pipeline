// The mood map: every well-known film laid out flat by how it feels, so
// films that feel alike sit together and one feeling shades into the next
// across the map. The site draws it at /map.
//
// The same job lays out the series map at /tv/map when run with
// MAP_MEDIUM=tv (layout-tv-mood-map.yml): every well-known series, from
// the tv_mood_map_* functions and tables, named with the TV moods. The two
// maps are laid out apart, each from its own catalogue, and everything
// below applies to both; "films" in it means series on the series map.
//
// The database decides which films are on the map and which moods each
// one belongs to (mood_map_input, mood_map_film_moods -- the same rules as
// the mood pages). This job works out where everything goes:
//   1. the layout itself;
//   2. turned the same way round every time -- darker films to the right,
//      warmer films at the top -- and scaled to 0-10000;
//   3. each mood's name, where that mood is most concentrated, kept apart
//      from the other names where the map allows.
// It writes the layout to mood_map_layout and the names to
// mood_map_label_layout, then calls mood_map_finish(), which puts both on
// the map in one go, so the site never shows half of one week's map and
// half of another's, or a map without its names.
//
// The layout is exact t-SNE (van der Maaten & Hinton 2008), started from
// the films' two strongest directions of difference (principal
// components) rather than from random positions, so the big picture --
// light against dark, warm against cold -- holds together (Kobak & Berens
// 2019). Every step follows scikit-learn's implementation: neighbourhoods
// sized by perplexity, early exaggeration, momentum and gains. Nothing in
// it is random, so a week in which nothing changed gives the same map.
//
// Chosen over UMAP after laying out this catalogue both ways: with t-SNE
// a film's ten closest films on the map are among the ten it feels most
// like twice as often (43% against 22%), while how far apart the regions
// sit stays as true to the readings (0.84 against 0.85, rank correlation
// of distances). Exact rather than approximated (Barnes-Hut): a few
// thousand films take under a minute.
//
// Distances are plain Euclidean on essence_vector_z, the same reading of
// how alike two films feel that the film-to-film paths use.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=true (lay out and report, write nothing)
//           MAP_MEDIUM=tv (the series map; the film map otherwise)

import { createClient } from '@supabase/supabase-js';
import { GATEWAY_STATUSES, resilientFetch } from './resilient-fetch';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');
const DRY_RUN = process.env.DRY_RUN === 'true';

// Where each map is read from and written to. The film map's names are
// the ones this job has always used.
const MAP =
  process.env.MAP_MEDIUM === 'tv'
    ? {
        name: 'series mood map',
        noun: 'series',
        input: 'tv_mood_map_input',
        about: 'tv_mood_map_show_moods',
        medium: 'tv',
        // "Sad TV shows" on the mood page, "Sad" on the map.
        suffix: /\s+TV shows$/i,
        layout: 'tv_mood_map_layout',
        id: 'show_id',
        labels: 'tv_mood_map_label_layout',
        finish: 'tv_mood_map_finish',
      }
    : {
        name: 'mood map',
        noun: 'films',
        input: 'mood_map_input',
        about: 'mood_map_film_moods',
        medium: 'movie',
        // "Sad movies" on the mood page, "Sad" on the map.
        suffix: /\s+movies$/i,
        layout: 'mood_map_layout',
        id: 'film_id',
        labels: 'mood_map_label_layout',
        finish: 'mood_map_finish',
      };

// Roughly how many films each film's neighbourhood takes in. Higher keeps
// more of the big picture, lower more of the fine detail; on this
// catalogue 50 kept nearly all of 30's fine detail (43% against 44%, as
// below) with more of the big picture and fewer films piled together.
const PERPLEXITY = 50;
const N_ITER = 1000;
// For the first stretch the pull between neighbours is exaggerated, so
// films find their groups before the groups settle against one another.
const EARLY_ITER = 250;
const EARLY_EXAGGERATION = 12;
const MOMENTUM_EARLY = 0.5;
const MOMENTUM = 0.8;
const MIN_GAIN = 0.01;

// The map runs 0 to MAP_SIZE along its longer side.
const MAP_SIZE = 10000;
// Mood names: how far around a spot counts as "there" (6% of the map),
// how far apart two places with names should be (12%), and how much worse
// than a mood's best spot a spot clear of the other names may be and still
// get that mood's name; past that, the name joins the names already where
// the mood is strongest, one under another.
const LABEL_RADIUS = 600;
const LABEL_SPACING = 1200;
const LABEL_SECOND_BEST = 0.4;
// A mood with fewer films than this on the map gets no name on it.
const LABEL_MIN_FILMS = 12;

const PAGE_SIZE = 1000;
const WRITE_CHUNK = 500;

// The finish call, dropped by the gateway, is sent again after a pause far
// longer than it takes (well under a second), and at most twice.
const GATEWAY_RETRIES = 2;
const GATEWAY_PAUSE_MS = 65000;

// A dropped connection or a gateway error is sent again where that is
// safe (see resilient-fetch.ts), instead of ending the run: the reads,
// the two upserts, and the map's two read-only (STABLE) input functions.
// The finish is left to main(), below.
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  global: {
    fetch: resilientFetch({
      label: 'database',
      retryOn: GATEWAY_STATUSES,
      repeatRpc: (name) => name === MAP.input || name === MAP.about,
    }),
  },
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// No answer from behind the gateway: a gateway status, or no answer at
// all (status 0 is how the client reports a request that never got one).
function isGatewayFailure(status: number): boolean {
  return status === 0 || GATEWAY_STATUSES.has(status);
}

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

interface Film {
  id: string;
  v: number[];
  darkness: number;
  warmth: number;
  moods: string[];
}

interface Mood {
  slug: string;
  name: string;
}

// ---------------------------------------------------------------------------
// Reading

function parseVector(raw: unknown): number[] | null {
  let arr: unknown[];
  if (Array.isArray(raw)) arr = raw;
  else if (typeof raw === 'string') arr = raw.replace(/[{}[\]\s]/g, '').split(',');
  else return null;
  const v = arr.map((x) => Number(x));
  return v.length > 0 && v.every((x) => Number.isFinite(x)) ? v : null;
}

async function fetchPaged<T>(fn: string): Promise<T[]> {
  const rows: T[] = [];
  for (let from = 0; ; from += PAGE_SIZE) {
    const { data, error } = await supabase
      .rpc(fn)
      .order('id', { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw error;
    const page = (data ?? []) as T[];
    rows.push(...page);
    if (page.length < PAGE_SIZE) break;
  }
  return rows;
}

async function fetchFilms(): Promise<Film[]> {
  const readings = await fetchPaged<{ id: string; v: unknown }>(MAP.input);
  const about = await fetchPaged<{ id: string; darkness: number | null; warmth: number | null; moods: string[] | null }>(
    MAP.about,
  );
  const byId = new Map(about.map((r) => [r.id, r]));

  const films: Film[] = [];
  for (const r of readings) {
    const v = parseVector(r.v);
    const a = byId.get(r.id);
    if (!v || !a) continue;
    films.push({
      id: r.id,
      v,
      darkness: Number(a.darkness ?? 50),
      warmth: Number(a.warmth ?? 50),
      moods: a.moods ?? [],
    });
  }

  // Every reading must have the same number of dimensions; a film read
  // under an older scheme would otherwise be measured against the rest on
  // different terms.
  const counts = new Map<number, number>();
  for (const f of films) counts.set(f.v.length, (counts.get(f.v.length) ?? 0) + 1);
  let dims = 0;
  let most = -1;
  for (const [d, c] of counts) {
    if (c > most) {
      most = c;
      dims = d;
    }
  }
  const kept = films.filter((f) => f.v.length === dims);
  if (kept.length < readings.length) {
    console.log(`Left out ${readings.length - kept.length} ${MAP.noun} with an unreadable or mismatched reading.`);
  }
  return kept;
}

async function fetchMoods(): Promise<Mood[]> {
  const { data, error } = await supabase
    .from('shelves')
    .select('slug, title, sort_order')
    .eq('kind', 'mood')
    .eq('medium', MAP.medium)
    .order('sort_order', { ascending: true });
  if (error) throw error;
  return ((data ?? []) as { slug: string; title: string }[]).map((s) => ({
    slug: s.slug,
    name: s.title.replace(MAP.suffix, ''),
  }));
}

// ---------------------------------------------------------------------------
// Neighbourhoods

interface Neighbours {
  k: number; // neighbours per film, not counting the film itself
  idx: Int32Array; // n * k, nearest first
  d2: Float64Array; // n * k, squared distances
}

// Exact nearest neighbours. A few thousand films is small enough to
// compare every pair, so nothing is approximated.
function nearestNeighbours(x: Float64Array[], k: number): Neighbours {
  const n = x.length;
  const dims = x[0].length;
  const idx = new Int32Array(n * k).fill(-1);
  const d2 = new Float64Array(n * k).fill(Infinity);
  for (let i = 0; i < n; i++) {
    const xi = x[i];
    const base = i * k;
    const last = base + k - 1;
    for (let j = 0; j < n; j++) {
      if (j === i) continue;
      const xj = x[j];
      let s = 0;
      for (let d = 0; d < dims; d++) {
        const t = xi[d] - xj[d];
        s += t * t;
      }
      if (s >= d2[last]) continue;
      let p = last;
      while (p > base && d2[p - 1] > s) {
        d2[p] = d2[p - 1];
        idx[p] = idx[p - 1];
        p--;
      }
      d2[p] = s;
      idx[p] = j;
    }
  }
  return { k, idx, d2 };
}

interface Affinities {
  i: Int32Array;
  j: Int32Array;
  p: Float64Array; // each unordered pair once; the full symmetric matrix sums to 1
}

// scikit-learn's _binary_search_perplexity over each film's 3 x PERPLEXITY
// nearest neighbours: a Gaussian around each film, narrowed until the
// neighbourhood holds about PERPLEXITY films' worth of weight -- tight
// where the catalogue is crowded, wide where it is sparse. Then made
// symmetric, P = (P + P^T) / 2n, as _joint_probabilities_nn does.
function affinities(nn: Neighbours, n: number): Affinities {
  const { k, idx, d2 } = nn;
  const cond = new Float64Array(n * k);
  const target = Math.log(PERPLEXITY);
  for (let i = 0; i < n; i++) {
    const base = i * k;
    let beta = 1;
    let lo = -Infinity;
    let hi = Infinity;
    for (let iter = 0; iter < 100; iter++) {
      let sum = 0;
      for (let p = base; p < base + k; p++) {
        cond[p] = Math.exp(-d2[p] * beta);
        sum += cond[p];
      }
      if (sum === 0) sum = 1e-8;
      let h = 0;
      for (let p = base; p < base + k; p++) {
        cond[p] /= sum;
        h += d2[p] * cond[p];
      }
      const diff = Math.log(sum) + beta * h - target;
      if (Math.abs(diff) <= 1e-5) break;
      if (diff > 0) {
        lo = beta;
        beta = hi === Infinity ? beta * 2 : (beta + hi) / 2;
      } else {
        hi = beta;
        beta = lo === -Infinity ? beta / 2 : (beta + lo) / 2;
      }
    }
  }

  const pairs = new Map<number, number>();
  for (let i = 0; i < n; i++) {
    for (let p = i * k; p < i * k + k; p++) {
      const j = idx[p];
      const key = Math.min(i, j) * n + Math.max(i, j);
      pairs.set(key, (pairs.get(key) ?? 0) + cond[p]);
    }
  }
  const keys = [...pairs.keys()].sort((a, b) => a - b);
  const pi = new Int32Array(keys.length);
  const pj = new Int32Array(keys.length);
  const pv = new Float64Array(keys.length);
  let total = 0;
  keys.forEach((key, e) => {
    pi[e] = Math.floor(key / n);
    pj[e] = key - pi[e] * n;
    pv[e] = pairs.get(key)!;
    total += 2 * pv[e];
  });
  for (let e = 0; e < pv.length; e++) pv[e] /= total;
  return { i: pi, j: pj, p: pv };
}

// ---------------------------------------------------------------------------
// Starting positions: the two principal components

// Eigenvalues and eigenvectors of a small symmetric matrix (Jacobi).
function symmetricEigen(m: number[][]): { values: number[]; vectors: number[][] } {
  const n = m.length;
  const a = m.map((r) => r.slice());
  const v: number[][] = Array.from({ length: n }, (_, i) => Array.from({ length: n }, (_, j) => (i === j ? 1 : 0)));
  for (let sweep = 0; sweep < 100; sweep++) {
    let off = 0;
    for (let p = 0; p < n; p++) for (let q = p + 1; q < n; q++) off += a[p][q] * a[p][q];
    if (off < 1e-22) break;
    for (let p = 0; p < n - 1; p++) {
      for (let q = p + 1; q < n; q++) {
        if (Math.abs(a[p][q]) < 1e-300) continue;
        const theta = (a[q][q] - a[p][p]) / (2 * a[p][q]);
        const t = (theta >= 0 ? 1 : -1) / (Math.abs(theta) + Math.sqrt(theta * theta + 1));
        const c = 1 / Math.sqrt(t * t + 1);
        const s = t * c;
        for (let r = 0; r < n; r++) {
          const arp = a[r][p];
          const arq = a[r][q];
          a[r][p] = c * arp - s * arq;
          a[r][q] = s * arp + c * arq;
        }
        for (let r = 0; r < n; r++) {
          const apr = a[p][r];
          const aqr = a[q][r];
          a[p][r] = c * apr - s * aqr;
          a[q][r] = s * apr + c * aqr;
        }
        for (let r = 0; r < n; r++) {
          const vrp = v[r][p];
          const vrq = v[r][q];
          v[r][p] = c * vrp - s * vrq;
          v[r][q] = s * vrp + c * vrq;
        }
      }
    }
  }
  const values = a.map((r, i) => r[i]);
  const vectors = values.map((_, col) => v.map((r) => r[col]));
  return { values, vectors };
}

// scikit-learn's init="pca": the two principal components, scaled so the
// first has a standard deviation of 1e-4.
function pcaStart(x: Float64Array[]): Float64Array {
  const n = x.length;
  const dims = x[0].length;
  const mean = new Float64Array(dims);
  for (const r of x) for (let d = 0; d < dims; d++) mean[d] += r[d];
  for (let d = 0; d < dims; d++) mean[d] /= n;
  const cov = Array.from({ length: dims }, () => new Array<number>(dims).fill(0));
  for (const r of x) {
    for (let p = 0; p < dims; p++) {
      const cp = r[p] - mean[p];
      for (let q = p; q < dims; q++) cov[p][q] += cp * (r[q] - mean[q]);
    }
  }
  for (let p = 0; p < dims; p++) {
    for (let q = p; q < dims; q++) {
      cov[p][q] /= n - 1;
      cov[q][p] = cov[p][q];
    }
  }
  const { values, vectors } = symmetricEigen(cov);
  const order = values.map((val, i) => ({ val, i })).sort((p, q) => q.val - p.val);
  const axes = [vectors[order[0].i], vectors[order[1].i]].map((axis) => {
    // A fixed sign, so the same catalogue always starts the same way up.
    let big = 0;
    for (const c of axis) if (Math.abs(c) > Math.abs(big)) big = c;
    return big < 0 ? axis.map((c) => -c) : axis;
  });

  const y = new Float64Array(n * 2);
  for (let i = 0; i < n; i++) {
    for (let c = 0; c < 2; c++) {
      let s = 0;
      for (let d = 0; d < dims; d++) s += (x[i][d] - mean[d]) * axes[c][d];
      y[i * 2 + c] = s;
    }
  }
  let m0 = 0;
  for (let i = 0; i < n; i++) m0 += y[i * 2];
  m0 /= n;
  let sd = 0;
  for (let i = 0; i < n; i++) sd += (y[i * 2] - m0) * (y[i * 2] - m0);
  sd = Math.sqrt(sd / n) || 1;
  for (let p = 0; p < n * 2; p++) y[p] = (y[p] / sd) * 1e-4;
  return y;
}

// ---------------------------------------------------------------------------
// Optimisation

// scikit-learn's exact t-SNE gradient and _gradient_descent: every pair of
// films pushes apart (Student-t), neighbours pull together in proportion
// to their affinity, with momentum and per-coordinate gains. Learning
// rate as scikit-learn's "auto": n / early exaggeration / 4, at least 50.
function optimise(y: Float64Array, P: Affinities, n: number): void {
  const learningRate = Math.max(n / EARLY_EXAGGERATION / 4, 50);
  const update = new Float64Array(n * 2);
  const gains = new Float64Array(n * 2).fill(1);
  const grad = new Float64Array(n * 2);
  const m = P.p.length;

  for (let iter = 0; iter < N_ITER; iter++) {
    const exaggeration = iter < EARLY_ITER ? EARLY_EXAGGERATION : 1;
    const momentum = iter < EARLY_ITER ? MOMENTUM_EARLY : MOMENTUM;
    grad.fill(0);

    let z = 0;
    for (let i = 0; i < n; i++) {
      const yi0 = y[i * 2];
      const yi1 = y[i * 2 + 1];
      let gx = 0;
      let gy = 0;
      for (let j = i + 1; j < n; j++) {
        const dx = yi0 - y[j * 2];
        const dy = yi1 - y[j * 2 + 1];
        const w = 1 / (1 + dx * dx + dy * dy);
        z += 2 * w;
        const f = w * w;
        gx += f * dx;
        gy += f * dy;
        grad[j * 2] -= f * dx;
        grad[j * 2 + 1] -= f * dy;
      }
      grad[i * 2] += gx;
      grad[i * 2 + 1] += gy;
    }
    const repel = -4 / z;
    for (let p = 0; p < n * 2; p++) grad[p] *= repel;

    for (let e = 0; e < m; e++) {
      const i = P.i[e];
      const j = P.j[e];
      const dx = y[i * 2] - y[j * 2];
      const dy = y[i * 2 + 1] - y[j * 2 + 1];
      const f = (4 * exaggeration * P.p[e]) / (1 + dx * dx + dy * dy);
      grad[i * 2] += f * dx;
      grad[i * 2 + 1] += f * dy;
      grad[j * 2] -= f * dx;
      grad[j * 2 + 1] -= f * dy;
    }

    for (let p = 0; p < n * 2; p++) {
      gains[p] = update[p] * grad[p] < 0 ? gains[p] + 0.2 : gains[p] * 0.8;
      if (gains[p] < MIN_GAIN) gains[p] = MIN_GAIN;
      update[p] = momentum * update[p] - learningRate * gains[p] * grad[p];
      y[p] += update[p];
    }
  }
}

// ---------------------------------------------------------------------------
// The same way round every time, at a fixed size

// Turn the map so the direction in which films get darker points right,
// flip it so warmer films are at the top (smaller y on a screen), then
// scale so the longer side runs 0-MAP_SIZE with the shorter one centred.
function orient(y: Float64Array, films: Film[]): Float64Array {
  const n = films.length;
  let mx = 0;
  let my = 0;
  for (let i = 0; i < n; i++) {
    mx += y[i * 2];
    my += y[i * 2 + 1];
  }
  mx /= n;
  my /= n;

  let md = 0;
  let mw = 0;
  for (const f of films) {
    md += f.darkness;
    mw += f.warmth;
  }
  md /= n;
  mw /= n;

  let gx = 0;
  let gy = 0;
  for (let i = 0; i < n; i++) {
    const d = films[i].darkness - md;
    gx += d * (y[i * 2] - mx);
    gy += d * (y[i * 2 + 1] - my);
  }
  const theta = gx === 0 && gy === 0 ? 0 : Math.atan2(gy, gx);
  const c = Math.cos(theta);
  const s = Math.sin(theta);

  const out = new Float64Array(n * 2);
  let warmY = 0;
  for (let i = 0; i < n; i++) {
    const px = y[i * 2] - mx;
    const py = y[i * 2 + 1] - my;
    out[i * 2] = px * c + py * s;
    out[i * 2 + 1] = -px * s + py * c;
    warmY += (films[i].warmth - mw) * out[i * 2 + 1];
  }
  if (warmY > 0) for (let i = 0; i < n; i++) out[i * 2 + 1] = -out[i * 2 + 1];

  let x0 = Infinity;
  let x1 = -Infinity;
  let y0 = Infinity;
  let y1 = -Infinity;
  for (let i = 0; i < n; i++) {
    x0 = Math.min(x0, out[i * 2]);
    x1 = Math.max(x1, out[i * 2]);
    y0 = Math.min(y0, out[i * 2 + 1]);
    y1 = Math.max(y1, out[i * 2 + 1]);
  }
  const k = MAP_SIZE / Math.max(x1 - x0, y1 - y0, 1e-9);
  const ox = (MAP_SIZE - (x1 - x0) * k) / 2;
  const oy = (MAP_SIZE - (y1 - y0) * k) / 2;
  for (let i = 0; i < n; i++) {
    out[i * 2] = (out[i * 2] - x0) * k + ox;
    out[i * 2 + 1] = (out[i * 2 + 1] - y0) * k + oy;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Mood names

interface Label {
  slug: string;
  name: string;
  x: number;
  y: number;
  members: number;
  rank: number;
}

// Films within LABEL_RADIUS of each film, found through a grid of cells
// LABEL_RADIUS wide.
function neighbourhoods(p: Float64Array, n: number): number[][] {
  const cell = (v: number) => Math.floor(v / LABEL_RADIUS);
  const grid = new Map<string, number[]>();
  for (let i = 0; i < n; i++) {
    const key = `${cell(p[i * 2])},${cell(p[i * 2 + 1])}`;
    const list = grid.get(key);
    if (list) list.push(i);
    else grid.set(key, [i]);
  }
  const r2 = LABEL_RADIUS * LABEL_RADIUS;
  const near: number[][] = [];
  for (let i = 0; i < n; i++) {
    const cx = cell(p[i * 2]);
    const cy = cell(p[i * 2 + 1]);
    const found: number[] = [];
    for (let gx = cx - 1; gx <= cx + 1; gx++) {
      for (let gy = cy - 1; gy <= cy + 1; gy++) {
        for (const j of grid.get(`${gx},${gy}`) ?? []) {
          const dx = p[i * 2] - p[j * 2];
          const dy = p[i * 2 + 1] - p[j * 2 + 1];
          if (dx * dx + dy * dy <= r2) found.push(j);
        }
      }
    }
    near.push(found);
  }
  return near;
}

// Each mood's name goes where that mood is most concentrated: the film of
// that mood around which the most films are of that mood, weighed by how
// large a share of all the films there they make up (m * m / all). The
// name sits at the centre of the mood's films around that spot.
//
// Moods overlap -- dark films are often tense ones -- so names are placed
// strongest first, and a name whose best spot is already named takes its
// best spot clear of the other names, if that spot is at least
// LABEL_SECOND_BEST as good; otherwise it joins the names already there,
// one under another, since that part of the map is all of those moods at
// once. Names at the same place share the same x and y; the site stacks
// them in rank order.
function placeLabels(p: Float64Array, films: Film[], moods: Mood[]): Label[] {
  const n = films.length;
  const near = neighbourhoods(p, n);
  const spacing2 = LABEL_SPACING * LABEL_SPACING;

  interface Candidates {
    mood: Mood;
    member: Uint8Array;
    count: number;
    score: Float64Array; // -1 where not a member
    bestAt: number;
  }
  const all: Candidates[] = [];
  for (const mood of moods) {
    const member = new Uint8Array(n);
    let count = 0;
    for (let i = 0; i < n; i++) {
      if (films[i].moods.includes(mood.slug)) {
        member[i] = 1;
        count++;
      }
    }
    if (count < LABEL_MIN_FILMS) continue;
    const score = new Float64Array(n).fill(-1);
    let bestAt = -1;
    for (let i = 0; i < n; i++) {
      if (!member[i]) continue;
      let m = 0;
      for (const j of near[i]) m += member[j];
      score[i] = (m * m) / near[i].length;
      if (bestAt < 0 || score[i] > score[bestAt]) bestAt = i;
    }
    all.push({ mood, member, count, score, bestAt });
  }
  all.sort((p1, p2) => p2.score[p2.bestAt] - p1.score[p1.bestAt]);

  const centre = (c: Candidates, at: number) => {
    let sx = 0;
    let sy = 0;
    let k = 0;
    for (const j of near[at]) {
      if (!c.member[j]) continue;
      sx += p[j * 2];
      sy += p[j * 2 + 1];
      k++;
    }
    return { x: sx / k, y: sy / k };
  };
  const dist2 = (i: number, g: { x: number; y: number }) => {
    const dx = p[i * 2] - g.x;
    const dy = p[i * 2 + 1] - g.y;
    return dx * dx + dy * dy;
  };

  const places: { x: number; y: number; names: Candidates[] }[] = [];
  for (const c of all) {
    let taken: (typeof places)[number] | null = null;
    for (const g of places) {
      if (dist2(c.bestAt, g) < spacing2 && (!taken || dist2(c.bestAt, g) < dist2(c.bestAt, taken))) taken = g;
    }
    if (!taken) {
      places.push({ ...centre(c, c.bestAt), names: [c] });
      continue;
    }
    let freeAt = -1;
    for (let i = 0; i < n; i++) {
      if (c.score[i] <= 0 || (freeAt >= 0 && c.score[i] <= c.score[freeAt])) continue;
      if (places.every((g) => dist2(i, g) >= spacing2)) freeAt = i;
    }
    if (freeAt >= 0 && c.score[freeAt] >= LABEL_SECOND_BEST * c.score[c.bestAt]) {
      places.push({ ...centre(c, freeAt), names: [c] });
    } else {
      taken.names.push(c);
    }
  }

  const labels: Label[] = [];
  for (const g of places) {
    for (const c of g.names) {
      labels.push({ slug: c.mood.slug, name: c.mood.name, x: g.x, y: g.y, members: c.count, rank: labels.length });
    }
  }
  return labels;
}

// ---------------------------------------------------------------------------
// Report: how well the map keeps each film next to the films it feels like

function keptNeighbours(y: Float64Array, nn: Neighbours, n: number, k: number): number {
  let total = 0;
  for (let i = 0; i < n; i++) {
    const near: { j: number; d: number }[] = [];
    for (let j = 0; j < n; j++) {
      if (j === i) continue;
      const dx = y[i * 2] - y[j * 2];
      const dy = y[i * 2 + 1] - y[j * 2 + 1];
      const d = dx * dx + dy * dy;
      if (near.length < k) {
        near.push({ j, d });
        near.sort((p, q) => p.d - q.d);
      } else if (d < near[k - 1].d) {
        near[k - 1] = { j, d };
        near.sort((p, q) => p.d - q.d);
      }
    }
    const felt = new Set<number>();
    for (let p = i * nn.k; p < i * nn.k + k; p++) felt.add(nn.idx[p]);
    let hits = 0;
    for (const { j } of near) if (felt.has(j)) hits++;
    total += hits / k;
  }
  return total / n;
}

// ---------------------------------------------------------------------------

async function main() {
  const started = Date.now();
  const runIso = new Date().toISOString();
  console.log(`Laying out the ${MAP.name}${DRY_RUN ? ' (DRY RUN)' : ''}...`);

  const [films, moods] = await Promise.all([fetchFilms(), fetchMoods()]);
  const n = films.length;
  const k = Math.floor(3 * PERPLEXITY);
  if (n <= k) throw new Error(`Only ${n} ${MAP.noun} to lay out -- too few for a map.`);
  console.log(`${n} ${MAP.noun}, ${films[0].v.length} dimensions, ${moods.length} moods.`);

  const x = films.map((f) => Float64Array.from(f.v));
  const nn = nearestNeighbours(x, k);
  const P = affinities(nn, n);
  console.log(`Neighbourhoods: ${P.p.length} pairs.`);

  const start = pcaStart(x);
  optimise(start, P, n);
  for (let p = 0; p < n * 2; p++) {
    if (!Number.isFinite(start[p])) throw new Error('The layout came out with a missing position.');
  }
  const pos = orient(start, films);

  const kept = keptNeighbours(pos, nn, n, 10);
  console.log(
    `On the map, ${(kept * 100).toFixed(1)}% of the 10 ${MAP.noun} nearest each one are among the 10 it feels most like.`,
  );

  const labels = placeLabels(pos, films, moods);
  for (const l of labels) {
    console.log(`  ${l.name}: ${l.members} ${MAP.noun}, name at (${Math.round(l.x)}, ${Math.round(l.y)})`);
  }
  console.log(`Layout took ${((Date.now() - started) / 1000).toFixed(1)}s.`);

  if (DRY_RUN) {
    console.log('Dry run: nothing written.');
    return;
  }

  const rows = films.map((f, i) => ({ [MAP.id]: f.id, x: pos[i * 2], y: pos[i * 2 + 1], run_at: runIso }));
  for (let i = 0; i < rows.length; i += WRITE_CHUNK) {
    const { error } = await supabase.from(MAP.layout).upsert(rows.slice(i, i + WRITE_CHUNK), { onConflict: MAP.id });
    if (error) throw error;
  }
  console.log(`Wrote ${rows.length} positions.`);

  const { error: labelError } = await supabase.from(MAP.labels).upsert(
    labels.map((l) => ({
      slug: l.slug,
      title: l.name,
      x: l.x,
      y: l.y,
      sort_order: l.rank,
      members: l.members,
      run_at: runIso,
    })),
    { onConflict: 'slug' },
  );
  if (labelError) throw labelError;

  // The finish copies this run's rows onto the map with upserts and
  // nothing else, so a second call leaves the map as the first did, even
  // one that meets the first still running; one the gateway drops is sent
  // again after GATEWAY_PAUSE_MS.
  let finished = await supabase.rpc(MAP.finish, { p_run: runIso });
  for (let retry = 1; finished.error && isGatewayFailure(finished.status) && retry <= GATEWAY_RETRIES; retry++) {
    console.log(
      `  no answer from the database for ${MAP.finish} (status ${finished.status}) -- ` +
        `sending it again in ${GATEWAY_PAUSE_MS / 1000}s`,
    );
    await sleep(GATEWAY_PAUSE_MS);
    finished = await supabase.rpc(MAP.finish, { p_run: runIso });
  }
  const { data, error } = finished;
  if (error) throw error;
  console.log(`Map updated: ${data} ${MAP.noun}, ${labels.length} mood names.`);
  console.log(`Done in ${((Date.now() - started) / 1000).toFixed(1)}s.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
