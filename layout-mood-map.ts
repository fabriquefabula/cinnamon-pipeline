// The mood map: every well-known film laid out flat by how it feels, so
// films that feel alike sit together and one feeling shades into the next
// across the map. The site draws it at /map.
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
// The layout is UMAP's (McInnes, Healy & Melville 2018), written out here
// rather than taken from a package, for two reasons. It starts from the
// films' two strongest directions of difference (principal components)
// instead of from random positions, so the big picture -- light against
// dark, warm against cold -- holds together instead of being torn into
// pieces. And it is seeded, so a week in which nothing changed gives the
// same map. Everything else (neighbourhoods, edge weights, the
// optimisation and its schedule) follows the reference implementation,
// umap-learn, step for step.
//
// Distances are plain Euclidean on essence_vector_z, the same reading of
// how alike two films feel that the film-to-film paths use.
//
// Required env vars: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Optional: DRY_RUN=true (lay out and report, write nothing)

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = requireEnv('SUPABASE_URL');
const SUPABASE_SERVICE_ROLE_KEY = requireEnv('SUPABASE_SERVICE_ROLE_KEY');
const DRY_RUN = process.env.DRY_RUN === 'true';

// How many films around each film make up its neighbourhood, counting the
// film itself, as umap-learn does. More pulls the map towards the big
// picture, fewer towards fine detail.
const N_NEIGHBORS = 15;
// How closely films that feel the same may be packed. Near 0 they pile on
// top of one another; higher spreads them out evenly enough that each one
// can be picked out and tapped on a phone.
const MIN_DIST = 0.25;
const SPREAD = 1.0;
const N_EPOCHS = 500; // umap-learn's choice for fewer than 10,000 points
const NEGATIVE_SAMPLE_RATE = 5;
const REPULSION = 1.0;
const LEARNING_RATE = 1.0;
const SEED = 1979;

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

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

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
  const readings = await fetchPaged<{ id: string; v: unknown }>('mood_map_input');
  const about = await fetchPaged<{ id: string; darkness: number | null; warmth: number | null; moods: string[] | null }>(
    'mood_map_film_moods',
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
    console.log(`Left out ${readings.length - kept.length} films with an unreadable or mismatched reading.`);
  }
  return kept;
}

async function fetchMoods(): Promise<Mood[]> {
  const { data, error } = await supabase
    .from('shelves')
    .select('slug, title, sort_order')
    .eq('kind', 'mood')
    .eq('medium', 'movie')
    .order('sort_order', { ascending: true });
  if (error) throw error;
  return ((data ?? []) as { slug: string; title: string }[]).map((s) => ({
    slug: s.slug,
    // "Sad movies" on the mood page, "Sad" on the map.
    name: s.title.replace(/\s+movies$/i, ''),
  }));
}

// ---------------------------------------------------------------------------
// Seeded randomness (mulberry32)

function makeRandom(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function gaussian(random: () => number): number {
  let u = 0;
  while (u === 0) u = random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * random());
}

// ---------------------------------------------------------------------------
// Neighbourhoods

interface Neighbours {
  k: number; // neighbours per film, not counting the film itself
  idx: Int32Array; // n * k, nearest first
  dist: Float64Array; // n * k
}

// Exact nearest neighbours. A few thousand films is small enough to
// compare every pair, so nothing is approximated.
function nearestNeighbours(x: Float64Array[], k: number): Neighbours {
  const n = x.length;
  const dims = x[0].length;
  const idx = new Int32Array(n * k).fill(-1);
  const dist = new Float64Array(n * k).fill(Infinity);
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
      if (s >= dist[last]) continue;
      let p = last;
      while (p > base && dist[p - 1] > s) {
        dist[p] = dist[p - 1];
        idx[p] = idx[p - 1];
        p--;
      }
      dist[p] = s;
      idx[p] = j;
    }
    for (let p = base; p <= last; p++) dist[p] = Math.sqrt(dist[p]);
  }
  return { k, idx, dist };
}

const SMOOTH_K_TOLERANCE = 1e-5;
const MIN_K_DIST_SCALE = 1e-3;

// umap-learn's smooth_knn_dist + compute_membership_strengths: how strongly
// each film belongs with each of its neighbours, measured against how far
// away that film's own neighbours are, so films in sparse corners of the
// catalogue get neighbourhoods just as real as those in crowded ones.
function memberships(nn: Neighbours, n: number): Float64Array {
  const { k, dist } = nn;
  const target = Math.log2(k + 1);
  let meanAll = 0;
  for (let p = 0; p < n * k; p++) meanAll += dist[p];
  meanAll /= n * k;

  const w = new Float64Array(n * k);
  for (let i = 0; i < n; i++) {
    const base = i * k;
    let rho = 0;
    for (let p = base; p < base + k; p++) {
      if (dist[p] > 0) {
        rho = dist[p];
        break;
      }
    }
    let lo = 0;
    let hi = Infinity;
    let mid = 1;
    for (let iter = 0; iter < 64; iter++) {
      let psum = 0;
      for (let p = base; p < base + k; p++) {
        const d = dist[p] - rho;
        psum += d > 0 ? Math.exp(-(d / mid)) : 1;
      }
      if (Math.abs(psum - target) < SMOOTH_K_TOLERANCE) break;
      if (psum > target) {
        hi = mid;
        mid = (lo + hi) / 2;
      } else {
        lo = mid;
        mid = hi === Infinity ? mid * 2 : (lo + hi) / 2;
      }
    }
    let sigma = mid;
    if (rho > 0) {
      let mean = 0;
      for (let p = base; p < base + k; p++) mean += dist[p];
      mean /= k;
      if (sigma < MIN_K_DIST_SCALE * mean) sigma = MIN_K_DIST_SCALE * mean;
    } else if (sigma < MIN_K_DIST_SCALE * meanAll) {
      sigma = MIN_K_DIST_SCALE * meanAll;
    }
    for (let p = base; p < base + k; p++) {
      const d = dist[p] - rho;
      w[p] = d <= 0 || sigma === 0 ? 1 : Math.exp(-(d / sigma));
    }
  }
  return w;
}

interface Edges {
  head: Int32Array;
  tail: Int32Array;
  weight: Float64Array;
}

// The neighbourhood graph made symmetric with a fuzzy union (a + b - ab),
// both directions of every pair kept, and edges too weak to be sampled
// even once in the whole run dropped -- all as umap-learn does.
function fuzzyUnion(nn: Neighbours, w: Float64Array, n: number): Edges {
  const { k, idx } = nn;
  const directed = new Map<number, number>();
  for (let i = 0; i < n; i++) {
    for (let p = i * k; p < i * k + k; p++) {
      const j = idx[p];
      if (j < 0 || j === i) continue;
      directed.set(i * n + j, w[p]);
    }
  }
  const pairs = new Map<number, number>();
  for (const [key, a] of directed) {
    const i = Math.floor(key / n);
    const j = key - i * n;
    const pk = Math.min(i, j) * n + Math.max(i, j);
    if (pairs.has(pk)) continue;
    const b = directed.get(j * n + i) ?? 0;
    pairs.set(pk, a + b - a * b);
  }
  let max = 0;
  for (const v of pairs.values()) if (v > max) max = v;
  const floor = max / N_EPOCHS;

  const keys = [...pairs.keys()].sort((p, q) => p - q);
  const head: number[] = [];
  const tail: number[] = [];
  const weight: number[] = [];
  for (const pk of keys) {
    const v = pairs.get(pk)!;
    if (v < floor) continue;
    const i = Math.floor(pk / n);
    const j = pk - i * n;
    head.push(i, j);
    tail.push(j, i);
    weight.push(v, v);
  }
  return { head: Int32Array.from(head), tail: Int32Array.from(tail), weight: Float64Array.from(weight) };
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

// umap-learn's init="pca": principal components, scaled so the furthest
// coordinate is 10, a whisper of noise to separate exact twins, then each
// axis stretched to 0-10.
function pcaStart(x: Float64Array[], random: () => number): Float64Array {
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
  let maxAbs = 0;
  for (let i = 0; i < n; i++) {
    for (let c = 0; c < 2; c++) {
      let s = 0;
      for (let d = 0; d < dims; d++) s += (x[i][d] - mean[d]) * axes[c][d];
      y[i * 2 + c] = s;
      if (Math.abs(s) > maxAbs) maxAbs = Math.abs(s);
    }
  }
  const expand = maxAbs > 0 ? 10 / maxAbs : 1;
  for (let p = 0; p < n * 2; p++) y[p] = y[p] * expand + gaussian(random) * 1e-4;
  for (let c = 0; c < 2; c++) {
    let lo = Infinity;
    let hi = -Infinity;
    for (let i = 0; i < n; i++) {
      lo = Math.min(lo, y[i * 2 + c]);
      hi = Math.max(hi, y[i * 2 + c]);
    }
    const span = hi - lo || 1;
    for (let i = 0; i < n; i++) y[i * 2 + c] = (10 * (y[i * 2 + c] - lo)) / span;
  }
  return y;
}

// ---------------------------------------------------------------------------
// Optimisation

// The curve 1 / (1 + a d^2b) that stands in for "how close two films sit
// on the map", fitted to MIN_DIST and SPREAD as umap-learn's find_ab_params
// does (least squares over 300 points out to 3 x SPREAD), here by a grid
// that narrows in on the best fit.
function fitCurve(spread: number, minDist: number): { a: number; b: number } {
  const xs: number[] = [];
  const ys: number[] = [];
  for (let i = 0; i < 300; i++) {
    const d = (spread * 3 * i) / 299;
    xs.push(d);
    ys.push(d < minDist ? 1 : Math.exp(-(d - minDist) / spread));
  }
  const err = (a: number, b: number) => {
    let s = 0;
    for (let i = 0; i < xs.length; i++) {
      const f = 1 / (1 + a * Math.pow(xs[i], 2 * b));
      s += (f - ys[i]) * (f - ys[i]);
    }
    return s;
  };
  let best = { a: 1, b: 1, e: err(1, 1) };
  let la0 = Math.log(1e-3);
  let la1 = Math.log(1e3);
  let b0 = 0.05;
  let b1 = 4;
  const steps = 80;
  for (let round = 0; round < 8; round++) {
    for (let ia = 0; ia <= steps; ia++) {
      const a = Math.exp(la0 + ((la1 - la0) * ia) / steps);
      for (let ib = 0; ib <= steps; ib++) {
        const b = b0 + ((b1 - b0) * ib) / steps;
        const e = err(a, b);
        if (e < best.e) best = { a, b, e };
      }
    }
    const dla = ((la1 - la0) / steps) * 2;
    const db = ((b1 - b0) / steps) * 2;
    la0 = Math.log(best.a) - dla;
    la1 = Math.log(best.a) + dla;
    b0 = Math.max(1e-3, best.b - db);
    b1 = best.b + db;
  }
  return { a: best.a, b: best.b };
}

function clip(v: number): number {
  return v > 4 ? 4 : v < -4 ? -4 : v;
}

// umap-learn's optimize_layout_euclidean, two dimensions, moving both ends
// of every sampled edge.
function optimise(y: Float64Array, edges: Edges, n: number, a: number, b: number, random: () => number): void {
  const m = edges.weight.length;
  let max = 0;
  for (let e = 0; e < m; e++) if (edges.weight[e] > max) max = edges.weight[e];
  const epochsPerSample = new Float64Array(m);
  for (let e = 0; e < m; e++) epochsPerSample[e] = max / edges.weight[e];
  const epochsPerNegative = new Float64Array(m);
  for (let e = 0; e < m; e++) epochsPerNegative[e] = epochsPerSample[e] / NEGATIVE_SAMPLE_RATE;
  const nextSample = Float64Array.from(epochsPerSample);
  const nextNegative = Float64Array.from(epochsPerNegative);

  for (let epoch = 0; epoch < N_EPOCHS; epoch++) {
    const alpha = LEARNING_RATE * (1 - epoch / N_EPOCHS);
    for (let e = 0; e < m; e++) {
      if (nextSample[e] > epoch) continue;
      const i = edges.head[e];
      const j = edges.tail[e];
      const ix = i * 2;
      const jx = j * 2;

      let dx = y[ix] - y[jx];
      let dy = y[ix + 1] - y[jx + 1];
      let d2 = dx * dx + dy * dy;
      if (d2 > 0) {
        const coeff = (-2 * a * b * Math.pow(d2, b - 1)) / (a * Math.pow(d2, b) + 1);
        const gx = clip(coeff * dx);
        const gy = clip(coeff * dy);
        y[ix] += gx * alpha;
        y[ix + 1] += gy * alpha;
        y[jx] -= gx * alpha;
        y[jx + 1] -= gy * alpha;
      }
      nextSample[e] += epochsPerSample[e];

      const negatives = Math.floor((epoch - nextNegative[e]) / epochsPerNegative[e]);
      for (let s = 0; s < negatives; s++) {
        const o = Math.floor(random() * n);
        if (o === i) continue;
        const ox = o * 2;
        dx = y[ix] - y[ox];
        dy = y[ix + 1] - y[ox + 1];
        d2 = dx * dx + dy * dy;
        if (d2 > 0) {
          const coeff = (2 * REPULSION * b) / ((0.001 + d2) * (a * Math.pow(d2, b) + 1));
          y[ix] += clip(coeff * dx) * alpha;
          y[ix + 1] += clip(coeff * dy) * alpha;
        } else {
          y[ix] += 4 * alpha;
          y[ix + 1] += 4 * alpha;
        }
      }
      nextNegative[e] += negatives * epochsPerNegative[e];
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
  console.log(`Laying out the mood map${DRY_RUN ? ' (DRY RUN)' : ''}...`);

  const [films, moods] = await Promise.all([fetchFilms(), fetchMoods()]);
  const n = films.length;
  if (n < N_NEIGHBORS * 4) throw new Error(`Only ${n} films to lay out -- too few for a map.`);
  console.log(`${n} films, ${films[0].v.length} dimensions, ${moods.length} moods.`);

  const x = films.map((f) => Float64Array.from(f.v));
  const random = makeRandom(SEED);

  const nn = nearestNeighbours(x, N_NEIGHBORS - 1);
  const w = memberships(nn, n);
  const edges = fuzzyUnion(nn, w, n);
  console.log(`Neighbourhood graph: ${edges.weight.length / 2} pairs.`);

  const { a, b } = fitCurve(SPREAD, MIN_DIST);
  console.log(`Curve: a=${a.toFixed(4)} b=${b.toFixed(4)}.`);

  const start = pcaStart(x, random);
  optimise(start, edges, n, a, b, random);
  for (let p = 0; p < n * 2; p++) {
    if (!Number.isFinite(start[p])) throw new Error('The layout came out with a missing position.');
  }
  const pos = orient(start, films);

  const kept = keptNeighbours(pos, nn, n, 10);
  console.log(`On the map, ${(kept * 100).toFixed(1)}% of each film's 10 closest films are among the 10 it feels most like.`);

  const labels = placeLabels(pos, films, moods);
  for (const l of labels) {
    console.log(`  ${l.name}: ${l.members} films, name at (${Math.round(l.x)}, ${Math.round(l.y)})`);
  }
  console.log(`Layout took ${((Date.now() - started) / 1000).toFixed(1)}s.`);

  if (DRY_RUN) {
    console.log('Dry run: nothing written.');
    return;
  }

  const rows = films.map((f, i) => ({ film_id: f.id, x: pos[i * 2], y: pos[i * 2 + 1], run_at: runIso }));
  for (let i = 0; i < rows.length; i += WRITE_CHUNK) {
    const { error } = await supabase.from('mood_map_layout').upsert(rows.slice(i, i + WRITE_CHUNK), { onConflict: 'film_id' });
    if (error) throw error;
  }
  console.log(`Wrote ${rows.length} positions.`);

  const { error: labelError } = await supabase.from('mood_map_label_layout').upsert(
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

  const { data, error } = await supabase.rpc('mood_map_finish', { p_run: runIso });
  if (error) throw error;
  console.log(`Map updated: ${data} films, ${labels.length} mood names.`);
  console.log(`Done in ${((Date.now() - started) / 1000).toFixed(1)}s.`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
