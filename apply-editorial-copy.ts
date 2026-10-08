// The descriptions on the database-backed pages: shelves, moods and
// tropes. Kept here so the wording is versioned like everything else, and
// applied by its own workflow whenever this file changes.
//
// Every write has to land on exactly the row it names. A slug or a
// sentence that matches nothing is an error, not a silent skip, and a
// re-run of text that is already in place is fine.

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  db: { schema: 'public' },
});

interface ShelfCopy {
  slug: string;
  title?: string;
  standfirst: string;
}

// Shelves and moods share one table.
const SHELVES: ShelfCopy[] = [
  // Shelves
  {
    slug: 'the-cinnamon-hundred',
    standfirst:
      "A hundred films that feel like nothing else, and are brilliant at it. You won't find most of them on the usual best-of lists. They run oldest to newest, because there's no number one.",
  },
  {
    slug: 'films-that-refuse-to-resolve',
    standfirst:
      "Films that end without telling you what happened, or what it meant. There used to be lots of them. Now they're rare.",
  },
  {
    slug: 'better-than-it-should-be',
    standfirst:
      "Films that sound ordinary on paper and turn out to be great. It's the timing, the cast, the jokes that land: things you only find out by watching.",
  },
  {
    slug: 'scariest-films-ever',
    standfirst:
      "The films that keep the dread going and never let you feel safe. Something is wrong, you can't tell who to trust, and the gore barely matters.",
  },
  {
    slug: 'comforting-and-profound',
    standfirst:
      'Films about something big that are still kind to you. Heavy enough to mean something, gentle enough to sit with, and very few manage both.',
  },
  {
    slug: 'sad-and-funny',
    standfirst:
      "Films that make you cry and laugh, sometimes in the same scene. It's the hardest trick in film, and hardly anyone pulls it off.",
  },
  {
    slug: 'alone-in-the-frame',
    standfirst:
      "Films about being alone that don't rush to fix it. No last-minute friendship, no big reunion, just someone on their own and a film that stays with them.",
  },
  {
    slug: 'strange-but-warm',
    standfirst: 'Odd films that like you. Weird usually comes with a chill. These are weird and warm.',
  },
  {
    slug: 'the-a24-coordinate',
    title: 'Films that feel like A24',
    standfirst:
      'Heavy, thoughtful and a little unsettling. Films that feel like A24 made them, whoever actually did.',
  },
  {
    slug: 'beautiful-and-bleak',
    standfirst:
      'Gorgeous to look at, hard to sit through. The most beautiful films are often not the kind ones.',
  },
  {
    slug: 'dread-without-blood',
    standfirst:
      'All the tension, hardly any gore. Horror that gets under your skin instead of turning your stomach.',
  },
  {
    slug: 'nothing-but-joy',
    standfirst: 'Pure fun, no dark side. Films that want nothing from you except a good time.',
  },
  {
    slug: 'the-long-haul',
    standfirst:
      'The long ones, and worth every minute. Big, serious films that take their time and earn it.',
  },
  {
    slug: 'german-melancholy',
    standfirst:
      'German-language films, the saddest and heaviest cinema of any country. Not light viewing, but often brilliant.',
  },
  {
    slug: 'japanese-strangeness',
    standfirst:
      "Japanese films, the strangest cinema of any country by a long way. Expect dream logic and things you won't see anywhere else.",
  },
  {
    slug: 'nothing-else-is-close',
    standfirst:
      'Films that are like nothing else. Put them next to anything of their kind and nothing comes close.',
  },
  {
    slug: 'the-cinnamon-hundred-television',
    standfirst:
      "A hundred series that feel like nothing else, and are brilliant at it. You won't find most of them on the usual best-of lists.",
  },
  {
    slug: 'television-comfort',
    standfirst:
      'The warmest, cosiest series there are. Shows that feel like company, which films hardly do any more.',
  },
  {
    slug: 'tense-television',
    standfirst: 'Series that run on dread. Every episode ends with you needing the next one.',
  },
  {
    slug: 'nothing-else-is-close-television',
    standfirst:
      'Series that are like nothing else on TV. Television has room to get strange, and these use every bit of it.',
  },

  // Moods
  {
    slug: 'sad-movies',
    standfirst:
      'Films that sit with grief instead of rushing you past it. The saddest come first, not the most famous.',
  },
  {
    slug: 'feel-good-movies',
    standfirst:
      'Warm, hopeful and fun, all at once. The films to put on when you need cheering up.',
  },
  {
    slug: 'funny-movies',
    standfirst: "The funniest films there are, whatever genre they're filed under.",
  },
  {
    slug: 'scary-movies',
    standfirst: 'Films that actually scare you. Dread first, body count second.',
  },
  {
    slug: 'tense-movies',
    standfirst: "Films that won't let you relax, in every genre, not just thrillers.",
  },
  {
    slug: 'mind-bending-movies',
    standfirst:
      'Films that need you awake: clever, strange, and happy to leave you without an answer.',
  },
  {
    slug: 'beautiful-movies',
    standfirst: 'The best-looking films there are. Every frame is worth pausing on.',
  },
  {
    slug: 'dark-movies',
    standfirst: 'Films with no comfort in them. Bleak, not just violent.',
  },
  {
    slug: 'weird-movies',
    standfirst:
      "Films that don't behave. Weird isn't the same as bad: some of these are among the most loved films ever made.",
  },
  {
    slug: 'thoughtful-movies',
    standfirst: 'Slow, serious and worth the attention. Lots of thinking, very little noise.',
  },
  {
    slug: 'sad-tv-shows',
    standfirst: 'Series that stay with grief for a whole run, not just one episode.',
  },
  {
    slug: 'feel-good-tv-shows',
    standfirst: 'Warm, hopeful and fun, all at once. Something TV does better than film.',
  },
  {
    slug: 'funny-tv-shows',
    standfirst: "The funniest series there are, whatever genre they're filed under.",
  },
  {
    slug: 'scary-tv-shows',
    standfirst: 'Slow, creeping dread instead of a jump scare every week.',
  },
  {
    slug: 'tense-tv-shows',
    standfirst: "Series that won't let you relax. You'll say one more episode and watch five.",
  },
  {
    slug: 'mind-bending-tv-shows',
    standfirst:
      'Series that need you awake: clever, strange, and happy to leave you without an answer.',
  },
  {
    slug: 'dark-tv-shows',
    standfirst: 'Series with no comfort in them. Bleak fiction, not true crime.',
  },
  {
    slug: 'weird-tv-shows',
    standfirst:
      "Series that don't behave. Some are loved, some are barely watchable, and none of them are ordinary.",
  },
];

const TROPES: { slug: string; standfirst: string }[] = [
  {
    slug: 'films-that-feel-like-autumn',
    standfirst:
      "Films that feel like autumn: back to school, coats on, everyone home for the holidays. They're warm, a little nostalgic and lovely to look at, mostly about people finding each other, and nothing in them puts you on edge.",
  },
  {
    slug: 'get-matt-damon-home',
    standfirst:
      "In 1998 a whole squad died bringing him home in Saving Private Ryan, and Hollywood never really stopped. Sometimes he's stranded, sometimes he's lost, sometimes he's just somewhere very expensive and has to be fetched. One of these is literally called The Odyssey.",
  },
  {
    slug: 'it-was-a-shit-show',
    standfirst:
      "Every one of these had a shoot that went wrong: a director fired, a set on fire, a star who wouldn't come out of the trailer, a budget that vanished. Films and a few series. They came out anyway, and some of them are great.",
  },
  {
    slug: 'sean-bean-dies',
    standfirst:
      "Spoilers, obviously. He doesn't die in every film, but he dies in enough of them, often at the worst possible moment, that the ones where he survives feel like a twist.",
  },
  {
    slug: 'the-bad-guy-falls-into-something',
    standfirst:
      "The most satisfying way to get rid of a villain: let gravity do it. Off a roof, into lava, into the crocodiles, into their own machine. The hero's hands stay clean and the scenery does the killing.",
  },
  {
    slug: 'the-two-kinds-of-halloween-film',
    standfirst:
      'There are two kinds of Halloween film: the fun ones you put on with the lights on, and the ones you watch with the lights off and regret. Both are strange. Only one is scary.',
  },
];

const TROPE_SECTIONS: { trope: string; from: string; to: string }[] = [
  { trope: 'films-that-feel-like-autumn', from: 'Term starts', to: 'Back to school' },
  { trope: 'films-that-feel-like-autumn', from: 'Coats and pavements', to: 'Coat weather' },
  { trope: 'films-that-feel-like-autumn', from: 'The table', to: 'Home for the holidays' },
];

// Matched on the old line, so the right card changes whatever order the
// list is in.
const TROPE_NOTES: { trope: string; from: string; to: string }[] = [
  {
    trope: 'films-that-feel-like-autumn',
    from: 'An English house going quiet. Measures as the saddest thing on this list.',
    to: 'An English house going quiet. The saddest film on this list.',
  },
  {
    trope: 'films-that-feel-like-autumn',
    from: "A boy in a wolf suit. Measures lonelier than a children's film has any right to.",
    to: "A boy in a wolf suit, and lonelier than a children's film has any right to be.",
  },
  {
    trope: 'the-bad-guy-falls-into-something',
    from: 'The T-1000 into the steel. The thing the film is remembered for, and the data has no record of it at all.',
    to: 'The T-1000 into the molten steel, the moment everyone remembers.',
  },
  {
    trope: 'the-two-kinds-of-halloween-film',
    from: 'The sunken place. Measures as paranoia on every reading, and is right to.',
    to: 'The sunken place. Paranoid from the first scene, and right to be.',
  },
  {
    trope: 'the-two-kinds-of-halloween-film',
    from: 'A gothic castle above a pastel suburb, and the kindest monster in the catalogue.',
    to: 'A gothic castle above a pastel suburb, and the kindest monster in the movies.',
  },
];

async function updateOne(
  table: string,
  values: Record<string, string>,
  match: Record<string, string>,
  label: string,
): Promise<number> {
  let q = supabase.from(table).update(values);
  for (const [k, v] of Object.entries(match)) q = q.eq(k, v);
  const { data, error } = await q.select();
  if (error) throw new Error(`${label}: ${error.message}`);
  return (data ?? []).length;
}

async function exists(table: string, match: Record<string, string>): Promise<boolean> {
  let q = supabase.from(table).select('*', { count: 'exact', head: true });
  for (const [k, v] of Object.entries(match)) q = q.eq(k, v);
  const { count, error } = await q;
  if (error) throw new Error(`${table}: ${error.message}`);
  return (count ?? 0) > 0;
}

async function main() {
  for (const s of SHELVES) {
    const values: Record<string, string> = { standfirst: s.standfirst };
    if (s.title) values.title = s.title;
    const n = await updateOne('shelves', values, { slug: s.slug }, `shelf ${s.slug}`);
    if (n !== 1) throw new Error(`shelf ${s.slug}: matched ${n} rows`);
  }
  console.log(`${SHELVES.length} shelves and moods`);

  for (const t of TROPES) {
    const n = await updateOne('tropes', { standfirst: t.standfirst }, { slug: t.slug }, `trope ${t.slug}`);
    if (n !== 1) throw new Error(`trope ${t.slug}: matched ${n} rows`);
  }
  console.log(`${TROPES.length} tropes`);

  for (const s of TROPE_SECTIONS) {
    const n = await updateOne(
      'trope_films',
      { bucket: s.to },
      { trope_slug: s.trope, bucket: s.from },
      `section ${s.from}`,
    );
    if (n === 0 && !(await exists('trope_films', { trope_slug: s.trope, bucket: s.to }))) {
      throw new Error(`section "${s.from}" in ${s.trope}: nothing to rename`);
    }
  }
  console.log(`${TROPE_SECTIONS.length} sections`);

  for (const c of TROPE_NOTES) {
    const n = await updateOne('trope_films', { note: c.to }, { trope_slug: c.trope, note: c.from }, `note in ${c.trope}`);
    if (n > 1) throw new Error(`note in ${c.trope}: matched ${n} rows`);
    if (n === 0 && !(await exists('trope_films', { trope_slug: c.trope, note: c.to }))) {
      throw new Error(`note in ${c.trope}: no card says "${c.from}"`);
    }
  }
  console.log(`${TROPE_NOTES.length} film notes`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
