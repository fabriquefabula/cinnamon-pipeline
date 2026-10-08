// Empties both recurrence penalty tables, the state they were in before
// 5 October. The 7 October revert restored the rail functions but not
// this data, so every rail recomputed since has still been down-weighting
// the titles in these tables. Nothing in the pipeline fills them any
// more, so once they are empty they stay empty.
//
// Runs by itself when this file or its workflow is pushed. Safe to run
// again: deleting from an empty table does nothing.

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  db: { schema: 'public' },
});

async function clear(table: string, idColumn: string): Promise<void> {
  // The API refuses a DELETE without a filter, so this one matches every
  // row instead of none.
  const { error } = await supabase.from(table).delete().not(idColumn, 'is', null);
  if (error) throw new Error(`${table}: ${error.message}`);

  const { count, error: countError } = await supabase
    .from(table)
    .select('*', { count: 'exact', head: true });
  if (countError) throw new Error(`${table}: ${countError.message}`);
  if (count !== 0) throw new Error(`${table} still has ${count} rows`);
  console.log(`${table}: empty`);
}

async function main() {
  await clear('movie_recurrence_penalty', 'movie_id');
  await clear('tv_recurrence_penalty', 'show_id');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
