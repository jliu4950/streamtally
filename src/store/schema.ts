/** Kept identical across both backends: SQLite and Postgres share this exact DDL and upsert. */
export const DDL = [
  `CREATE TABLE IF NOT EXISTS processed_events (
     event_id TEXT PRIMARY KEY
   )`,
  `CREATE TABLE IF NOT EXISTS rollups (
     bucket TEXT NOT NULL,
     type   TEXT NOT NULL,
     count  BIGINT NOT NULL,
     total  DOUBLE PRECISION NOT NULL,
     PRIMARY KEY (bucket, type)
   )`,
  'CREATE INDEX IF NOT EXISTS rollups_bucket_idx ON rollups (bucket)',
];

/** Truncate an ISO timestamp to its minute, e.g. 2026-09-20T14:35:00.000Z. */
export function minuteBucket(occurredAt: string): string {
  const at = new Date(occurredAt);
  if (Number.isNaN(at.getTime())) throw new Error(`invalid occurredAt: ${occurredAt}`);
  at.setUTCSeconds(0, 0);
  return at.toISOString();
}

export interface FoldedRollup {
  bucket: string;
  type: string;
  count: number;
  total: number;
}

/**
 * Collapse a batch into one row per (bucket, type) before touching the database.
 *
 * A 500-event batch usually spans a handful of buckets and types, so this turns 500 upserts
 * into a handful. It is the single biggest win in the aggregator's hot path, and it is only
 * safe because the rollup upsert is associative: folding in memory and folding in SQL produce
 * the same totals.
 */
export function foldBatch(events: readonly FoldedInput[]): FoldedRollup[] {
  const folded = new Map<string, FoldedRollup>();
  for (const event of events) {
    const bucket = minuteBucket(event.occurredAt);
    const key = JSON.stringify([bucket, event.type]);
    const current = folded.get(key);
    if (current) {
      current.count += 1;
      current.total += event.value;
    } else {
      folded.set(key, { bucket, type: event.type, count: 1, total: event.value });
    }
  }
  return [...folded.values()];
}

export interface FoldedInput {
  occurredAt: string;
  type: string;
  value: number;
}
