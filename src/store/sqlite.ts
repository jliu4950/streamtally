import { DatabaseSync } from 'node:sqlite';
import type { ApplyResult, Bucket, EventRecord, Store, Totals } from '../types.js';
import { DDL, foldBatch } from './schema.js';

const UPSERT = `
  INSERT INTO rollups (bucket, type, count, total) VALUES (?, ?, ?, ?)
  ON CONFLICT (bucket, type) DO UPDATE
  SET count = rollups.count + excluded.count,
      total = rollups.total + excluded.total`;

/**
 * Zero-dependency store built on Node's own SQLite. This is what makes `npm run demo` work
 * on a machine with no Docker: same SQL, same guarantees, no infrastructure.
 */
export class SqliteStore implements Store {
  private readonly db: DatabaseSync;

  constructor(path: string) {
    this.db = new DatabaseSync(path);
  }

  async init(): Promise<void> {
    // WAL lets the HTTP read path query while the aggregator is writing.
    this.db.exec('PRAGMA journal_mode = WAL');
    this.db.exec('PRAGMA synchronous = NORMAL');
    for (const statement of DDL) this.db.exec(statement);
  }

  async applyBatch(events: EventRecord[]): Promise<ApplyResult> {
    if (events.length === 0) return { inserted: 0, duplicates: 0 };

    // Collapse duplicates inside the batch first. Leaving them for the database would make
    // the multi-row upsert ambiguous, and it costs one pass to remove them here.
    const unique = new Map<string, EventRecord>();
    let duplicates = 0;
    for (const event of events) {
      if (unique.has(event.eventId)) duplicates += 1;
      else unique.set(event.eventId, event);
    }

    const claim = this.db.prepare(
      'INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT DO NOTHING',
    );
    const upsert = this.db.prepare(UPSERT);

    // One transaction for the whole batch. Claiming an event id and counting it must commit
    // together: if they could not, a crash between them would either lose the event or let a
    // redelivery count it twice, and the exactness harness exists to catch exactly that.
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const fresh: EventRecord[] = [];
      for (const event of unique.values()) {
        if (claim.run(event.eventId).changes === 1) fresh.push(event);
        else duplicates += 1;
      }
      for (const rollup of foldBatch(fresh)) {
        upsert.run(rollup.bucket, rollup.type, rollup.count, rollup.total);
      }
      this.db.exec('COMMIT');
      return { inserted: fresh.length, duplicates };
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  async timeseries(sinceMinutes: number): Promise<Bucket[]> {
    const floor = new Date(Date.now() - sinceMinutes * 60_000).toISOString();
    const rows = this.db
      .prepare('SELECT bucket, type, count, total FROM rollups WHERE bucket >= ? ORDER BY bucket')
      .all(floor) as Array<Record<string, unknown>>;
    return rows.map((row) => ({
      bucket: String(row.bucket),
      type: String(row.type),
      count: Number(row.count),
      total: Number(row.total),
    }));
  }

  async topTypes(limit: number): Promise<Array<{ type: string; count: number; total: number }>> {
    const rows = this.db
      .prepare(
        'SELECT type, SUM(count) AS count, SUM(total) AS total FROM rollups GROUP BY type ORDER BY count DESC LIMIT ?',
      )
      .all(limit) as Array<Record<string, unknown>>;
    return rows.map((row) => ({
      type: String(row.type),
      count: Number(row.count),
      total: Number(row.total),
    }));
  }

  async totals(): Promise<Totals> {
    const row = this.db
      .prepare(
        'SELECT COALESCE(SUM(count), 0) AS events, COALESCE(SUM(total), 0) AS value, COUNT(*) AS buckets FROM rollups',
      )
      .get() as Record<string, unknown>;
    return {
      events: Number(row.events),
      value: Number(row.value),
      buckets: Number(row.buckets),
    };
  }

  async reset(): Promise<void> {
    this.db.exec('DELETE FROM rollups');
    this.db.exec('DELETE FROM processed_events');
  }

  async close(): Promise<void> {
    this.db.close();
  }
}
