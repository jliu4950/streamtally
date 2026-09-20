import pg from 'pg';
import type { ApplyResult, Bucket, EventRecord, Store, Totals } from '../types.js';
import { DDL, foldBatch } from './schema.js';

/**
 * `count` is BIGINT, which node-postgres returns as a string to avoid silent precision loss.
 * Every read below goes through Number() deliberately rather than relying on that default.
 */
const CLAIM = `
  INSERT INTO processed_events (event_id)
  SELECT unnest($1::text[])
  ON CONFLICT DO NOTHING
  RETURNING event_id`;

const UPSERT = `
  INSERT INTO rollups (bucket, type, count, total)
  SELECT * FROM unnest($1::text[], $2::text[], $3::bigint[], $4::double precision[])
  ON CONFLICT (bucket, type) DO UPDATE
  SET count = rollups.count + EXCLUDED.count,
      total = rollups.total + EXCLUDED.total`;

export class PostgresStore implements Store {
  private readonly pool: pg.Pool;

  constructor(connectionString: string) {
    this.pool = new pg.Pool({ connectionString, max: 8 });
  }

  async init(): Promise<void> {
    for (const statement of DDL) await this.pool.query(statement);
  }

  async applyBatch(events: EventRecord[]): Promise<ApplyResult> {
    if (events.length === 0) return { inserted: 0, duplicates: 0 };

    const unique = new Map<string, EventRecord>();
    let duplicates = 0;
    for (const event of events) {
      if (unique.has(event.eventId)) duplicates += 1;
      else unique.set(event.eventId, event);
    }

    const client = await this.pool.connect();
    try {
      // Same contract as the SQLite store: claiming ids and counting them commit together.
      await client.query('BEGIN');
      const claimed = await client.query<{ event_id: string }>(CLAIM, [[...unique.keys()]]);
      const freshIds = new Set(claimed.rows.map((row) => row.event_id));
      duplicates += unique.size - freshIds.size;

      const fresh = [...unique.values()].filter((event) => freshIds.has(event.eventId));
      const rollups = foldBatch(fresh);
      if (rollups.length > 0) {
        await client.query(UPSERT, [
          rollups.map((rollup) => rollup.bucket),
          rollups.map((rollup) => rollup.type),
          rollups.map((rollup) => rollup.count),
          rollups.map((rollup) => rollup.total),
        ]);
      }
      await client.query('COMMIT');
      return { inserted: fresh.length, duplicates };
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async timeseries(sinceMinutes: number): Promise<Bucket[]> {
    const floor = new Date(Date.now() - sinceMinutes * 60_000).toISOString();
    const result = await this.pool.query(
      'SELECT bucket, type, count, total FROM rollups WHERE bucket >= $1 ORDER BY bucket',
      [floor],
    );
    return result.rows.map((row) => ({
      bucket: String(row.bucket),
      type: String(row.type),
      count: Number(row.count),
      total: Number(row.total),
    }));
  }

  async topTypes(limit: number): Promise<Array<{ type: string; count: number; total: number }>> {
    const result = await this.pool.query(
      'SELECT type, SUM(count) AS count, SUM(total) AS total FROM rollups GROUP BY type ORDER BY count DESC LIMIT $1',
      [limit],
    );
    return result.rows.map((row) => ({
      type: String(row.type),
      count: Number(row.count),
      total: Number(row.total),
    }));
  }

  async totals(): Promise<Totals> {
    const result = await this.pool.query(
      'SELECT COALESCE(SUM(count), 0) AS events, COALESCE(SUM(total), 0) AS value, COUNT(*) AS buckets FROM rollups',
    );
    const row = result.rows[0] ?? { events: 0, value: 0, buckets: 0 };
    return { events: Number(row.events), value: Number(row.value), buckets: Number(row.buckets) };
  }

  async reset(): Promise<void> {
    await this.pool.query('TRUNCATE rollups, processed_events');
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
