import { randomUUID } from 'node:crypto';
import { rmSync } from 'node:fs';
import { afterEach, describe, expect, it } from 'vitest';
import { foldBatch, minuteBucket } from '../src/store/schema.js';
import { SqliteStore } from '../src/store/sqlite.js';
import type { EventRecord } from '../src/types.js';

const dbs: string[] = [];

function store(): SqliteStore {
  const path = `test-${randomUUID()}.db`;
  dbs.push(path);
  return new SqliteStore(path);
}

function event(overrides: Partial<EventRecord> = {}): EventRecord {
  return {
    eventId: randomUUID(),
    tenantId: 'test',
    type: 'page_view',
    value: 1,
    occurredAt: '2026-09-20T10:15:30.000Z',
    ...overrides,
  };
}

afterEach(() => {
  for (const path of dbs.splice(0)) {
    for (const suffix of ['', '-wal', '-shm']) {
      rmSync(`${path}${suffix}`, { force: true });
    }
  }
});

describe('minuteBucket', () => {
  it('truncates to the minute the event occurred', () => {
    expect(minuteBucket('2026-09-20T10:15:59.999Z')).toBe('2026-09-20T10:15:00.000Z');
  });

  it('rejects an unparseable timestamp rather than bucketing into NaN', () => {
    expect(() => minuteBucket('not-a-date')).toThrow(/invalid occurredAt/);
  });
});

describe('foldBatch', () => {
  it('collapses a batch to one row per bucket and type', () => {
    const folded = foldBatch([
      { occurredAt: '2026-09-20T10:15:10.000Z', type: 'a', value: 2 },
      { occurredAt: '2026-09-20T10:15:50.000Z', type: 'a', value: 3 },
      { occurredAt: '2026-09-20T10:16:01.000Z', type: 'a', value: 5 },
      { occurredAt: '2026-09-20T10:15:20.000Z', type: 'b', value: 7 },
    ]);
    expect(folded).toHaveLength(3);
    const first = folded.find(
      (row) => row.bucket === '2026-09-20T10:15:00.000Z' && row.type === 'a',
    );
    expect(first).toMatchObject({ count: 2, total: 5 });
  });
});

describe('SqliteStore.applyBatch', () => {
  it('counts each event once and sums its value', async () => {
    const subject = store();
    await subject.init();
    const result = await subject.applyBatch([event({ value: 4 }), event({ value: 6 })]);
    expect(result).toEqual({ inserted: 2, duplicates: 0 });
    expect(await subject.totals()).toMatchObject({ events: 2, value: 10 });
    await subject.close();
  });

  it('ignores a repeat of an event id it has already seen', async () => {
    const subject = store();
    await subject.init();
    const one = event({ value: 5 });
    await subject.applyBatch([one]);
    const second = await subject.applyBatch([one]);
    expect(second).toEqual({ inserted: 0, duplicates: 1 });
    expect(await subject.totals()).toMatchObject({ events: 1, value: 5 });
    await subject.close();
  });

  it('collapses duplicates that arrive inside a single batch', async () => {
    const subject = store();
    await subject.init();
    const one = event({ value: 3 });
    const result = await subject.applyBatch([one, one, one]);
    expect(result).toEqual({ inserted: 1, duplicates: 2 });
    expect(await subject.totals()).toMatchObject({ events: 1, value: 3 });
    await subject.close();
  });

  it('leaves nothing behind when the transaction fails', async () => {
    const subject = store();
    await subject.init();
    await subject.applyBatch([event()]);
    // A bad timestamp makes foldBatch throw after ids were already claimed in this
    // transaction. If claiming were not rolled back with the rollup, those ids would be
    // permanently burned and the events silently uncountable on redelivery.
    await expect(subject.applyBatch([event(), event({ occurredAt: 'garbage' })])).rejects.toThrow();
    expect(await subject.totals()).toMatchObject({ events: 1 });
    // Proof the claim rolled back: the same events apply cleanly on the retry.
    const retry = await subject.applyBatch([event(), event()]);
    expect(retry.inserted).toBe(2);
    await subject.close();
  });

  it('buckets by occurrence time, not arrival time', async () => {
    const subject = store();
    await subject.init();
    await subject.applyBatch([
      event({ occurredAt: '2026-09-20T10:15:01.000Z' }),
      event({ occurredAt: '2026-09-20T10:16:01.000Z' }),
    ]);
    const buckets = await subject.timeseries(60 * 24 * 365 * 10);
    expect(buckets.map((row) => row.bucket).sort()).toEqual([
      '2026-09-20T10:15:00.000Z',
      '2026-09-20T10:16:00.000Z',
    ]);
    await subject.close();
  });
});
