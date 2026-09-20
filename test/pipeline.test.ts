import { randomUUID } from 'node:crypto';
import { rmSync } from 'node:fs';
import { afterEach, describe, expect, it } from 'vitest';
import { MemoryBus } from '../src/bus/memory.js';
import { Aggregator } from '../src/pipeline/aggregator.js';
import { buildServer } from '../src/server.js';
import { SqliteStore } from '../src/store/sqlite.js';
import type { EventRecord } from '../src/types.js';

const dbs: string[] = [];

async function harness() {
  const path = `test-${randomUUID()}.db`;
  dbs.push(path);
  const store = new SqliteStore(path);
  const bus = new MemoryBus(100, 5);
  await store.init();
  await bus.start();
  const aggregator = new Aggregator(bus, store);
  await aggregator.start(50);
  return { store, bus, aggregator };
}

function events(count: number, value = 1): EventRecord[] {
  return Array.from({ length: count }, (_, index) => ({
    eventId: randomUUID(),
    tenantId: 'test',
    type: index % 2 === 0 ? 'page_view' : 'checkout',
    value,
    occurredAt: new Date().toISOString(),
  }));
}

afterEach(() => {
  for (const path of dbs.splice(0)) {
    for (const suffix of ['', '-wal', '-shm']) rmSync(`${path}${suffix}`, { force: true });
  }
});

describe('end to end', () => {
  it('aggregates published events into rollups', async () => {
    const { store, bus, aggregator } = await harness();
    await bus.publish(events(250, 2));
    await bus.drain();
    const totals = await store.totals();
    expect(totals.events).toBe(250);
    expect(totals.value).toBe(500);
    await aggregator.stop();
    await bus.close();
    await store.close();
  });

  it('accepts events over HTTP and counts a retried request once', async () => {
    const { store, bus, aggregator } = await harness();
    const app = buildServer({ bus, store, describe: () => ({ bus: 'memory', store: 'sqlite' }) });
    await app.ready();

    const body = { eventId: 'stable-key-1', type: 'checkout', value: 9 };
    const first = await app.inject({ method: 'POST', url: '/ingest', payload: body });
    const second = await app.inject({ method: 'POST', url: '/ingest', payload: body });
    expect(first.statusCode).toBe(202);
    expect(second.statusCode).toBe(202);

    await bus.drain();
    // The client sent it twice; a caller-supplied event id makes the retry free.
    expect(await store.totals()).toMatchObject({ events: 1, value: 9 });

    await app.close();
    await aggregator.stop();
    await bus.close();
    await store.close();
  });

  it('rejects a malformed event with 400 rather than poisoning the stream', async () => {
    const { store, bus, aggregator } = await harness();
    const app = buildServer({ bus, store, describe: () => ({ bus: 'memory', store: 'sqlite' }) });
    await app.ready();
    const response = await app.inject({
      method: 'POST',
      url: '/ingest',
      payload: { type: '', value: 'not-a-number' },
    });
    expect(response.statusCode).toBe(400);
    expect(response.json().error).toBe('invalid_event');
    await app.close();
    await aggregator.stop();
    await bus.close();
    await store.close();
  });

  it('exposes prometheus metrics including consumer lag', async () => {
    const { store, bus, aggregator } = await harness();
    const app = buildServer({ bus, store, describe: () => ({ bus: 'memory', store: 'sqlite' }) });
    await app.ready();
    const response = await app.inject({ method: 'GET', url: '/metrics' });
    expect(response.statusCode).toBe(200);
    expect(response.body).toContain('streamtally_consumer_lag');
    expect(response.body).toContain('streamtally_events_duplicate_total');
    await app.close();
    await aggregator.stop();
    await bus.close();
    await store.close();
  });
});
