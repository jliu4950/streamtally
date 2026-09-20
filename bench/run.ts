/**
 * Ingest benchmark.
 *
 * Measures the ingest path only -- validation, event-id assignment, publish, 202 -- because
 * that is the part a client waits on. Aggregation is asynchronous by design, so folding it
 * into a request-latency number would be measuring the wrong thing.
 *
 * After each run it drains the pipeline and checks that every accepted event was counted
 * exactly once. A throughput number from a pipeline that drops events under load is worse
 * than no number at all.
 *
 *   npm run bench
 */

import { rmSync } from 'node:fs';
import autocannon from 'autocannon';
import { MemoryBus } from '../src/bus/memory.js';
import { eventsIngested } from '../src/metrics.js';
import { Aggregator } from '../src/pipeline/aggregator.js';
import { buildServer } from '../src/server.js';
import { SqliteStore } from '../src/store/sqlite.js';

interface Case {
  name: string;
  eventsPerRequest: number;
  connections: number;
  duration: number;
}

// CI runs a short smoke pass; the published numbers come from a full local run.
const DURATION = Number(process.env.BENCH_DURATION ?? '10');

const CASES: Case[] = [
  { name: 'single event per request', eventsPerRequest: 1, connections: 50, duration: DURATION },
  { name: 'batch of 50 per request', eventsPerRequest: 50, connections: 20, duration: DURATION },
];

function payload(eventsPerRequest: number): string {
  const types = ['page_view', 'add_to_cart', 'checkout', 'search'];
  if (eventsPerRequest === 1) {
    return JSON.stringify({ type: 'page_view', value: 1 });
  }
  return JSON.stringify(
    Array.from({ length: eventsPerRequest }, (_, index) => ({
      type: types[index % types.length],
      value: 1,
    })),
  );
}

async function runCase(testCase: Case) {
  const dbPath = `bench-${testCase.eventsPerRequest}.db`;
  for (const suffix of ['', '-wal', '-shm']) rmSync(`${dbPath}${suffix}`, { force: true });

  eventsIngested.reset();
  const store = new SqliteStore(dbPath);
  const bus = new MemoryBus(500, 5);
  await store.init();
  await bus.start();
  const aggregator = new Aggregator(bus, store);
  await aggregator.start(500);

  const app = buildServer({ bus, store, describe: () => ({ bus: 'memory', store: 'sqlite' }) });
  await app.listen({ port: 0, host: '127.0.0.1' });
  const address = app.server.address();
  if (typeof address === 'string' || address === null) throw new Error('no port');

  const result = await autocannon({
    url: `http://127.0.0.1:${address.port}/ingest`,
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: payload(testCase.eventsPerRequest),
    connections: testCase.connections,
    duration: testCase.duration,
  });

  // Ground truth for "accepted" is the server's own counter, not autocannon's tally of
  // responses it received. The load generator stops reading at the end of the window while
  // the server is still finishing in-flight requests, so its count runs slightly low.
  const ingestedMetric = await eventsIngested.get();
  const accepted = ingestedMetric.values[0]?.value ?? 0;
  await bus.drain();
  const totals = await store.totals();

  await app.close();
  await aggregator.stop();
  await bus.close();
  await store.close();
  for (const suffix of ['', '-wal', '-shm']) rmSync(`${dbPath}${suffix}`, { force: true });

  return {
    name: testCase.name,
    connections: testCase.connections,
    rps: Math.round(result.requests.average),
    eventsPerSecond: Math.round(result.requests.average * testCase.eventsPerRequest),
    p50: result.latency.p50,
    p95: result.latency.p97_5,
    p99: result.latency.p99,
    nonTwoXX: result.non2xx,
    accepted,
    counted: totals.events,
    exact: totals.events === accepted,
  };
}

async function main(): Promise<number> {
  const results = [];
  for (const testCase of CASES) {
    process.stdout.write(`running "${testCase.name}" for ${testCase.duration}s ... `);
    const result = await runCase(testCase);
    console.log(`${result.eventsPerSecond} events/s, p99 ${result.p99} ms`);
    results.push(result);
  }

  console.log(
    '\n| scenario | conns | req/s | events/s | p50 | p95 | p99 | non-2xx | counted exactly |',
  );
  console.log('|---|---|---|---|---|---|---|---|---|');
  for (const result of results) {
    console.log(
      `| ${result.name} | ${result.connections} | ${result.rps} | ${result.eventsPerSecond} | ` +
        `${result.p50} ms | ${result.p95} ms | ${result.p99} ms | ${result.nonTwoXX} | ` +
        `${result.exact ? `yes (${result.counted})` : `NO (${result.counted}/${result.accepted})`} |`,
    );
  }

  const inexact = results.filter((result) => !result.exact);
  if (inexact.length > 0) {
    console.error(`\nFAILED: ${inexact.length} scenario(s) lost or duplicated events under load`);
    return 1;
  }
  console.log('\nevery accepted event counted exactly once in all scenarios');
  return 0;
}

main().then(
  (code) => process.exit(code),
  (error) => {
    console.error(error);
    process.exit(1);
  },
);
