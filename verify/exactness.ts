/**
 * Exactness harness: try to make the counts wrong, then prove they are not.
 *
 * A dashboard that renders a number nobody has stress-tested is decoration. The claim this
 * project makes is narrow and falsifiable: **every accepted event is counted exactly once,
 * even under redelivery and mid-batch failure.** This harness is the attempt to break it.
 *
 * The scenario that matters is `fail-after-commit`. A batch is written to the store and the
 * process then dies before the offset is committed, so the bus redelivers work that has
 * already been applied. An aggregator that trusts delivery semantics double-counts here;
 * only the store's idempotency claim saves it. That is the whole design in one test.
 *
 *   npm run verify:exactness
 *   BUS=kafka STORE=postgres npm run verify:exactness
 */

import { randomUUID } from 'node:crypto';
import { rmSync } from 'node:fs';
import { loadConfig } from '../src/config.js';
import { makeBus, makeStore } from '../src/factory.js';
import { Aggregator } from '../src/pipeline/aggregator.js';
import type { ApplyResult, EventRecord, Store } from '../src/types.js';

const TYPES = ['page_view', 'add_to_cart', 'checkout', 'search', 'signup'];

interface Scenario {
  name: string;
  description: string;
  /** Total publishes, including repeats. Ground truth is always over *unique* ids. */
  build: (unique: EventRecord[]) => EventRecord[];
  fault?: 'before-commit' | 'after-commit';
}

const SCENARIOS: Scenario[] = [
  {
    name: 'clean',
    description: 'no faults, no repeats -- the control case',
    build: (unique) => unique,
  },
  {
    name: 'redelivered',
    description: 'every event published twice, a tenth of them three times',
    build: (unique) => [...unique, ...unique, ...unique.filter((_, i) => i % 10 === 0)],
  },
  {
    name: 'duplicates-within-batch',
    description: 'repeats adjacent in the stream, so they land in one batch',
    build: (unique) => unique.flatMap((event) => [event, event]),
  },
  {
    name: 'fail-before-commit',
    description: 'store throws before writing; the bus must redeliver, nothing is lost',
    build: (unique) => unique,
    fault: 'before-commit',
  },
  {
    name: 'fail-after-commit',
    description: 'store writes, then the process dies before the offset commit',
    build: (unique) => unique,
    fault: 'after-commit',
  },
];

/** Wraps a store and injects failures on a deterministic subset of batches. */
class FaultyStore implements Store {
  private calls = 0;
  public injected = 0;

  constructor(
    private readonly inner: Store,
    private readonly mode: 'before-commit' | 'after-commit',
    private readonly everyNth = 3,
  ) {}

  async applyBatch(events: EventRecord[]): Promise<ApplyResult> {
    this.calls += 1;
    const shouldFail = this.calls % this.everyNth === 0;
    if (shouldFail && this.mode === 'before-commit') {
      this.injected += 1;
      throw new Error('injected failure before commit');
    }
    const result = await this.inner.applyBatch(events);
    if (shouldFail && this.mode === 'after-commit') {
      this.injected += 1;
      // The write is durable; the acknowledgement never happens. Redelivery is now certain
      // and the dedupe table is the only thing standing between us and a double count.
      throw new Error('injected failure after commit');
    }
    return result;
  }

  init = () => this.inner.init();
  timeseries = (m: number) => this.inner.timeseries(m);
  topTypes = (n: number) => this.inner.topTypes(n);
  totals = () => this.inner.totals();
  reset = () => this.inner.reset();
  close = () => this.inner.close();
}

function buildUnique(count: number): EventRecord[] {
  const base = Date.now() - count * 10;
  return Array.from({ length: count }, (_, index) => ({
    eventId: randomUUID(),
    tenantId: 'verify',
    type: TYPES[index % TYPES.length] as string,
    value: (index % 7) + 1,
    occurredAt: new Date(base + index * 10).toISOString(),
  }));
}

async function waitForTotal(store: Store, expected: number, timeoutMs: number): Promise<number> {
  const deadline = Date.now() + timeoutMs;
  let last = -1;
  while (Date.now() < deadline) {
    const totals = await store.totals();
    last = totals.events;
    if (last >= expected) return last;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  return last;
}

interface Outcome {
  scenario: string;
  description: string;
  published: number;
  unique: number;
  counted: number;
  expectedValue: number;
  countedValue: number;
  faultsInjected: number;
  pass: boolean;
}

async function runScenario(scenario: Scenario, uniqueCount: number): Promise<Outcome> {
  const config = loadConfig();
  const bus = makeBus(config);
  const realStore = makeStore({
    ...config,
    sqlitePath: config.store === 'sqlite' ? `verify-${scenario.name}.db` : config.sqlitePath,
    kafkaGroupId: `${config.kafkaGroupId}-verify-${scenario.name}`,
    kafkaTopic: `${config.kafkaTopic}-verify-${scenario.name}`,
  });
  await realStore.init();
  await realStore.reset();

  const faulty = scenario.fault ? new FaultyStore(realStore, scenario.fault) : null;
  const store: Store = faulty ?? realStore;

  await bus.start();
  const aggregator = new Aggregator(bus, store);
  await aggregator.start(250);

  const unique = buildUnique(uniqueCount);
  const published = scenario.build(unique);
  const expectedValue = unique.reduce((sum, event) => sum + event.value, 0);

  for (let index = 0; index < published.length; index += 200) {
    await bus.publish(published.slice(index, index + 200));
  }

  const counted = await waitForTotal(realStore, unique.length, 30_000);
  const totals = await realStore.totals();

  await aggregator.stop();
  await bus.close();
  await realStore.close();
  if (config.store === 'sqlite') {
    for (const suffix of ['', '-wal', '-shm']) {
      rmSync(`verify-${scenario.name}.db${suffix}`, { force: true });
    }
  }

  return {
    scenario: scenario.name,
    description: scenario.description,
    published: published.length,
    unique: unique.length,
    counted,
    expectedValue,
    countedValue: totals.value,
    faultsInjected: faulty?.injected ?? 0,
    pass: counted === unique.length && Math.abs(totals.value - expectedValue) < 1e-6,
  };
}

async function main(): Promise<number> {
  const uniqueCount = Number(process.env.VERIFY_EVENTS ?? '5000');
  const config = loadConfig();
  console.log(
    `exactness harness: ${uniqueCount} unique events per scenario ` +
      `(bus=${config.bus} store=${config.store})\n`,
  );

  const outcomes: Outcome[] = [];
  for (const scenario of SCENARIOS) {
    outcomes.push(await runScenario(scenario, uniqueCount));
  }

  const width = Math.max(...outcomes.map((outcome) => outcome.scenario.length));
  for (const outcome of outcomes) {
    const drift = outcome.counted - outcome.unique;
    console.log(
      `${outcome.pass ? 'PASS' : 'FAIL'}  ${outcome.scenario.padEnd(width)}  ` +
        `published=${String(outcome.published).padStart(6)} ` +
        `unique=${String(outcome.unique).padStart(5)} ` +
        `counted=${String(outcome.counted).padStart(5)} ` +
        `drift=${drift >= 0 ? '+' : ''}${drift} ` +
        `faults=${outcome.faultsInjected}`,
    );
    console.log(`      ${outcome.description}`);
  }

  const failed = outcomes.filter((outcome) => !outcome.pass);
  const suffix = failed.length > 0 ? ` -- FAILED: ${failed.map((f) => f.scenario).join(', ')}` : '';
  console.log(`\n${outcomes.length - failed.length}/${outcomes.length} scenarios exact${suffix}`);
  return failed.length > 0 ? 1 : 0;
}

main().then(
  (code) => process.exit(code),
  (error) => {
    console.error(error);
    process.exit(1);
  },
);
