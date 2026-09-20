import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { MemoryBus } from '../src/bus/memory.js';
import type { EventRecord } from '../src/types.js';

function events(count: number, value = 1): EventRecord[] {
  return Array.from({ length: count }, (_, index) => ({
    eventId: randomUUID(),
    tenantId: 'test',
    type: index % 2 === 0 ? 'page_view' : 'checkout',
    value,
    occurredAt: new Date().toISOString(),
  }));
}

describe('MemoryBus', () => {
  it('redelivers a batch whose handler threw instead of dropping it', async () => {
    const bus = new MemoryBus(10, 1);
    await bus.start();
    let attempts = 0;
    const seen: string[] = [];
    await bus.subscribe(async (batch) => {
      attempts += 1;
      if (attempts === 1) throw new Error('boom');
      for (const event of batch) seen.push(event.eventId);
    });
    const batch = events(3);
    await bus.publish(batch);
    await bus.drain();
    expect(attempts).toBeGreaterThanOrEqual(2);
    expect(seen.sort()).toEqual(batch.map((event) => event.eventId).sort());
    await bus.close();
  });

  it('reports queue depth as lag', async () => {
    const bus = new MemoryBus(10, 1);
    await bus.start();
    await bus.publish(events(5));
    expect(await bus.lag()).toBe(5);
    await bus.close();
  });
});
