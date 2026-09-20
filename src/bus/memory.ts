import type { Bus, EventRecord } from '../types.js';

/**
 * In-process bus used by `npm run demo` and by most tests.
 *
 * It is not a Kafka emulator, but it does reproduce the one property the aggregator's
 * correctness depends on: **at-least-once delivery**. A batch whose handler throws goes back
 * to the front of the queue and is delivered again, exactly as a Kafka consumer would see it
 * after a crash before commit. Without that, the exactness harness would be testing nothing.
 */
export class MemoryBus implements Bus {
  private queue: EventRecord[] = [];
  private handler: ((batch: EventRecord[]) => Promise<void>) | null = null;
  private running = false;
  private draining: Promise<void> | null = null;

  constructor(
    private readonly batchSize: number,
    private readonly lingerMs: number,
  ) {}

  async start(): Promise<void> {
    this.running = true;
  }

  async publish(events: EventRecord[]): Promise<void> {
    this.queue.push(...events);
    this.kick();
  }

  async subscribe(handler: (batch: EventRecord[]) => Promise<void>): Promise<void> {
    this.handler = handler;
    this.kick();
  }

  async lag(): Promise<number> {
    return this.queue.length;
  }

  /** Test and harness affordance: resolve once the queue has been fully processed. */
  async drain(): Promise<void> {
    while (this.queue.length > 0 || this.draining) {
      await this.draining;
      if (this.queue.length > 0) await new Promise((resolve) => setTimeout(resolve, this.lingerMs));
    }
  }

  async close(): Promise<void> {
    this.running = false;
    await this.draining;
  }

  private kick(): void {
    if (!this.running || !this.handler || this.draining) return;
    this.draining = this.loop().finally(() => {
      this.draining = null;
    });
  }

  private async loop(): Promise<void> {
    while (this.running && this.handler && this.queue.length > 0) {
      const batch = this.queue.splice(0, this.batchSize);
      try {
        await this.handler(batch);
      } catch {
        // Redelivery, not loss. Order is preserved so the retry sees the same batch first.
        this.queue.unshift(...batch);
        await new Promise((resolve) => setTimeout(resolve, this.lingerMs));
      }
    }
  }
}
