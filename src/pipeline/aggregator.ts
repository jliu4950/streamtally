import {
  batchDuration,
  batchFailures,
  consumerLag,
  eventsDuplicate,
  eventsProcessed,
} from '../metrics.js';
import type { Bus, EventRecord, Store } from '../types.js';

/**
 * Bridges the bus to the store.
 *
 * Deliberately thin: the interesting guarantees live one level down. The bus provides
 * at-least-once delivery, the store makes claiming-and-counting atomic, and together those
 * give exactly-once counting. The aggregator's only real job is to not swallow errors --
 * a batch that fails must propagate so the bus redelivers it rather than dropping it.
 */
export class Aggregator {
  private lagTimer: NodeJS.Timeout | null = null;

  constructor(
    private readonly bus: Bus,
    private readonly store: Store,
  ) {}

  async start(lagPollMs = 1000): Promise<void> {
    await this.bus.subscribe(async (batch) => this.apply(batch));
    this.lagTimer = setInterval(() => {
      this.bus
        .lag()
        .then((lag) => consumerLag.set(lag))
        .catch(() => {
          /* a lag probe failure must never take down the pipeline */
        });
    }, lagPollMs);
    this.lagTimer.unref?.();
  }

  async apply(batch: EventRecord[]): Promise<void> {
    if (batch.length === 0) return;
    const stop = batchDuration.startTimer();
    try {
      const result = await this.store.applyBatch(batch);
      eventsProcessed.inc(result.inserted);
      eventsDuplicate.inc(result.duplicates);
    } catch (error) {
      batchFailures.inc();
      throw error;
    } finally {
      stop();
    }
  }

  async stop(): Promise<void> {
    if (this.lagTimer) clearInterval(this.lagTimer);
    this.lagTimer = null;
  }
}
