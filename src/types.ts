/** One analytics event. `eventId` is the idempotency key: the pipeline must count it once. */
export interface EventRecord {
  eventId: string;
  tenantId: string;
  type: string;
  value: number;
  /** ISO-8601. Events are bucketed by the minute they *occurred*, not the minute they arrived. */
  occurredAt: string;
}

export interface ApplyResult {
  inserted: number;
  duplicates: number;
}

export interface Bucket {
  bucket: string;
  type: string;
  count: number;
  total: number;
}

export interface Totals {
  events: number;
  value: number;
  buckets: number;
}

export interface Store {
  init(): Promise<void>;
  /** Must be atomic per batch: dedupe and rollup either both apply or neither does. */
  applyBatch(events: EventRecord[]): Promise<ApplyResult>;
  timeseries(sinceMinutes: number): Promise<Bucket[]>;
  topTypes(limit: number): Promise<Array<{ type: string; count: number; total: number }>>;
  totals(): Promise<Totals>;
  reset(): Promise<void>;
  close(): Promise<void>;
}

export interface Bus {
  start(): Promise<void>;
  publish(events: EventRecord[]): Promise<void>;
  subscribe(handler: (batch: EventRecord[]) => Promise<void>): Promise<void>;
  /** Messages accepted but not yet processed. Kafka consumer lag, or queue depth in memory mode. */
  lag(): Promise<number>;
  close(): Promise<void>;
}
