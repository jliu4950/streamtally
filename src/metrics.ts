import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

export const registry = new Registry();
collectDefaultMetrics({ register: registry });

export const eventsIngested = new Counter({
  name: 'streamtally_events_ingested_total',
  help: 'Events accepted by the ingest endpoint and published to the bus.',
  registers: [registry],
});

export const eventsProcessed = new Counter({
  name: 'streamtally_events_processed_total',
  help: 'Events folded into a rollup by the aggregator.',
  registers: [registry],
});

export const eventsDuplicate = new Counter({
  name: 'streamtally_events_duplicate_total',
  help: 'Redelivered events recognised by their event id and counted exactly once.',
  registers: [registry],
});

export const batchFailures = new Counter({
  name: 'streamtally_batch_failures_total',
  help: 'Aggregator batches that threw and were redelivered.',
  registers: [registry],
});

export const consumerLag = new Gauge({
  name: 'streamtally_consumer_lag',
  help: 'Messages accepted but not yet folded into a rollup.',
  registers: [registry],
});

export const ingestDuration = new Histogram({
  name: 'streamtally_ingest_duration_seconds',
  help: 'Wall time of an ingest request, from validation to bus acknowledgement.',
  buckets: [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1],
  registers: [registry],
});

export const batchDuration = new Histogram({
  name: 'streamtally_batch_apply_duration_seconds',
  help: 'Wall time of one aggregator transaction.',
  buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1, 2.5],
  registers: [registry],
});
