export type BusKind = 'memory' | 'kafka';
export type StoreKind = 'sqlite' | 'postgres';

export interface Config {
  port: number;
  bus: BusKind;
  store: StoreKind;
  sqlitePath: string;
  postgresUrl: string;
  kafkaBrokers: string[];
  kafkaTopic: string;
  kafkaGroupId: string;
  /** Aggregator batch size. Larger batches amortise the transaction cost. */
  batchSize: number;
  batchLingerMs: number;
}

function env(name: string, fallback: string): string {
  const value = process.env[name];
  return value === undefined || value === '' ? fallback : value;
}

export function loadConfig(): Config {
  const bus = env('BUS', 'memory');
  const store = env('STORE', 'sqlite');
  if (bus !== 'memory' && bus !== 'kafka') {
    throw new Error(`BUS must be memory or kafka, got ${bus}`);
  }
  if (store !== 'sqlite' && store !== 'postgres') {
    throw new Error(`STORE must be sqlite or postgres, got ${store}`);
  }
  return {
    port: Number(env('PORT', '8080')),
    bus,
    store,
    sqlitePath: env('SQLITE_PATH', 'streamtally.db'),
    postgresUrl: env(
      'DATABASE_URL',
      'postgres://streamtally:streamtally@localhost:5432/streamtally',
    ),
    kafkaBrokers: env('KAFKA_BROKERS', 'localhost:19092')
      .split(',')
      .map((broker) => broker.trim()),
    kafkaTopic: env('KAFKA_TOPIC', 'events'),
    kafkaGroupId: env('KAFKA_GROUP_ID', 'streamtally-aggregator'),
    batchSize: Number(env('BATCH_SIZE', '500')),
    batchLingerMs: Number(env('BATCH_LINGER_MS', '50')),
  };
}
