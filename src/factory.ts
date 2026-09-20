import { KafkaBus } from './bus/kafka.js';
import { MemoryBus } from './bus/memory.js';
import type { Config } from './config.js';
import { PostgresStore } from './store/postgres.js';
import { SqliteStore } from './store/sqlite.js';
import type { Bus, Store } from './types.js';

export function makeBus(config: Config): Bus {
  if (config.bus === 'kafka') {
    return new KafkaBus({
      brokers: config.kafkaBrokers,
      topic: config.kafkaTopic,
      groupId: config.kafkaGroupId,
    });
  }
  return new MemoryBus(config.batchSize, config.batchLingerMs);
}

export function makeStore(config: Config): Store {
  return config.store === 'postgres'
    ? new PostgresStore(config.postgresUrl)
    : new SqliteStore(config.sqlitePath);
}
