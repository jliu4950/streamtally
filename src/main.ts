import { MemoryBus } from './bus/memory.js';
import { loadConfig } from './config.js';
import { Aggregator } from './pipeline/aggregator.js';
import { buildServer } from './server.js';
import { SqliteStore } from './store/sqlite.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const bus = new MemoryBus(config.batchSize, config.batchLingerMs);
  const store = new SqliteStore(config.sqlitePath);

  await store.init();
  await bus.start();

  const aggregator = new Aggregator(bus, store);
  await aggregator.start();

  const app = buildServer({
    bus,
    store,
    describe: () => ({ bus: config.bus, store: config.store }),
  });

  await app.listen({ port: config.port, host: '0.0.0.0' });
  console.log(`streamtally listening on :${config.port}`);

  const shutdown = async (signal: string) => {
    console.log(`\n${signal} received, draining`);
    await aggregator.stop();
    await app.close();
    await bus.close();
    await store.close();
    process.exit(0);
  };
  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
