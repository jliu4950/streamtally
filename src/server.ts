import { randomUUID } from 'node:crypto';
import { existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import fastifyStatic from '@fastify/static';
import Fastify, { type FastifyInstance } from 'fastify';
import { z } from 'zod';
import { eventsIngested, ingestDuration, registry } from './metrics.js';
import type { Bus, EventRecord, Store } from './types.js';

const here = dirname(fileURLToPath(import.meta.url));
const WEB_DIST = resolve(here, '../web/dist');

const eventInput = z.object({
  // Callers may supply their own idempotency key. If they do, a retried HTTP request is free:
  // the event is counted once no matter how many times it is sent.
  eventId: z.string().min(1).max(128).optional(),
  tenantId: z.string().min(1).max(64).default('demo'),
  type: z.string().min(1).max(64),
  value: z.number().finite().default(1),
  occurredAt: z.string().datetime().optional(),
});

const ingestBody = z.union([eventInput, z.array(eventInput).min(1).max(1000)]);

export interface ServerDeps {
  bus: Bus;
  store: Store;
  describe: () => { bus: string; store: string };
}

export function buildServer(deps: ServerDeps): FastifyInstance {
  const app = Fastify({ logger: false, bodyLimit: 4 * 1024 * 1024 });

  app.get('/health', async () => ({ status: 'ok', ...deps.describe() }));

  app.post('/ingest', async (request, reply) => {
    const stop = ingestDuration.startTimer();
    try {
      const parsed = ingestBody.safeParse(request.body);
      if (!parsed.success) {
        return reply.code(400).send({ error: 'invalid_event', detail: parsed.error.issues });
      }
      const now = new Date().toISOString();
      const inputs = Array.isArray(parsed.data) ? parsed.data : [parsed.data];
      const events: EventRecord[] = inputs.map((input) => ({
        eventId: input.eventId ?? randomUUID(),
        tenantId: input.tenantId,
        type: input.type,
        value: input.value,
        occurredAt: input.occurredAt ?? now,
      }));
      await deps.bus.publish(events);
      eventsIngested.inc(events.length);
      // 202, not 200: the events are durably queued, not yet aggregated. Saying otherwise
      // would be lying to the client about a guarantee the pipeline does not make here.
      return reply.code(202).send({ accepted: events.length });
    } finally {
      stop();
    }
  });

  app.get('/api/timeseries', async (request) => {
    const minutes = Number((request.query as Record<string, string>).minutes ?? '60');
    return { buckets: await deps.store.timeseries(Number.isFinite(minutes) ? minutes : 60) };
  });

  app.get('/api/top', async (request) => {
    const limit = Number((request.query as Record<string, string>).limit ?? '8');
    return { types: await deps.store.topTypes(Number.isFinite(limit) ? limit : 8) };
  });

  app.get('/api/totals', async () => ({
    totals: await deps.store.totals(),
    lag: await deps.bus.lag(),
  }));

  app.get('/api/stream', async (request, reply) => {
    reply.raw.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive',
    });
    const push = async () => {
      try {
        const payload = {
          totals: await deps.store.totals(),
          lag: await deps.bus.lag(),
          top: await deps.store.topTypes(8),
          buckets: await deps.store.timeseries(30),
        };
        reply.raw.write(`data: ${JSON.stringify(payload)}\n\n`);
      } catch {
        /* a failed tick should not kill the stream */
      }
    };
    await push();
    const timer = setInterval(push, 1000);
    request.raw.on('close', () => clearInterval(timer));
    return reply;
  });

  app.get('/metrics', async (_request, reply) => {
    reply.header('content-type', registry.contentType);
    return registry.metrics();
  });

  if (existsSync(WEB_DIST)) {
    app.register(fastifyStatic, { root: WEB_DIST });
  } else {
    app.get('/', async () => ({
      message: 'Dashboard not built. Run `npm run build:web`, or `npm run dev:web` for HMR.',
      api: ['/api/totals', '/api/timeseries', '/api/top', '/api/stream', '/metrics'],
    }));
  }

  return app;
}
