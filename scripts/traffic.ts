/**
 * Demo traffic generator.
 *
 * Backfills a realistic-looking recent history so the dashboard has a shape to draw, then
 * keeps a steady trickle going. Deliberately re-sends a small share of events with the same
 * event id: the "counted" tile is supposed to stay exact while the duplicate counter climbs.
 *
 *   npm run traffic -- --url http://127.0.0.1:8080 --rps 40
 */

import { randomUUID } from 'node:crypto';

const TYPES = [
  { type: 'page_view', weight: 55 },
  { type: 'search', weight: 18 },
  { type: 'add_to_cart', weight: 14 },
  { type: 'checkout', weight: 8 },
  { type: 'signup', weight: 5 },
];

function argValue(name: string, fallback: string): string {
  const index = process.argv.indexOf(`--${name}`);
  return index >= 0 ? (process.argv[index + 1] ?? fallback) : fallback;
}

function pickType(): string {
  const total = TYPES.reduce((sum, entry) => sum + entry.weight, 0);
  let roll = Math.random() * total;
  for (const entry of TYPES) {
    roll -= entry.weight;
    if (roll <= 0) return entry.type;
  }
  return 'page_view';
}

interface OutboundEvent {
  eventId: string;
  type: string;
  value: number;
  occurredAt?: string;
}

async function post(url: string, body: unknown): Promise<void> {
  const response = await fetch(`${url}/ingest`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(`ingest failed: ${response.status}`);
}

/** The ingest endpoint caps an array body at 1000 events, so long minutes post in chunks. */
const MAX_EVENTS_PER_REQUEST = 1000;

async function backfill(url: string, minutes: number, rps: number): Promise<number> {
  const now = Date.now();
  // Scale history to the live rate. Backfilling a flat 40/minute under a 2400/minute stream
  // produced a chart that was one spike and a floor -- technically correct, useless to read.
  const perMinute = Math.max(20, Math.round(rps * 60));
  let sent = 0;

  for (let minute = minutes; minute >= 1; minute -= 1) {
    const curve = 1 + 0.35 * Math.sin((minute / minutes) * Math.PI * 1.6);
    const jitter = 1 + (Math.random() - 0.5) * 0.18;
    const count = Math.max(10, Math.round(perMinute * curve * jitter));
    const events: OutboundEvent[] = Array.from({ length: count }, () => ({
      eventId: randomUUID(),
      type: pickType(),
      value: 1,
      occurredAt: new Date(now - minute * 60_000 + Math.random() * 60_000).toISOString(),
    }));
    for (let offset = 0; offset < events.length; offset += MAX_EVENTS_PER_REQUEST) {
      await post(url, events.slice(offset, offset + MAX_EVENTS_PER_REQUEST));
    }
    sent += events.length;
  }
  return sent;
}

async function main(): Promise<void> {
  const url = argValue('url', 'http://127.0.0.1:8080').replace(/\/$/, '');
  const rps = Number(argValue('rps', '40'));
  const backfillMinutes = Number(argValue('backfill', '25'));

  if (backfillMinutes > 0) {
    const sent = await backfill(url, backfillMinutes, rps);
    console.log(`backfilled ${sent} events across ${backfillMinutes} minutes`);
  }

  console.log(`streaming ~${rps} events/s (ctrl-c to stop)`);
  let duplicates = 0;
  let sent = 0;
  const recent: OutboundEvent[] = [];

  setInterval(() => {
    const batch: OutboundEvent[] = Array.from({ length: Math.max(1, Math.round(rps / 4)) }, () => ({
      eventId: randomUUID(),
      type: pickType(),
      value: 1,
    }));
    recent.push(...batch);
    if (recent.length > 400) recent.splice(0, recent.length - 400);

    // Roughly one batch in six replays something already sent, mimicking an at-least-once
    // producer retry. The counted total must not move when this happens.
    if (recent.length > 0 && Math.random() < 0.17) {
      const replay = recent[Math.floor(Math.random() * recent.length)];
      if (replay) {
        batch.push(replay);
        duplicates += 1;
      }
    }

    sent += batch.length;
    post(url, batch).catch((error) => console.error(String(error)));
    if (sent % 500 < batch.length) {
      console.log(`sent ${sent} (${duplicates} deliberate replays)`);
    }
  }, 250);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
