import { type Admin, type Consumer, Kafka, type Producer, logLevel } from 'kafkajs';
import type { Bus, EventRecord } from '../types.js';

export interface KafkaBusOptions {
  brokers: string[];
  topic: string;
  groupId: string;
  clientId?: string;
}

/**
 * Kafka-backed bus. Offsets are committed only after the batch handler resolves, which makes
 * delivery at-least-once: a crash mid-batch replays it. Deduplication in the store is what
 * turns at-least-once delivery into exactly-once *counting* -- the broker cannot do that part.
 */
export class KafkaBus implements Bus {
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly consumer: Consumer;
  private readonly admin: Admin;
  private readonly topic: string;
  private readonly groupId: string;

  constructor(options: KafkaBusOptions) {
    this.kafka = new Kafka({
      clientId: options.clientId ?? 'streamtally',
      brokers: options.brokers,
      logLevel: logLevel.ERROR,
      retry: { initialRetryTime: 200, retries: 8 },
    });
    this.topic = options.topic;
    this.groupId = options.groupId;
    this.producer = this.kafka.producer({ allowAutoTopicCreation: true });
    this.consumer = this.kafka.consumer({ groupId: options.groupId });
    this.admin = this.kafka.admin();
  }

  async start(): Promise<void> {
    await this.admin.connect();
    try {
      await this.admin.createTopics({
        topics: [{ topic: this.topic, numPartitions: 3 }],
        waitForLeaders: true,
      });
    } catch (error) {
      // An existing topic is the normal case on restart; anything else is a real failure.
      const message = error instanceof Error ? error.message : String(error);
      if (!/already exists/i.test(message)) throw error;
    }
    await this.producer.connect();
  }

  async publish(events: EventRecord[]): Promise<void> {
    if (events.length === 0) return;
    await this.producer.send({
      topic: this.topic,
      // Keying by event id spreads load evenly and keeps a redelivered event on its partition.
      messages: events.map((event) => ({ key: event.eventId, value: JSON.stringify(event) })),
    });
  }

  async subscribe(handler: (batch: EventRecord[]) => Promise<void>): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.topic, fromBeginning: true });
    await this.consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, heartbeat }) => {
        const events: EventRecord[] = [];
        for (const message of batch.messages) {
          if (message.value) events.push(JSON.parse(message.value.toString()) as EventRecord);
        }
        // Process first, then mark the offsets. Reversing these two lines is the classic way
        // to turn at-least-once into at-most-once and silently lose events on restart.
        await handler(events);
        for (const message of batch.messages) resolveOffset(message.offset);
        await commitOffsetsIfNecessary();
        await heartbeat();
      },
    });
  }

  async lag(): Promise<number> {
    const [high, committed] = await Promise.all([
      this.admin.fetchTopicOffsets(this.topic),
      this.admin.fetchOffsets({ groupId: this.groupId, topics: [this.topic] }),
    ]);
    const committedByPartition = new Map<number, number>();
    for (const topic of committed) {
      for (const partition of topic.partitions) {
        committedByPartition.set(partition.partition, Number(partition.offset));
      }
    }
    let lag = 0;
    for (const partition of high) {
      const end = Number(partition.offset);
      const at = committedByPartition.get(partition.partition) ?? 0;
      // A group that has never committed reports -1; treat that as "everything is pending".
      lag += at < 0 ? end : Math.max(0, end - at);
    }
    return lag;
  }

  async close(): Promise<void> {
    await Promise.allSettled([
      this.consumer.disconnect(),
      this.producer.disconnect(),
      this.admin.disconnect(),
    ]);
  }
}
