import { test, expect } from '@playwright/test';
import { Kafka } from 'kafkajs';

const brokers = process.env.KAFKA_BROKERS?.split(',') ?? ['localhost:9092'];
const pacs002RequestsTopic = process.env.PACS002_REQUESTS_TOPIC ?? 'pacs002-requests';
const eventsTopic = process.env.PAYMENT_EVENTS_TOPIC ?? 'payment-events';

async function waitForEvent(kafka: Kafka, topic: string, expectedKey: string, timeoutMs = 30000) {
  // Snapshot baseline offsets
  const admin = kafka.admin();
  await admin.connect();
  const endOffsets = await admin.fetchTopicOffsets(topic);
  const baseline = new Map(endOffsets.map(p => [p.partition, Number(p.offset)]));
  await admin.disconnect();

  const consumer = kafka.consumer({ groupId: `sender-e2e-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  let seen = false;
  const timer = setTimeout(async () => {
    try { await consumer.disconnect(); } catch {}
  }, timeoutMs);

  await new Promise<void>(async (resolve) => {
    await consumer.run({
      eachMessage: async ({ message, partition }) => {
        const offsetNum = Number(message.offset);
        const base = baseline.get(partition) ?? 0;
        if (offsetNum >= base && message.key && message.key.toString() === expectedKey) {
          seen = true;
          clearTimeout(timer);
          try { await consumer.disconnect(); } catch {}
          resolve();
        }
      }
    });
  });
  return seen;
}

async function ensureTopics(kafka: Kafka, topics: string[]) {
  const admin = kafka.admin();
  await admin.connect();
  const existing = new Set((await admin.listTopics()));
  const toCreate = topics.filter(t => !existing.has(t));
  if (toCreate.length) {
    await admin.createTopics({ topics: toCreate.map(t => ({ topic: t, numPartitions: 1, replicationFactor: 1 })) });
  }
  await admin.disconnect();
}

async function snapshotOffsets(kafka: Kafka, topic: string) {
  const admin = kafka.admin();
  await admin.connect();
  const offsets = await admin.fetchTopicOffsets(topic);
  await admin.disconnect();
  return offsets;
}

async function waitUntilAdvanced(kafka: Kafka, topic: string, before: any, timeoutMs = 30000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const after = await snapshotOffsets(kafka, topic);
    if (Number(after[0].offset) > Number(before[0].offset)) return true;
    await new Promise(r => setTimeout(r, 500));
  }
  return false;
}

async function produceFramedJsonAvro(kafka: Kafka, topic: string, key: string, value: any) {
  const producer = kafka.producer();
  await producer.connect();
  const json = Buffer.from(JSON.stringify(value), 'utf-8');
  const buf = Buffer.alloc(1 + 4 + json.length);
  buf.writeUInt8(0, 0); // magic byte
  buf.writeUInt32BE(1, 1); // dummy schema id
  json.copy(buf, 5);
  await producer.send({ topic, messages: [{ key, value: buf }] });
  await producer.disconnect();
}

test('Avro pacs002-request triggers event JSON', async ({}, testInfo) => {
  testInfo.setTimeout(120000);
  const kafka = new Kafka({ clientId: `sender-e2e-${process.pid}`, brokers });

  await ensureTopics(kafka, [pacs002RequestsTopic, eventsTopic]);

  const puid = `P-${Date.now()}`;
  const xml = `<?xml version="1.0"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13"><FIToFICstmrCdtTrf><GrpHdr><MsgId>${puid}</MsgId></GrpHdr></FIToFICstmrCdtTrf></Document>`;

  const before = await snapshotOffsets(kafka, eventsTopic);
  await new Promise(r => setTimeout(r, 500));
  await produceFramedJsonAvro(kafka, pacs002RequestsTopic, puid, { puid, payload: xml });
  const advanced = await waitUntilAdvanced(kafka, eventsTopic, before, 60000);
  expect(advanced).toBeTruthy();
});


