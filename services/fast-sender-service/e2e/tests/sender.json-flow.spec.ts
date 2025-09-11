import { test, expect } from '@playwright/test';
import { Kafka } from 'kafkajs';

const brokers = process.env.KAFKA_BROKERS?.split(',') ?? ['localhost:9092'];
const pacs002RequestsTopic = process.env.PACS002_REQUESTS_TOPIC ?? 'pacs002-requests';
const eventsTopic = process.env.PAYMENT_EVENTS_TOPIC ?? 'payment-events';

async function ensureTopics(kafka: Kafka, topics: string[]) {
  const admin = kafka.admin();
  await admin.connect();
  const existing = new Set(await admin.listTopics());
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
    await new Promise(r => setTimeout(r, 300));
  }
  return false;
}

async function producePlainJson(kafka: Kafka, topic: string, key: string, value: any) {
  const producer = kafka.producer();
  await producer.connect();
  await producer.send({ topic, messages: [{ key, value: Buffer.from(JSON.stringify(value), 'utf-8') }] });
  await producer.disconnect();
}

test('Plain JSON pacs002-request triggers event JSON', async ({}, testInfo) => {
  testInfo.setTimeout(120000);
  const kafka = new Kafka({ clientId: `sender-json-${process.pid}`, brokers });
  await ensureTopics(kafka, [pacs002RequestsTopic, eventsTopic]);
  const puid = `PJ-${Date.now()}`;
  const xml = `<?xml version="1.0"?><Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13"><FIToFICstmrCdtTrf/></Document>`;
  const before = await snapshotOffsets(kafka, eventsTopic);
  await new Promise(r => setTimeout(r, 300));
  await producePlainJson(kafka, pacs002RequestsTopic, puid, { puid, originalXml: xml, messageType: 'pacs.008.001.13' });
  const advanced = await waitUntilAdvanced(kafka, eventsTopic, before, 60000);
  expect(advanced).toBeTruthy();
});


