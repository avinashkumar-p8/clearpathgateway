import { test, expect } from '@playwright/test';
import { Kafka } from 'kafkajs';
import avsc from 'avsc';
import path from 'path';
import { fileURLToPath } from 'url';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const healthUrl = process.env.ROUTER_HEALTH_URL || 'http://localhost:8080/health';
const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const exceptionTopic = process.env.EXCEPTION_TOPIC || 'exception-queue';
const pacs002Topic = process.env.PACS002_TOPIC || 'pacs002-requests';
const activemqApi = process.env.ACTIVEMQ_API || 'http://localhost:8161/api/message';
const inboundQ = process.env.ACTIVEMQ_INBOUND || 'payment.inbound';

async function waitForHealth(url: string, timeoutMs = 30000) {
  const start = Date.now();
  for (;;) {
    try {
      const resp = await fetch(url);
      if (resp.ok) return true;
    } catch {}
    if (Date.now() - start > timeoutMs) return false;
    await new Promise(r => setTimeout(r, 500));
  }
}

async function readAvroSchema(schemaPath: string): Promise<avsc.Type> {
  const fs = await import('fs/promises');
  const schemaContent = await fs.readFile(schemaPath, 'utf-8');
  return avsc.Type.forSchema(JSON.parse(schemaContent));
}

function decodeAvroMessage(buffer: Buffer, schema: avsc.Type): any {
  try { return schema.fromBuffer(buffer); } catch { return null; }
}

async function sendToInbound(xml: string) {
  const basic = Buffer.from(`${process.env.ACTIVEMQ_USERNAME||'admin'}:${process.env.ACTIVEMQ_PASSWORD||'admin'}`).toString('base64');
  await fetch(`${activemqApi}/${inboundQ}?type=queue`, {
    method: 'POST',
    headers: { 'Authorization': `Basic ${basic}`, 'Content-Type': 'text/plain' },
    body: xml,
  });
}

test.describe.configure({ mode: 'serial' });

test.describe('XSD failure flows', () => {
  let exceptionSchema: avsc.Type;
  let pacs002Schema: avsc.Type;

  test.beforeAll(async () => {
    const base = path.resolve(__dirname, '../../src/main/resources/avro');
    exceptionSchema = await readAvroSchema(path.join(base, 'router-exception.avsc'));
    pacs002Schema = await readAvroSchema(path.join(base, 'pacs002-request.avsc'));
    // Ensure topics exist (exception + pacs002)
    const kafka = new Kafka({ clientId: `xsd-admin-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    try {
      const existing = await admin.listTopics();
      const needed: string[] = [];
      if (!existing.includes(exceptionTopic)) needed.push(exceptionTopic);
      if (!existing.includes(pacs002Topic)) needed.push(pacs002Topic);
      if (needed.length > 0) {
        await admin.createTopics({ topics: needed.map(t => ({ topic: t, numPartitions: 1, replicationFactor: 1 })) });
      }
    } finally {
      await admin.disconnect().catch(() => {});
    }
  });

  async function produceAndAwaitOffsetAdvance(topic: string, doSend: () => Promise<void>, timeoutMs = 60000) {
    const kafka = new Kafka({ clientId: `xsd-admin-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    try {
      const before = await admin.fetchTopicOffsets(topic);
      await doSend();
      const start = Date.now();
      for (;;) {
        const after = await admin.fetchTopicOffsets(topic);
        let advanced = false;
        for (let i = 0; i < after.length; i++) {
          if (Number(after[i].offset) > Number(before[i].offset)) { advanced = true; break; }
        }
        if (advanced) return before;
        if (Date.now() - start > timeoutMs) return before;
        await new Promise(r => setTimeout(r, 500));
      }
    } finally {
      await admin.disconnect().catch(() => {});
    }
  }

  async function consumeFromOffset(topic: string, schema: avsc.Type, before: Array<{ partition: number; offset: string | number }>, timeoutMs = 120000, acceptAnyOnDecodeFailure = false): Promise<any> {
    const kafka = new Kafka({ clientId: `xsd-e2e-cons-${process.pid}`, brokers });
    const consumer = kafka.consumer({ groupId: `xsd-e2e-cons-${process.pid}-${Math.random().toString(36).slice(2,8)}` });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    // Wait for group join then seek
    let joined = false;
    consumer.on(consumer.events.GROUP_JOIN, async () => {
      if (joined) return; joined = true;
      for (const p of before) {
        try {
          await consumer.seek({ topic, partition: p.partition, offset: String(p.offset) });
        } catch {}
      }
    });
    return new Promise(async (resolve) => {
      const t = setTimeout(async () => { try { await consumer.disconnect(); } catch {} resolve(null); }, timeoutMs);
      await consumer.run({
        eachMessage: async ({ message }) => {
          if (!message.value) return;
          const decoded = decodeAvroMessage(message.value as Buffer, schema);
          if (decoded || acceptAnyOnDecodeFailure) {
            clearTimeout(t);
            try { await consumer.disconnect(); } catch {}
            resolve(decoded || { any: true });
          }
        }
      });
    });
  }

  async function snapshotOffsets(topics: string[]): Promise<Record<string, Array<{ partition: number; offset: number }>>> {
    const kafka = new Kafka({ clientId: `xsd-admin-snap-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    try {
      const res: Record<string, Array<{ partition: number; offset: number }>> = {};
      for (const t of topics) {
        const parts = await admin.fetchTopicOffsets(t);
        res[t] = parts.map(p => ({ partition: p.partition, offset: Number(p.offset) }));
      }
      return res;
    } finally {
      await admin.disconnect().catch(() => {});
    }
  }

  async function waitUntilAdvanced(topics: string[], before: Record<string, Array<{ partition: number; offset: number }>>, timeoutMs = 60000) {
    const kafka = new Kafka({ clientId: `xsd-admin-wait-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    try {
      const start = Date.now();
      for (;;) {
        let allAdvanced = true;
        for (const t of topics) {
          const after = await admin.fetchTopicOffsets(t);
          const bef = before[t];
          let adv = false;
          for (let i = 0; i < after.length; i++) {
            if (Number(after[i].offset) > Number(bef[i].offset)) { adv = true; break; }
          }
          if (!adv) { allAdvanced = false; break; }
        }
        if (allAdvanced) return;
        if (Date.now() - start > timeoutMs) return;
        await new Promise(r => setTimeout(r, 500));
      }
    } finally {
      await admin.disconnect().catch(() => {});
    }
  }

  async function consumeAfter(topic: string, schema: avsc.Type, sendAfterReady: () => Promise<void>, timeoutMs = 180000, acceptAnyOnDecodeFailure = false): Promise<any> {
    const kafka = new Kafka({ clientId: `xsd-e2e-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    const parts = await admin.fetchTopicOffsets(topic);
    const endOffsets = parts.map(p => ({ partition: p.partition, offset: Number(p.offset) }));
    await admin.disconnect().catch(() => {});
    const consumer = kafka.consumer({ groupId: `xsd-e2e-${process.pid}-${Math.random().toString(36).slice(2,8)}` });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    let joined = false;
    consumer.on(consumer.events.GROUP_JOIN, async () => {
      if (joined) return; joined = true;
      for (const p of endOffsets) {
        try { await consumer.seek({ topic, partition: p.partition, offset: String(p.offset) }); } catch {}
      }
      // send only after we are joined and have sought
      try { await sendAfterReady(); } catch {}
    });
    return new Promise(async (resolve) => {
      const t = setTimeout(async () => { try { await consumer.disconnect(); } catch {} resolve(null); }, timeoutMs);
      await consumer.run({
        eachMessage: async ({ message }) => {
          if (!message.value) return;
          const decoded = decodeAvroMessage(message.value as Buffer, schema);
          if (decoded || acceptAnyOnDecodeFailure) {
            clearTimeout(t);
            try { await consumer.disconnect(); } catch {}
            resolve(decoded || { any: true });
          }
        }
      });
    });
  }

  test('pacs.003 XSD failure emits exception (offset-verified)', async ({}, testInfo) => {
    testInfo.setTimeout(120000);
    const ok = await waitForHealth(healthUrl, 30000);
    expect(ok).toBeTruthy();

    const bad003 = `<?xml version="1.0" encoding="UTF-8"?>\n<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11">\n  <CstmrDrctDbtInitn></CstmrDrctDbtInitn>\n</Document>`;

    const before = await snapshotOffsets([exceptionTopic, pacs002Topic]);
    await sendToInbound(bad003);
    await waitUntilAdvanced([exceptionTopic], before, 60000);
    const kafka = new Kafka({ clientId: `xsd-admin-verify-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    const afterExc = await admin.fetchTopicOffsets(exceptionTopic);
    await admin.disconnect().catch(() => {});
    const advancedExc = Number(afterExc[0].offset) > Number(before[exceptionTopic][0].offset);
    expect(advancedExc).toBeTruthy();
  });

  test('pacs.007 XSD failure emits exception (offset-verified)', async ({}, testInfo) => {
    testInfo.setTimeout(120000);
    const bad007 = `<?xml version="1.0" encoding="UTF-8"?>\n<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.007.001.13">\n  <FIToFIPmtRtr></FIToFIPmtRtr>\n</Document>`;

    const before = await snapshotOffsets([exceptionTopic, pacs002Topic]);
    await sendToInbound(bad007);
    await waitUntilAdvanced([exceptionTopic], before, 60000);
    const kafka = new Kafka({ clientId: `xsd-admin-verify-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    const afterExc = await admin.fetchTopicOffsets(exceptionTopic);
    await admin.disconnect().catch(() => {});
    const advancedExc = Number(afterExc[0].offset) > Number(before[exceptionTopic][0].offset);
    expect(advancedExc).toBeTruthy();
  });

  test('camt.056 XSD failure emits exception (offset-verified)', async ({}, testInfo) => {
    testInfo.setTimeout(120000);
    const bad056 = `<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Document xmlns=\"urn:iso:std:iso:20022:tech:xsd:camt.056.001.11\">\n  <FIToFIPmtCxlReq></FIToFIPmtCxlReq>\n</Document>`;

    const before = await snapshotOffsets([exceptionTopic, pacs002Topic]);
    await sendToInbound(bad056);
    await waitUntilAdvanced([exceptionTopic], before, 60000);
    const kafka = new Kafka({ clientId: `xsd-admin-verify-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    const afterExc = await admin.fetchTopicOffsets(exceptionTopic);
    await admin.disconnect().catch(() => {});
    const advancedExc = Number(afterExc[0].offset) > Number(before[exceptionTopic][0].offset);
    expect(advancedExc).toBeTruthy();
  });

  test('HEAD.001.001.01 XSD failure emits exception (offset-verified)', async ({}, testInfo) => {
    testInfo.setTimeout(120000);
    const badHead = `<?xml version="1.0" encoding="UTF-8"?>\n<AppHdr xmlns="urn:iso:std:iso:20022:tech:xsd:head.001.001.01"></AppHdr>`;

    const before = await snapshotOffsets([exceptionTopic, pacs002Topic]);
    await sendToInbound(badHead);
    await waitUntilAdvanced([exceptionTopic], before, 60000);
    const kafka = new Kafka({ clientId: `xsd-admin-verify-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    const afterExc = await admin.fetchTopicOffsets(exceptionTopic);
    await admin.disconnect().catch(() => {});
    const advancedExc = Number(afterExc[0].offset) > Number(before[exceptionTopic][0].offset);
    expect(advancedExc).toBeTruthy();
  });

  test('Wrong namespace causes XSD failure and emits exception', async ({}, testInfo) => {
    testInfo.setTimeout(120000);
    // Correct-ish body but wrong namespace that should not match any mapped XSD
    const wrongNs = `<?xml version="1.0" encoding="UTF-8"?>\n<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.003.001.99">\n  <CstmrDrctDbtInitn/>\n</Document>`;

    const before = await snapshotOffsets([exceptionTopic, pacs002Topic]);
    await sendToInbound(wrongNs);
    await waitUntilAdvanced([exceptionTopic], before, 60000);
    const kafka = new Kafka({ clientId: `xsd-admin-verify-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    const afterExc = await admin.fetchTopicOffsets(exceptionTopic);
    await admin.disconnect().catch(() => {});
    const advancedExc = Number(afterExc[0].offset) > Number(before[exceptionTopic][0].offset);
    expect(advancedExc).toBeTruthy();
  });
});


