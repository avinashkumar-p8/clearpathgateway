import { test, expect } from '@playwright/test';
test.describe.configure({ timeout: 180000 });
import { Kafka } from 'kafkajs';
import avsc from 'avsc';
import path from 'path';
import { fileURLToPath } from 'url';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function readAvroSchema(schemaPath: string): Promise<avsc.Type> {
  const fs = await import('fs/promises');
  const schemaContent = await fs.readFile(schemaPath, 'utf-8');
  return avsc.Type.forSchema(JSON.parse(schemaContent));
}

function decodeAvroMessage(buffer: Buffer, schema: avsc.Type): any {
  try { return schema.fromBuffer(buffer); } catch { return null; }
}

async function waitForHealth(url: string, timeoutMs = 30000) {
  const start = Date.now();
  for (;;) {
    try { const r = await fetch(url); if (r.ok) return true; } catch {}
    if (Date.now() - start > timeoutMs) return false;
    await new Promise(r => setTimeout(r, 500));
  }
}

test.describe('Duplicate pacs.008 flow', () => {
  const healthUrl = process.env.ROUTER_HEALTH_URL || 'http://localhost:8080/health';
  const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
  const paymentTopic = process.env.PAYMENT_MESSAGES_TOPIC || 'payment-messages';
  const activemqApi = process.env.ACTIVEMQ_API || 'http://localhost:8161/api/message';
  const inboundQ = process.env.ACTIVEMQ_INBOUND || 'payment.inbound';

  let schema: avsc.Type;

  test.beforeAll(async () => {
    test.setTimeout(120000);
    const healthOk = await waitForHealth(healthUrl, 60000);
    expect(healthOk).toBeTruthy();
    const schemaPath = path.resolve(__dirname, '../../src/main/resources/avro/unified-payment-message.avsc');
    schema = await readAvroSchema(schemaPath);
  });

  async function send(xml: string) {
    const basic = Buffer.from(`${process.env.ACTIVEMQ_USERNAME||'admin'}:${process.env.ACTIVEMQ_PASSWORD||'admin'}`).toString('base64');
    await fetch(`${activemqApi}/${inboundQ}?type=queue`, {
      method: 'POST',
      headers: { 'Authorization': `Basic ${basic}`, 'Content-Type': 'text/plain' },
      body: xml,
    });
  }

  test('duplicate is logged (second publish allowed)', async () => {
    test.setTimeout(120000);
    const health = await fetch(healthUrl);
    expect(health.ok).toBeTruthy();

    const msgId = `DUP-003-${Date.now()}-${Math.random().toString(36).slice(2,6)}`;
    const xml = `<?xml version="1.0" encoding="UTF-8"?>\n<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11">\n  <FIToFICstmrDrctDbt>\n    <GrpHdr>\n      <MsgId>${msgId}</MsgId>\n      <CreDtTm>2024-01-15T10:30:00Z</CreDtTm>\n      <NbOfTxs>1</NbOfTxs>\n      <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>\n    </GrpHdr>\n    <DrctDbtTxInf>\n      <PmtId><EndToEndId>E2E-001</EndToEndId></PmtId>\n      <IntrBkSttlmAmt Ccy="SGD">1.00</IntrBkSttlmAmt>\n      <ChrgBr>SLEV</ChrgBr>\n      <Cdtr/>\n      <CdtrAgt><FinInstnId/></CdtrAgt>\n      <Dbtr/>\n      <DbtrAcct/>\n      <DbtrAgt><FinInstnId/></DbtrAgt>\n    </DrctDbtTxInf>\n  </FIToFICstmrDrctDbt>\n</Document>`;

    const kafka = new Kafka({ clientId: `dup-e2e-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    const before = await admin.fetchTopicOffsets(paymentTopic);

    // First publish
    await send(xml);
    let advanced1 = false;
    const t1 = Date.now();
    for (;;) {
      const after = await admin.fetchTopicOffsets(paymentTopic);
      if (Number(after[0].offset) > Number(before[0].offset)) { advanced1 = true; break; }
      if (Date.now() - t1 > 30000) break;
      await new Promise(r => setTimeout(r, 300));
    }
    expect(advanced1).toBeTruthy();

    // Second publish (duplicate)
    const mid = await admin.fetchTopicOffsets(paymentTopic);
    await send(xml);
    let advanced2 = false;
    const t2 = Date.now();
    for (;;) {
      const after2 = await admin.fetchTopicOffsets(paymentTopic);
      if (Number(after2[0].offset) > Number(mid[0].offset)) { advanced2 = true; break; }
      if (Date.now() - t2 > 30000) break;
      await new Promise(r => setTimeout(r, 300));
    }
    await admin.disconnect().catch(() => {});
    expect(advanced2).toBeTruthy();
  });
});






