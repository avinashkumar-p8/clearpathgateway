import { test, expect } from '@playwright/test';
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

test.describe('Duplicate flows across message types (invalid path via exception topic)', () => {
  const healthUrl = process.env.ROUTER_HEALTH_URL || 'http://localhost:8080/health';
  const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
  const exceptionTopic = process.env.EXCEPTION_TOPIC || 'exception-queue';
  const activemqApi = process.env.ACTIVEMQ_API || 'http://localhost:8161/api/message';
  const inboundQ = process.env.ACTIVEMQ_INBOUND || 'payment.inbound';

  let exceptionSchema: avsc.Type;
  test.beforeAll(async () => {
    const schemaPath = path.resolve(__dirname, '../../src/main/resources/avro/router-exception.avsc');
    exceptionSchema = await readAvroSchema(schemaPath);
    const ok = await waitForHealth(healthUrl, 30000);
    expect(ok).toBeTruthy();
  });

  async function send(xml: string) {
    const basic = Buffer.from(`${process.env.ACTIVEMQ_USERNAME||'admin'}:${process.env.ACTIVEMQ_PASSWORD||'admin'}`).toString('base64');
    await fetch(`${activemqApi}/${inboundQ}?type=queue`, {
      method: 'POST',
      headers: { 'Authorization': `Basic ${basic}`, 'Content-Type': 'text/plain' },
      body: xml,
    });
  }

  async function verifyDuplicateInvalid(xml: string) {
    const health = await fetch(healthUrl);
    expect(health.ok).toBeTruthy();
    const kafka = new Kafka({ clientId: `dup-all-${process.pid}`, brokers });
    // Snapshot exception-topic offsets
    const admin = kafka.admin();
    await admin.connect();
    const before = await admin.fetchTopicOffsets(exceptionTopic);
    await admin.disconnect().catch(() => {});
    // Send twice
    await send(xml);
    await new Promise(r => setTimeout(r, 800));
    await send(xml);
    // Assert offset advanced
    const admin2 = kafka.admin();
    await admin2.connect();
    const start = Date.now();
    let advanced = false;
    for (;;) {
      const after = await admin2.fetchTopicOffsets(exceptionTopic);
      if (Number(after[0].offset) > Number(before[0].offset)) { advanced = true; break; }
      if (Date.now() - start > 15000) break;
      await new Promise(r => setTimeout(r, 300));
    }
    await admin2.disconnect().catch(() => {});
    expect(advanced).toBeTruthy();
  }

  test('pacs.003 duplicate flow invalid -> exception topic', async () => {
    const xml = `<?xml version="1.0" encoding="UTF-8"?>\n<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11">\n  <FIToFICstmrDrctDbt>\n    <GrpHdr>\n      <MsgId>DUP-003-001</MsgId>\n      <CreDtTm>2024-01-15T10:30:00Z</CreDtTm>\n      <NbOfTxs>1</NbOfTxs>\n    </GrpHdr>\n  </FIToFICstmrDrctDbt>\n</Document>`;
    await verifyDuplicateInvalid(xml);
  });

  test('pacs.007 duplicate flow invalid -> exception topic', async () => {
    const xml = `<?xml version="1.0" encoding="UTF-8"?>\n<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.007.001.13">\n  <FIToFIPmtRvsl>\n    <GrpHdr>\n      <MsgId>DUP-007-001</MsgId>\n      <CreDtTm>2024-01-15T10:30:00Z</CreDtTm>\n    </GrpHdr>\n  </FIToFIPmtRvsl>\n</Document>`;
    await verifyDuplicateInvalid(xml);
  });

  test('camt.056 duplicate flow invalid -> exception topic', async () => {
    const xml = `<?xml version="1.0" encoding="UTF-8"?>\n<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.056.001.11">\n  <FIToFIPmtCxlReq>\n    <Assgnmt><Id>DUP-056-001</Id></Assgnmt>\n  </FIToFIPmtCxlReq>\n</Document>`;
    await verifyDuplicateInvalid(xml);
  });
});


