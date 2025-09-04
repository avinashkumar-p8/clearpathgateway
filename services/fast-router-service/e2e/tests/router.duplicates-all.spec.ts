import { test, expect } from '@playwright/test';
import { Kafka } from 'kafkajs';
import * as avsc from 'avsc';
import path from 'path';

async function readAvroSchema(schemaPath: string): Promise<avsc.Type> {
  const fs = await import('fs/promises');
  const schemaContent = await fs.readFile(schemaPath, 'utf-8');
  return avsc.Type.forSchema(JSON.parse(schemaContent));
}

function decodeAvroMessage(buffer: Buffer, schema: avsc.Type): any {
  try { return schema.fromBuffer(buffer); } catch { return null; }
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
    const consumer = kafka.consumer({ groupId: `dup-all-exc-${process.pid}-${Math.random().toString(36).slice(2,8)}` });
    await consumer.connect();
    await consumer.subscribe({ topic: exceptionTopic });
    const received: any[] = [];
    const done = new Promise<boolean>(resolve => {
      const t = setTimeout(() => resolve(true), 12000);
      consumer.run({
        eachMessage: async ({ message }) => {
          if (!message.value) return;
          const d = decodeAvroMessage(message.value as Buffer, exceptionSchema);
          if (d && d.puid && d.originalXml) received.push(d);
        }
      });
    });
    await send(xml);
    await new Promise(r => setTimeout(r, 800));
    await send(xml); // duplicate
    await done;
    await consumer.disconnect();
    expect(received.length).toBeGreaterThanOrEqual(1);
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


