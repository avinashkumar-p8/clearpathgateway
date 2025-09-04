import { test, expect } from '@playwright/test';
import { Kafka } from 'kafkajs';
import * as avsc from 'avsc';
import path from 'path';

async function readAvroSchema(schemaPath: string): Promise<avsc.Type> {
  const fs = await import('fs/promises');
  const schemaContent = await fs.readFile(schemaPath, 'utf-8');
  return avsc.Type.forSchema(JSON.parse(schemaContent));
}

function decodeAvroMessage(messageBuffer: Buffer, schema: avsc.Type): any {
  try {
    return schema.fromBuffer(messageBuffer);
  } catch {
    return null;
  }
}

test.describe('Fast Router Service E2E', () => {
  const healthUrl = process.env.ROUTER_HEALTH_URL || 'http://localhost:8080/health';
  const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
  const paymentTopic = process.env.PAYMENT_MESSAGES_TOPIC || 'payment-messages';
  const activemqApi = process.env.ACTIVEMQ_API || 'http://localhost:8161/api/message';

  let schema: avsc.Type;

  test.beforeAll(async () => {
    const schemaPath = path.resolve(__dirname, '../../src/main/resources/avro/unified-payment-message.avsc');
    schema = await readAvroSchema(schemaPath);
  });

  test('health and PACS.008 happy flow', async () => {
    const health = await fetch(healthUrl);
    expect(health.ok).toBeTruthy();

    const kafka = new Kafka({ clientId: `router-e2e-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    const parts = await admin.fetchTopicOffsets(paymentTopic);
    const endOffsets = parts.map(p => ({ partition: p.partition, offset: Number(p.offset) }));
    await admin.disconnect().catch(() => {});

    const consumer = kafka.consumer({ groupId: `router-e2e-${process.pid}-${Math.random().toString(36).slice(2,8)}` });
    await consumer.connect();
    await consumer.subscribe({ topic: paymentTopic, fromBeginning: true });

    let joined = false;
    let joinedResolve: (() => void) | null = null;
    const joinedPromise = new Promise<void>(res => { joinedResolve = res; });
    consumer.on(consumer.events.GROUP_JOIN, async () => {
      if (joined) return; joined = true;
      for (const p of endOffsets) {
        try { await consumer.seek({ topic: paymentTopic, partition: p.partition, offset: String(p.offset) }); } catch {}
      }
      if (joinedResolve) joinedResolve();
    });

    let received: any | null = null;
    const got = new Promise<boolean>((resolve) => {
      const timeoutId = setTimeout(async () => { resolve(false); }, 45000);
      consumer.run({
        eachMessage: async ({ topic, message }) => {
          if (!message.value) return;
          const decoded = decodeAvroMessage(message.value as Buffer, schema);
          if (topic === paymentTopic && decoded && decoded.messageType === 'PACS_008') {
            received = decoded;
            clearTimeout(timeoutId);
            resolve(true);
          }
        }
      });
    });

    await joinedPromise; // ensure we seeked to end before producing

    const validXml = `<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13">
  <FIToFICstmrCdtTrf>
    <GrpHdr>
      <MsgId>MSG-E2E-ROUTER-001</MsgId>
      <CreDtTm>2024-01-15T10:30:00Z</CreDtTm>
      <NbOfTxs>1</NbOfTxs>
      <CtrlSum>1000.00</CtrlSum>
      <IntrBkSttlmDt>2024-01-16</IntrBkSttlmDt>
      <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
    </GrpHdr>
    <CdtTrfTxInf>
      <PmtId><EndToEndId>E2E-ROUTER-001</EndToEndId></PmtId>
      <IntrBkSttlmAmt Ccy="SGD">1000.00</IntrBkSttlmAmt>
      <ChrgBr>SHAR</ChrgBr>
      <Dbtr><Nm>John Doe</Nm></Dbtr>
      <DbtrAcct><Id><Othr><Id>ACC-ROUTER-DBTR</Id></Othr></Id></DbtrAcct>
      <DbtrAgt><FinInstnId><BICFI>DBSSSGSGXXX</BICFI></FinInstnId></DbtrAgt>
      <CdtrAgt><FinInstnId><BICFI>ANZBSGSGXXX</BICFI></FinInstnId></CdtrAgt>
      <Cdtr><Nm>Jane Smith</Nm></Cdtr>
      <CdtrAcct><Id><Othr><Id>ACC-ROUTER-CDTR</Id></Othr></Id></CdtrAcct>
    </CdtTrfTxInf>
  </FIToFICstmrCdtTrf>
</Document>`;

    const q = process.env.ACTIVEMQ_INBOUND || 'payment.inbound';
    const basic = Buffer.from(`${process.env.ACTIVEMQ_USERNAME||'admin'}:${process.env.ACTIVEMQ_PASSWORD||'admin'}`).toString('base64');
    await fetch(`${activemqApi}/${q}?type=queue`, {
      method: 'POST',
      headers: {
        'Authorization': `Basic ${basic}`,
        'Content-Type': 'text/plain'
      },
      body: validXml
    });

    const ok = await got;
    await consumer.stop();
    await consumer.disconnect();
    expect(ok).toBeTruthy();

    expect(received.messageType).toBe('PACS_008');
    expect(received.messageId).toBeTruthy();
    expect(received.creationDateTime).toBeTruthy();
    expect(typeof received.supplementaryData).toBe('object');
    expect(received.supplementaryData.rawUnifiedJson).toBeTruthy();
    const rawUnifiedJson = JSON.parse(received.supplementaryData.rawUnifiedJson);
    expect(rawUnifiedJson.messageType).toBe('PACS_008');
    expect(rawUnifiedJson.messageId).toBe('MSG-E2E-ROUTER-001');
    expect(Array.isArray(rawUnifiedJson.transactions)).toBeTruthy();
  });
});


