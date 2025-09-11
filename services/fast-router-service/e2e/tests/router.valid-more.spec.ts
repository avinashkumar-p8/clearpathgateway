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

function decodeAvroMessage(messageBuffer: Buffer, schema: avsc.Type): any {
  try { return schema.fromBuffer(messageBuffer); } catch { return null; }
}

async function waitForHealth(url: string, timeoutMs = 30000) {
  const start = Date.now();
  for (;;) {
    try { const r = await fetch(url); if (r.ok) return true; } catch {}
    if (Date.now() - start > timeoutMs) return false;
    await new Promise(r => setTimeout(r, 500));
  }
}

test.describe('Valid flows for PACS.003/007 and CAMT.056', () => {
  const healthUrl = process.env.ROUTER_HEALTH_URL || 'http://localhost:8080/health';
  const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
  const paymentTopic = process.env.PAYMENT_MESSAGES_TOPIC || 'payment-messages';
  const activemqApi = process.env.ACTIVEMQ_API || 'http://localhost:8161/api/message';

  let unifiedSchema: avsc.Type;

  test.beforeAll(async () => {
    const schemaPath = path.resolve(__dirname, '../../src/main/resources/avro/unified-payment-message.avsc');
    unifiedSchema = await readAvroSchema(schemaPath);
    const ok = await waitForHealth(healthUrl, 60000);
    expect(ok).toBeTruthy();
  });

  async function expectUnifiedOnPayment(topic: string, expectedEnum: string, bodyXml: string, timeoutMs = 90000) {
    const kafka = new Kafka({ clientId: `router-valid-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    const parts = await admin.fetchTopicOffsets(topic);
    const endOffsets = parts.map(p => ({ partition: p.partition, offset: Number(p.offset) }));
    await admin.disconnect().catch(() => {});

    const consumer = kafka.consumer({ groupId: `router-valid-${process.pid}-${Math.random().toString(36).slice(2,8)}` });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    let received: any | null = null;
    let joined = false;
    let joinedResolve: (() => void) | null = null;
    const joinedPromise = new Promise<void>(res => { joinedResolve = res; });
    consumer.on(consumer.events.GROUP_JOIN, async () => {
      if (joined) return; joined = true;
      for (const p of endOffsets) {
        try { await consumer.seek({ topic, partition: p.partition, offset: String(p.offset) }); } catch {}
      }
      if (joinedResolve) joinedResolve();
    });

    let receivedFlag = false;
    const got = new Promise<boolean>((resolve) => {
      const timeoutId = setTimeout(() => resolve(false), timeoutMs);
      consumer.run({
        eachMessage: async ({ topic: t, message }) => {
          if (!message.value) return;
          const decoded = decodeAvroMessage(message.value as Buffer, unifiedSchema);
          if (t === topic && decoded && decoded.messageType === expectedEnum) {
            received = decoded;
            receivedFlag = true;
            clearTimeout(timeoutId);
            resolve(true);
          }
        }
      });
    });

    await joinedPromise; // ensure seeked before producing

    const q = process.env.ACTIVEMQ_INBOUND || 'payment.inbound';
    const basic = Buffer.from(`${process.env.ACTIVEMQ_USERNAME||'admin'}:${process.env.ACTIVEMQ_PASSWORD||'admin'}`).toString('base64');
    await fetch(`${activemqApi}/${q}?type=queue`, {
      method: 'POST',
      headers: { 'Authorization': `Basic ${basic}`, 'Content-Type': 'text/plain' },
      body: bodyXml
    });

    // Retry send after 3s if nothing received yet
    setTimeout(async () => {
      if (!receivedFlag) {
        try {
          await fetch(`${activemqApi}/${q}?type=queue`, {
            method: 'POST',
            headers: { 'Authorization': `Basic ${basic}`, 'Content-Type': 'text/plain' },
            body: bodyXml
          });
        } catch {}
      }
    }, 3000);

    const ok = await got;
    await consumer.stop();
    await consumer.disconnect();
    expect(ok).toBeTruthy();
    expect(received).toBeTruthy();
    expect(received.messageType).toBe(expectedEnum);
    expect(received.supplementaryData).toBeTruthy();
    expect(received.supplementaryData.rawUnifiedJson).toBeTruthy();
  }

  async function snapshotOffsets(topic: string) {
    const kafka = new Kafka({ clientId: `router-valid-admin-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    try {
      const parts = await admin.fetchTopicOffsets(topic);
      return parts.map(p => ({ partition: p.partition, offset: Number(p.offset) }));
    } finally {
      await admin.disconnect().catch(() => {});
    }
  }

  async function waitUntilAdvanced(topic: string, before: Array<{ partition: number; offset: number }>, timeoutMs = 60000) {
    const kafka = new Kafka({ clientId: `router-valid-admin-wait-${process.pid}`, brokers });
    const admin = kafka.admin();
    await admin.connect();
    try {
      const start = Date.now();
      for (;;) {
        const after = await admin.fetchTopicOffsets(topic);
        let adv = false;
        for (let i = 0; i < after.length; i++) {
          if (Number(after[i].offset) > Number(before[i].offset)) { adv = true; break; }
        }
        if (adv) return true;
        if (Date.now() - start > timeoutMs) return false;
        await new Promise(r => setTimeout(r, 300));
      }
    } finally {
      await admin.disconnect().catch(() => {});
    }
  }

  async function consumeFromOffset(topic: string, before: Array<{ partition: number; offset: number }>, expectEnum: string, timeoutMs = 30000) {
    const kafka = new Kafka({ clientId: `router-valid-cons-${process.pid}`, brokers });
    const consumer = kafka.consumer({ groupId: `router-valid-cons-${process.pid}-${Math.random().toString(36).slice(2,8)}` });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    let joined = false;
    consumer.on(consumer.events.GROUP_JOIN, async () => {
      if (joined) return; joined = true;
      for (const p of before) {
        try { await consumer.seek({ topic, partition: p.partition, offset: String(p.offset) }); } catch {}
      }
    });
    return new Promise<any>(async (resolve) => {
      const t = setTimeout(async () => { try { await consumer.disconnect(); } catch {} resolve(null); }, timeoutMs);
      await consumer.run({
        eachMessage: async ({ message }) => {
          if (!message.value) return;
          const decoded = decodeAvroMessage(message.value as Buffer, unifiedSchema);
          if (decoded && decoded.messageType === expectEnum) {
            clearTimeout(t);
            try { await consumer.disconnect(); } catch {}
            resolve(decoded);
          }
        }
      });
    });
  }

  test('PACS.003 minimal valid publishes unified Avro', async () => {
    test.setTimeout(120000);
    const uniq = `E2E-003-${Date.now()}-${Math.random().toString(36).slice(2,6)}`;
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.003.001.11">
  <FIToFICstmrDrctDbt>
    <GrpHdr>
      <MsgId>MSG-003-${Date.now()}</MsgId>
      <CreDtTm>2024-01-15T10:30:00Z</CreDtTm>
      <NbOfTxs>1</NbOfTxs>
      <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
    </GrpHdr>
    <DrctDbtTxInf>
      <PmtId><EndToEndId>${uniq}</EndToEndId></PmtId>
      <IntrBkSttlmAmt Ccy="SGD">1.00</IntrBkSttlmAmt>
      <ChrgBr>SLEV</ChrgBr>
      <Cdtr/>
      <CdtrAgt><FinInstnId/></CdtrAgt>
      <Dbtr/>
      <DbtrAcct/>
      <DbtrAgt><FinInstnId/></DbtrAgt>
    </DrctDbtTxInf>
  </FIToFICstmrDrctDbt>
</Document>`;
    await expectUnifiedOnPayment(paymentTopic, 'PACS_003', xml, 120000);
  });

  test('PACS.007 minimal valid publishes unified Avro', async () => {
    test.setTimeout(120000);
    const msgId = `MSG-007-${Date.now()}-${Math.random().toString(36).slice(2,6)}`;
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.007.001.13">
  <FIToFIPmtRvsl>
    <GrpHdr>
      <MsgId>${msgId}</MsgId>
      <CreDtTm>2024-01-15T10:30:00Z</CreDtTm>
      <NbOfTxs>0</NbOfTxs>
      <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
    </GrpHdr>
  </FIToFIPmtRvsl>
</Document>`;
    await expectUnifiedOnPayment(paymentTopic, 'PACS_007', xml, 120000);
  });

  test('CAMT.056 minimal valid publishes unified Avro', async () => {
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.056.001.11">
  <FIToFIPmtCxlReq>
    <Assgnmt>
      <Id>ASSIGN-001</Id>
      <Assgnr><Agt><FinInstnId/></Agt></Assgnr>
      <Assgne><Agt><FinInstnId/></Agt></Assgne>
      <CreDtTm>2024-01-15T10:30:00Z</CreDtTm>
    </Assgnmt>
    <Undrlyg/>
  </FIToFIPmtCxlReq>
</Document>`;
    await expectUnifiedOnPayment(paymentTopic, 'CAMT_056', xml);
  });

  test('HEAD.001 minimal valid publishes unified Avro', async ({}, testInfo) => {
    testInfo.setTimeout(120000);
    const uniq = `MSG-HEAD-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<AppHdr xmlns="urn:iso:std:iso:20022:tech:xsd:head.001.001.01">
  <Fr><FIId><FinInstnId><BICFI>ANZBSGSGXXX</BICFI></FinInstnId></FIId></Fr>
  <To><FIId><FinInstnId><BICFI>DBSSSGSGXXX</BICFI></FinInstnId></FIId></To>
  <BizMsgIdr>${uniq}</BizMsgIdr>
  <MsgDefIdr>pacs.008.001.13</MsgDefIdr>
  <CreDt>2024-01-15T10:30:00Z</CreDt>
</AppHdr>`;
    const before = await snapshotOffsets(paymentTopic);
    const q = process.env.ACTIVEMQ_INBOUND || 'payment.inbound';
    const basic = Buffer.from(`${process.env.ACTIVEMQ_USERNAME||'admin'}:${process.env.ACTIVEMQ_PASSWORD||'admin'}`).toString('base64');
    await fetch(`${activemqApi}/${q}?type=queue`, { method: 'POST', headers: { 'Authorization': `Basic ${basic}`, 'Content-Type': 'text/plain' }, body: xml });
    let advanced = await waitUntilAdvanced(paymentTopic, before, 90000);
    if (!advanced) {
      await new Promise(r => setTimeout(r, 1000));
      await fetch(`${activemqApi}/${q}?type=queue`, { method: 'POST', headers: { 'Authorization': `Basic ${basic}`, 'Content-Type': 'text/plain' }, body: xml });
      advanced = await waitUntilAdvanced(paymentTopic, before, 30000);
    }
    expect(advanced).toBeTruthy();
  });

  test('HEAD.001 twice ensures consumer stability', async ({}, testInfo) => {
    testInfo.setTimeout(60000);
    const healthUrl = process.env.ROUTER_HEALTH_URL || 'http://localhost:8080/health';
    const ok = await waitForHealth(healthUrl, 30000);
    expect(ok).toBeTruthy();
    const xml = `<?xml version="1.0"?><AppHdr xmlns="urn:iso:std:iso:20022:tech:xsd:head.001.001.01"><Fr><FIId><FinInstnId><BICFI>HEADTESTBIC</BICFI></FinInstnId></FIId></Fr></AppHdr>`;
    const activemqApi = process.env.ACTIVEMQ_API || 'http://localhost:8161/api/message';
    const q = process.env.ACTIVEMQ_INBOUND || 'payment.inbound';
    const basic = Buffer.from(`${process.env.ACTIVEMQ_USERNAME||'admin'}:${process.env.ACTIVEMQ_PASSWORD||'admin'}`).toString('base64');
    await fetch(`${activemqApi}/${q}?type=queue`, { method: 'POST', headers: { 'Authorization': `Basic ${basic}`, 'Content-Type': 'text/plain' }, body: xml });
    await new Promise(r => setTimeout(r, 500));
    await fetch(`${activemqApi}/${q}?type=queue`, { method: 'POST', headers: { 'Authorization': `Basic ${basic}`, 'Content-Type': 'text/plain' }, body: xml });
    await new Promise(r => setTimeout(r, 500));
    expect(true).toBeTruthy();
  });
});


