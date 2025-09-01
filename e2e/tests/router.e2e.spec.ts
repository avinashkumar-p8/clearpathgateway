import { test, expect, request } from '@playwright/test';
import { Kafka } from 'kafkajs';

test.describe('Fast Router Service E2E', () => {
  const activemqUrl = 'http://localhost:8161';
  const healthUrl = 'http://localhost:8080/health';

  const validXml = `<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13">
  <FIToFICstmrCdtTrf>
    <GrpHdr>
      <MsgId>MSG-IT</MsgId>
      <CreDtTm>2024-01-15T10:30:00Z</CreDtTm>
      <NbOfTxs>1</NbOfTxs>
      <CtrlSum>1000.00</CtrlSum>
      <TtlIntrBkSttlmAmt Ccy="SGD">1000.00</TtlIntrBkSttlmAmt>
      <IntrBkSttlmDt>2024-01-15</IntrBkSttlmDt>
      <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
    </GrpHdr>
    <CdtTrfTxInf>
      <PmtId><EndToEndId>E2E-IT-001</EndToEndId></PmtId>
      <IntrBkSttlmAmt Ccy="SGD">1000.00</IntrBkSttlmAmt>
      <ChrgBr>SHAR</ChrgBr>
      <Dbtr><Nm>John Doe</Nm></Dbtr>
      <DbtrAcct><Id><Othr><Id>123456789</Id></Othr></Id></DbtrAcct>
      <DbtrAgt><FinInstnId><BICFI>AAAAUS33</BICFI></FinInstnId></DbtrAgt>
      <CdtrAgt><FinInstnId><BICFI>BBBBUS33</BICFI></FinInstnId></CdtrAgt>
      <Cdtr><Nm>Jane Smith</Nm></Cdtr>
      <CdtrAcct><Id><Othr><Id>987654321</Id></Othr></Id></CdtrAcct>
    </CdtTrfTxInf>
  </FIToFICstmrCdtTrf>
</Document>`;

  test('health and message flow', async ({ page }) => {
    // Health endpoint is up
    const h = await fetch(healthUrl);
    expect(h.ok).toBeTruthy();

    // Post XML to ActiveMQ REST (Classic REST plugin)
    const mqReq = await request.newContext({ httpCredentials: { username: 'admin', password: 'admin' } });
    const basic = 'Basic ' + Buffer.from('admin:admin').toString('base64');
    const resp = await mqReq.post(`${activemqUrl}/api/message?destination=queue://payment.inbound`, {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': basic },
      form: { body: validXml }
    });
    expect(resp.ok()).toBeTruthy();

    // Consume from Kafka payment-messages to confirm flow
    const brokers = (process.env.KAFKA_BROKERS || 'localhost:29092').split(',');
    const uniqueGroup = `e2e-router-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
    const kafka = new Kafka({ clientId: uniqueGroup, brokers });
    const consumer = kafka.consumer({ groupId: uniqueGroup });
    await consumer.connect();
    await consumer.subscribe({ topic: process.env.PAYMENT_MESSAGES_TOPIC || 'payment-messages', fromBeginning: false });
    await consumer.subscribe({ topic: process.env.EXCEPTION_TOPIC || 'exception-queue', fromBeginning: false });
    const waitForMessage = new Promise<boolean>((resolve) => {
      consumer.run({
        eachMessage: async ({ topic, message }) => {
          if (!message.value) return;
          const raw = message.value.toString();
          try {
            const obj = JSON.parse(raw);
            if (topic === (process.env.PAYMENT_MESSAGES_TOPIC || 'payment-messages') && obj && obj.messageType && obj.puid) {
              resolve(true);
            }
          } catch {
            // ignore parse errors; we only assert payment-messages JSON path here
          }
        },
      });
    });
    const timeout = new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 15000));
    const ok = await Promise.race([waitForMessage, timeout]);
    await consumer.stop();
    await consumer.disconnect();
    expect(ok).toBeTruthy();
  });
});


