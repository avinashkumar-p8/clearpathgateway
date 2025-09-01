import { test, expect, request } from '@playwright/test';
import { spannerClient } from './helpers/spanner';
import { Spanner } from '@google-cloud/spanner';

// Skeleton: assumes system under test already produced valid flow records

test('valid flow persists unified JSON (Spanner) end-to-end', async () => {
  // 1) Send valid XML to ActiveMQ
  const activemqUrl = process.env.ACTIVEMQ_ADMIN_URL || 'http://localhost:8161';
  const basic = 'Basic ' + Buffer.from(`${process.env.ACTIVEMQ_USERNAME || 'admin'}:${process.env.ACTIVEMQ_PASSWORD || 'admin'}`).toString('base64');
  const mqReq = await request.newContext();
  const validXml = `<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13">
  <FIToFICstmrCdtTrf>
    <GrpHdr>
      <MsgId>MSG-E2E-VAL-001</MsgId>
      <CreDtTm>2025-01-01T10:00:00Z</CreDtTm>
      <NbOfTxs>1</NbOfTxs>
      <TtlIntrBkSttlmAmt Ccy="SGD">100.50</TtlIntrBkSttlmAmt>
      <IntrBkSttlmDt>2025-01-01</IntrBkSttlmDt>
      <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
    </GrpHdr>
    <CdtTrfTxInf>
      <PmtId><EndToEndId>E2E-E2E-VAL-01</EndToEndId></PmtId>
      <IntrBkSttlmAmt Ccy="SGD">100.50</IntrBkSttlmAmt>
      <ChrgBr>SHAR</ChrgBr>
      <Dbtr><Nm>John Doe</Nm></Dbtr>
      <DbtrAcct><Id><Othr><Id>ACC-001</Id></Othr></Id></DbtrAcct>
      <Cdtr><Nm>Jane Smith</Nm></Cdtr>
      <CdtrAcct><Id><Othr><Id>ACC-002</Id></Othr></Id></CdtrAcct>
    </CdtTrfTxInf>
  </FIToFICstmrCdtTrf>
</Document>`;
  const post = await mqReq.post(`${activemqUrl}/api/message/payment.inbound?type=queue`, {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': basic },
    form: { body: validXml }
  });
  expect(post.ok()).toBeTruthy();

  // 2) Poll Spanner for UnifiedMessages row appearing
  const projectId = process.env.GCP_PROJECT_ID || 'local-project';
  const instanceId = process.env.SPANNER_INSTANCE || 'payment-gateway-local';
  const databaseId = process.env.SPANNER_DATABASE || 'router-db';
  const db = spannerClient(projectId, instanceId, databaseId);

  let found = false;
  const started = Date.now();
  while (Date.now() - started < 15000 && !found) {
    try {
      const rows = await db.query('SELECT puid, message_type, created_at FROM UnifiedMessages LIMIT 1');
      if (rows.length > 0) { found = true; break; }
    } catch (e: any) {
      const msg = String(e?.message || e);
      if (msg.includes('Table not found: UnifiedMessages') && process.env.SPANNER_EMULATOR_HOST) {
        // Create table on the fly (local emulator only)
        const sp = new Spanner({ projectId });
        const database = sp.instance(instanceId).database(databaseId);
        try {
          await database.updateSchema([`CREATE TABLE UnifiedMessages (
            puid STRING(16) NOT NULL,
            message_type STRING(64),
            created_at TIMESTAMP,
            json STRING(MAX)
          ) PRIMARY KEY (puid)`]);
        } finally {
          await database.close();
        }
      } else {
        throw e;
      }
    }
    await new Promise(r => setTimeout(r, 750));
  }
  await db.close();
  expect(found).toBeTruthy();
});
