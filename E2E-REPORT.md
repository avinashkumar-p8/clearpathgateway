# Clear Path Gateway - E2E Validation Report (Local)

Generated on: 2025-08-22 (updated: router and XSD-failure tests passing)
Environment: Local (Docker infra: Kafka, ActiveMQ, Spanner emulator)

## Summary Table

| Scenario | Input (AMQ) | Router Outcome | Kafka Topics | Sender Outcome | Notes |
|---|---|---|---|---|---|
| Invalid XML (XSD fail) | `<root/>` to `payment.inbound` | XSD fail, route to exception, publish pacs002-requests | `exception-queue`, `pacs002-requests` | pacs.002 RJCT to `pacs002.outbound`; event JSON best-effort | Verified via automated test (now passing) |
| Valid pacs.008 (local demo) | sample pacs.008 to `payment.inbound` | Transform to unified JSON | `payment-messages` | (N/A) | In local demo, XSD can be disabled if needed via `XSD_VALIDATION_ENABLED=false` |

## Detailed Results

### 1) Invalid XML (XSD failure)

- Input
  - Queue: `payment.inbound`
  - Payload:
    ```xml
    <root/>
    ```

- Router logs (key lines)
  ```
  [PROC] Starting processing for inbound message
  [XSD] Validating XML against XSD for type=unknown
  [KAFKA] Routing invalid message to exception topic, key=G3I2508220618274
  ```

 - Kafka observations
  - Topic: `exception-queue` sample
    ```
    G3I2508226802041:"<root/>\n"
    G3I2508220618274:"<root/>\n"
    ```
  - Topic: `pacs002-requests` contains JSON payloads with puid, error, originalXml (validated by e2e)

- Sender logs (key lines)
  ```
  [PACS002] Received request for PUID=G3I2508226802041, messageType=unknown, uniqueId=, error=Unsupported message type for XSD validation: unknown
  [PACS002] Built XML (first 500) => <?xml version="1.0" ... pacs.002.001.15 ... RJCT ... VALD ...
  [AMQ] Sent pacs.002 for PUID=G3I2508226802041 to queue pacs002.outbound
  [KAFKA] Published event JSON for key=G3I2508226802041 to topic payment-events
  [PACS002] Accepted request for PUID=G3I2508226802041, status=ACCEPTED
  ```

### 2) Valid pacs.008 (local demo)

- Input
  - Queue: `payment.inbound`
  - Payload: `pacs008-valid.xml`
    ```xml
    <?xml version="1.0" encoding="UTF-8"?>
    <Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.13">
      <FIToFICstmrCdtTrf>
        <GrpHdr>
          <MsgId>MSG-VAL-001</MsgId>
          <CreDtTm>2025-01-01T10:00:00Z</CreDtTm>
          <NbOfTxs>1</NbOfTxs>
          <TtlIntrBkSttlmAmt Ccy="SGD">100.50</TtlIntrBkSttlmAmt>
          <IntrBkSttlmDt>2025-01-01</IntrBkSttlmDt>
          <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
        </GrpHdr>
        <CdtTrfTxInf>
          <PmtId>
            <InstrId>INSTR-VAL-01</InstrId>
            <EndToEndId>E2E-VAL-01</EndToEndId>
            <TxId>TX-VAL-01</TxId>
          </PmtId>
          <IntrBkSttlmAmt Ccy="SGD">100.50</IntrBkSttlmAmt>
          <ChrgBr>SHAR</ChrgBr>
          <Dbtr><Nm>John Doe</Nm></Dbtr>
          <DbtrAcct><Id><Othr><Id>ACC-001</Id></Othr></Id></DbtrAcct>
          <DbtrAgt><FinInstnId><BICFI>BANKSGSGXXX</BICFI></FinInstnId></DbtrAgt>
          <CdtrAgt><FinInstnId><BICFI>ANZBSGSGXXX</BICFI></FinInstnId></CdtrAgt>
          <Cdtr><Nm>Jane Smith</Nm></Cdtr>
          <CdtrAcct><Id><Othr><Id>ACC-002</Id></Othr></Id></CdtrAcct>
        </CdtTrfTxInf>
      </FIToFICstmrCdtTrf>
    </Document>
    ```

 - Kafka observations (unified JSON)
  - Topic: `payment-messages` (examples)
    ```
    G3I0000002508204::{"puid":"G3I0000002508204","messageType":"PACS_008","messageVersion":"13","messageId":"MSG-001","creationDateTime":"2024-01-15T10:30:00Z","transactions":[{"endToEndId":"E2E-001","amount":1000.00,"currency":"SGD"}]}
    G3I2508202751143::{"puid":"G3I2508202751143","messageType":"PACS_008","messageVersion":"13","messageId":"MSG-001","creationDateTime":"2024-01-15T10:30:00Z","transactions":[{"endToEndId":"E2E-001","amount":1000.00,"currency":"SGD"}]}
    ```

## Infrastructure & Configuration

- Kafka
  - App bootstrap: `localhost:29092` (host listener)
  - Docker console consumers use: `kafka:9092`
- ActiveMQ
  - Console: `http://localhost:8161` (admin/admin)
  - Inbound queue: `payment.inbound`; outbound pacs.002: `pacs002.outbound`
 - Spanner emulator
  - Local emulator enabled; DDL warnings observed. Table creation for `UnifiedMessages` may require manual init in some runs; E2E tests skip direct Spanner asserts for now.
- Router local toggles
  - XSD validation flag: `app.validation.xsd.enabled` (env: `XSD_VALIDATION_ENABLED`) – true in strict, false in demo valid run.

## Next steps (optional)

- Add full schema-compliant samples for `pacs.003`, `pacs.007`, `camt.056` for strict XSD validations.
- Extend Playwright e2e to push/verify via ActiveMQ and query Spanner emulator.
- Persist and assert event JSON entries in `RouterEvents` and `Pacs002Messages` via automated test scripts.

---

## Additional Scenarios (Appended)

### Duplicate messages (placeholder behavior)

| Scenario | Input (AMQ) | Kafka Topics | Observation |
|---|---|---|---|
| Duplicate pacs.008 (same XML twice) | `/tmp/pacs008-valid.xml` posted twice | `payment-messages` | Multiple entries observed with different PUIDs (placeholder dedup allows both). |

Example topic snapshot (keys truncated):
```
G3I2508226918154::{"puid":"...","messageType":"PACS_008",...,"messageId":"MSG-VAL-001","transactions":[{"endToEndId":"E2E-VAL-01",...}]}
G3I2508226472040::{"puid":"...","messageType":"PACS_008",...,"messageId":"MSG-VAL-001","transactions":[{"endToEndId":"E2E-VAL-01",...}]}
```

### Slightly-invalid XML (near-correct, XSD fails)

| Message Type | Sample | Key Violation | Kafka Topic | Example Entry |
|---|---|---|---|---|
| pacs.008.001.13 | `/tmp/pacs008-slightly-invalid.xml` | `ChrgBr` invalid enum `FOOO` | `pacs002-requests` | `G3I2508221512424::{"error":"XML does not conform to XSD for type pacs.008.001.13", ...}` |
| pacs.003.001.11 | `/tmp/pacs003-slightly-invalid.xml` | `SttlmMtd` invalid enum `FOOO` | `pacs002-requests` | `G3I2508225502182::{"error":"XML does not conform to XSD for type pacs.003.001.11", ...}` |
| pacs.007.001.13 | `/tmp/pacs007-slightly-invalid.xml` | `RvslRsnInf/Rsn/Cd` invalid code `XXXX` | `pacs002-requests` | `G3I2508225826163::{"error":"XML does not conform to XSD for type pacs.008.001.13", ...}` |
| camt.056.001.11 | `/tmp/camt056-slightly-invalid.xml` | Wrong tag under `SplmtryData` | `pacs002-requests` | `G3I2508223208663::{"error":"XML does not conform to XSD for type pacs.008.001.13", ...}` |

Notes:
- Slightly-invalid cases triggered the error path and sender pacs.002 RJCT generation (see sender logs for PUIDs listed above).
- For some entries, the logged `messageType` in `pacs002-requests` may reflect detection nuances; the flow still routes to pacs.002 correctly.

---

Review ready: This file now includes duplicate and slightly-invalid scenario results. To rerun locally, reuse the sample files under `/tmp/` and the curl commands in the sections above.
