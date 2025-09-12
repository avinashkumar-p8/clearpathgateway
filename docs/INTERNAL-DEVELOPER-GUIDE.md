# ClearPath Gateway – Internal Developer Guide (Router & Sender)

This guide is a deep, atomic-level reference for developers working on the two core services:
- Fast Router Service (`services/fast-router-service`)
- Fast Sender Service (`services/fast-sender-service`)

It covers idea, architecture/design, detailed logic, code map, build/test/deploy/operate at scale, and an FAQ.

---

## 1) Idea and Responsibilities

- Router
  - Ingest ISO 20022 XML over JMS (ActiveMQ/IBM MQ)
  - Validate against XSD
  - Extract IDs, determine message type and version
  - Transform to a unified JSON/Avro model and publish to Kafka (`payment-messages`)
  - Publish exception/diagnostic events to `exception-queue` on invalid inputs
  - Current policy for duplicates: log-only (no enforced dedup)

- Sender
  - Consume requests from Kafka (e.g. `pacs002-requests`, `camt029-requests`)
  - Build outbound ISO 20022 XML responses (`pacs.002`, `camt.029`)
  - Publish outbound XML to JMS queues (ActiveMQ) for host integration
  - Emit payment events to Kafka (`payment-events`)
  - Idempotency/persistence checks where implemented; log-only otherwise
  - Retry AMQ send with configurable limits and backoff

---

## 2) Architecture & Design

- Messaging
  - Inbound Router: JMS queue (ActiveMQ/IBM MQ), e.g. `payment.inbound`
  - Outbound Router: Kafka
    - `payment-messages` (unified Avro payload)
    - `exception-queue` (router exceptions)
  - Sender inbound: Kafka
    - `pacs002-requests` (requests for pacs.002)
    - `camt029-requests` (requests for camt.029)
  - Sender outbound: JMS
    - e.g. `pacs002.outbound`, `camt29.outbound`
  - Observability events: Kafka `payment-events`

- Serialization
  - Router unified Avro schema: `services/fast-router-service/src/main/resources/avro/unified-payment-message.avsc`
  - Confluent Avro framing tolerated in tests (magic byte + schema id); plain JSON also accepted in some paths

- Validation & Transformation
  - XSD validation: `XmlSchemaValidator`
  - Type detection: `Iso20022MessageTypeDetector`
  - XML → unified JSON: `Iso20022Transformer` (raw JSON sometimes preserved in supplementary data)

- Retry/Idempotency
  - Router: log-only for duplicates; no blocking
  - Sender: AMQ send retry with attempts/backoff; optional idempotency via repository checks

---

## 3) Detailed Logic (Atomic Level)

### 3.1 Router: JMS Inbound → Kafka Unified Flow

1) Receive JMS message on inbound queue
   - Listener: `AbstractJmsMessageListener` concrete impls (ActiveMQ/IBM MQ)
   - Empty/null payload → warn & skip

2) Detect message type/version
   - `Iso20022MessageTypeDetector` inspects root namespace/elements
   - Supported examples: `pacs.003.001.11`, `pacs.007.001.13`, `pacs.008.001.13`, `camt.056.001.11`, `head.001.001.01`

3) Validate XML against XSD
   - `XmlSchemaValidator` chooses XSD; rejects bad namespace/structure
   - On failure: publish exception event to `exception-queue` and stop normal path

4) Extract IDs
   - `UniqueIdExtractor`: derive message/transaction identifiers (with fallbacks)

5) Transform & publish
   - `Iso20022Transformer` → unified JSON → Avro object
   - Publish to Kafka `payment-messages` via `KafkaPublisher`

6) Duplicates
   - Policy: log-only (tests may see second publish)

### 3.2 Router: Exception Flow

- On validation/transform errors, publish exception record to `exception-queue` with context.

### 3.3 Sender: pacs.002 Flow (Kafka → JMS + Events)

1) Consume `pacs002-requests`
   - `Pacs002RequestConsumer` normalizes Avro-framed or JSON payloads, validates `puid`

2) `Pacs002ServiceImpl`
   - Optional idempotency check
   - Build pacs.002 XML (ACK/NACK semantics based on input/error)
   - Persist entity (where configured)
   - Publish XML to AMQ queue (`pacs002.outbound`) with retries
   - Publish event JSON to `payment-events`

### 3.4 Sender: camt.029 Flow (Kafka → JMS + Events)

1) Consume `camt029-requests`
   - `Camt029RequestConsumer` accepts Avro or JSON; validates `puid`

2) `Camt029ServiceImpl`
   - Extract `OrgnlMsgId` from camt.056 XML or fallback to `puid`
   - Build camt.029 XML and publish to AMQ (retry policy applies)
   - Emit event JSON

---

## 4) Code Map (Key Classes)

- Router
  - `RouterOrchestrator`: orchestrates validate → transform → publish
  - `Iso20022MessageTypeDetector`: type detection
  - `XmlSchemaValidator`: XSD validation
  - `Iso20022Transformer`: XML → unified JSON/Avro
  - `EventPublisher` / `KafkaPublisher`: Kafka output
  - `AbstractJmsMessageListener` (+ `IbmMqMessageListener` / `ActiveMqMessageListener`): JMS ingress
  - `UniqueIdExtractor`: ID derivation

- Sender
  - `Pacs002RequestConsumer`, `Camt029RequestConsumer`: Kafka ingress
  - `Pacs002ServiceImpl`, `Camt029ServiceImpl`: main business logic
  - `KafkaPublisher`, `EventJsonPublisher`: Kafka egress
  - `Pacs002Publisher` (if present): JMS egress helper
  - `KafkaAvroConfig`, `GenericAvroSerializer` (where used): Avro wiring

---

## 5) Configuration & Environments

- Common ENV
  - `KAFKA_BROKERS` (comma-separated)
  - `ACTIVEMQ_API`, `ACTIVEMQ_USERNAME`, `ACTIVEMQ_PASSWORD`

- Router ENV
  - `ROUTER_HEALTH_URL` (default `http://localhost:8080/health`)
  - `PAYMENT_MESSAGES_TOPIC` (default `payment-messages`)
  - `EXCEPTION_TOPIC` (default `exception-queue`)
  - `ACTIVEMQ_INBOUND` (default `payment.inbound`)

- Sender ENV
  - `PACS002_REQUESTS_TOPIC` (default `pacs002-requests`)
  - `PAYMENT_EVENTS_TOPIC` (default `payment-events`)
  - `CAMT029_REQUESTS_TOPIC` (if configured)
  - `PACS002_OUTBOUND_QUEUE`, `CAMT029_OUTBOUND_QUEUE`

- JaCoCo thresholds (BUNDLE level)
  - Router: LINE ≥ 80%, BRANCH ≥ 80%
  - Sender: LINE ≥ 80%, BRANCH ≥ 80%

---

## 6) Build

- Full build + tests (skip ITs):
```bash
mvn -T1C -DskipITs verify
```

- Module-specific:
```bash
mvn -T1C -pl services/fast-router-service -am -DskipITs verify
mvn -T1C -pl services/fast-sender-service -am -DskipITs verify
```

- JaCoCo report:
  - HTML: `target/site/jacoco/index.html`
  - CSV: `target/site/jacoco/jacoco.csv`

---

## 7) Test

### 7.1 Unit Tests
```bash
mvn -T1C -pl services/fast-router-service -am -DskipITs test
mvn -T1C -pl services/fast-sender-service -am -DskipITs test
```

### 7.2 E2E (Playwright)

- Router
```bash
cd services/fast-router-service/e2e
npm ci
npx playwright install --with-deps
npx playwright test --reporter=list
```

- Sender
```bash
cd services/fast-sender-service/e2e
npm ci
npx playwright install --with-deps
npx playwright test --reporter=list
```

- ID Generator (optional)
```bash
cd services/fast-id-generator-service/e2e
npm ci
npx playwright install --with-deps
npx playwright test --reporter=list
```

### 7.3 Test Data & Infra
- Kafka: local broker or Testcontainers
- ActiveMQ: local broker or Docker
- Ensure topics/queues exist (tests often auto-create via admin client)

---

## 8) Deploy

- Jib container builds:
```bash
mvn -pl services/fast-router-service -am jib:build
mvn -pl services/fast-sender-service -am jib:build
```

- Spring Profiles
  - `local`, `dev`, `prod` (externalize broker/queue/topic via env)

- Config/Secrets
  - Broker URLs & credentials
  - Health endpoints for readiness/liveness

---

## 9) Scale

- Both services are stateless; scale horizontally
- Kafka
  - Increase partitions on hot topics; separate consumer groups
- JMS
  - Increase listener concurrency; mind ordering needs
- JVM
  - Tune heap/GC; monitor with metrics

---

## 10) Operate & Manage

- Observability
  - Micrometer + Prometheus (where configured)
  - Structured logging; include keys (mask sensitive)

- Health
  - `/health` endpoints for both services

- Failures
  - Router: XSD/transform errors → `exception-queue`
  - Sender: AMQ retry; final failure logged; event JSON still emitted

---

## 11) Developer Tips

- Fast module test
```bash
mvn -T1C -pl services/fast-sender-service -am -DskipITs test
```

- View coverage
```bash
open services/fast-sender-service/target/site/jacoco/index.html
```

- Playwright single spec
```bash
npx playwright test tests/sender.json-flow.spec.ts -g "Plain JSON"
```

- Kafka admin snippet
```ts
const admin = kafka.admin();
await admin.connect();
await admin.createTopics({ topics: [{ topic: payment-messages, numPartitions: 1, replicationFactor: 1 }] });
await admin.disconnect();
```

---

## 12) FAQ

- Q: Duplicate handling policy in Router?
  - A: Log-only. Duplicates are not blocked; second publish may occur.

- Q: Avro-framed vs JSON requests in Sender?
  - A: Both accepted; consumers normalize (`payload` → `originalXml`).

- Q: How are Sender retries configured?
  - A: In service impls via properties (max attempts, backoff). `0` disables retries.

- Q: Where to tune JaCoCo thresholds?
  - A: `jacoco-maven-plugin` in each service pom. Current minimums: 80% line/branch.

- Q: Running only E2E?
  - A: Use the `e2e` folders and Playwright commands above.

---

## 13) Roadmap / Nice-to-have

- Optional: enforce dedup (DB/Redis) if strict single-publish is required
- Expand schema registry integration
- Harden XML validation (XXE, external entities fully disabled)

---

## 14) Appendix – Quick Commands

- Router
```bash
mvn -T1C -pl services/fast-router-service -am -DskipITs verify
cd services/fast-router-service/e2e && npm ci && npx playwright test --reporter=list
```

- Sender
```bash
mvn -T1C -pl services/fast-sender-service -am -DskipITs verify
cd services/fast-sender-service/e2e && npm ci && npx playwright test --reporter=list
```

- Push branch to upstream
```bash
git push -u upstream code-coverage
```

---

Keep this guide updated as flows evolve.
