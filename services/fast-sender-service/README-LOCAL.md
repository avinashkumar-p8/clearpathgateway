# Fast Sender Service – Local Development Notes

## What this service does (local)
- Consumes `pacs002-requests` from Kafka.
- Generates pacs.002 (reject) XML based on request context.
- Persists pacs.002 XML and event JSON to Spanner emulator (table `Pacs002Messages`).
- Publishes pacs.002 XML to ActiveMQ queue `${app.activemq.pacs002-queue}`.
- Publishes event JSON to Kafka `${app.kafka.topics.payment-events}`.

## Configuration reference
- Kafka
  - `spring.kafka.bootstrap-servers`: `localhost:9092`
  - Consumer group: `${spring.application.name}`
- ActiveMQ
  - `spring.activemq.broker-url`: `tcp://localhost:61616`
  - Outbound queue: `app.activemq.pacs002-queue: pacs002.outbound`
- Spanner (local)
  - `spring.cloud.gcp.spanner.instance-id: payment-gateway-local`
  - `spring.cloud.gcp.spanner.database: sender-db`
  - `spring.cloud.gcp.spanner.emulator.enabled: true`

## Tables (local emulator)
- `Pacs002Messages(puid STRING(16) PK, unique_id STRING(64), created_at TIMESTAMP, xml STRING(MAX), event_json STRING(MAX))`

## Testing locally
- Run unit tests for this module:
```bash
mvn -q -pl services/fast-sender-service test
```

- Observe ActiveMQ outbound messages for `pacs002.outbound` via the web console at `http://localhost:8161`.
