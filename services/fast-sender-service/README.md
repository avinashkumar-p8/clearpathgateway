# Fast Sender Service

## Overview

The Fast Sender Service handles all outbound message transmission to the G3 Host via CPG. It manages the reliable delivery of payment responses, status updates, and administrative messages while maintaining protocol compliance and security requirements.

## Responsibilities

### Core Functions
- **Response Transmission**: Send PACS.002 Payment Status Reports to G3 Host
- **Outbound Payment Messages**: Transmit PACS.008 for outward credit transfers
- **Administrative Messages**: Handle sign-on/sign-off sequences
- **Cancelation Responses**: Send CAMT.029 cancelation responses
- **Message Formatting**: Ensure proper message structure and headers
- **Delivery Confirmation**: Track message delivery status
- **Retry Logic**: Handle transmission failures with appropriate retry mechanisms

### Message Types Sent
- **PACS.002**: Payment Status Report (Accept/Reject responses)
- **PACS.008**: Credit Transfer Initiation (outward payments)
- **CAMT.029**: Cancelation Response
- **Administrative**: Sign-on/Sign-off messages
- **SNM**: Settlement and status notification messages

## Architecture

### Input Sources
- fast-inward-clearing-processor (payment responses)
- fast-outward-clearing-processor (outbound payments)
- fast-availability-service (administrative messages)

### Output Targets
- G3 Host (via CPG)
- Audit/Event streams
- Delivery confirmation queues

### Technology Stack
- **Framework**: Spring Boot 3.x
- **Messaging**: Kafka Consumer, MQ Client for G3 Host
- **Database**: Cloud Spanner (delivery tracking)
- **Cache**: Redis (message deduplication)
- **Security**: MTLS for G3 Host communication

## Key Features

### Performance Requirements
- **Throughput**: 20+ TPS message transmission
- **Latency**: <500ms transmission time
- **Availability**: 99.95% uptime
- **Delivery Rate**: >99.9% successful delivery

### Message Processing Flow
1. **Receive**: Accept message from upstream service
2. **Format**: Apply G3 Host message formatting requirements
3. **Security**: Add required security headers and certificates
4. **Transmit**: Send message to G3 Host via CPG
5. **Confirm**: Wait for delivery confirmation
6. **Track**: Update delivery status in database
7. **Audit**: Log transmission details

### Delivery Guarantees
- **At-least-once delivery**: Messages guaranteed to be delivered
- **Idempotency**: Duplicate detection on G3 Host side
- **Ordering**: Maintain message sequence where required
- **Retry Logic**: Configurable retry attempts with exponential backoff

## Configuration

### Environment Variables
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
G3_HOST_ENDPOINT=https://g3host.anz.com/api
CPG_ENDPOINT=https://cpg.anz.com/gateway
SPANNER_INSTANCE=payment-gateway
SPANNER_DATABASE=message-tracking
MTLS_KEYSTORE_PATH=/etc/ssl/client.p12
MTLS_KEYSTORE_PASSWORD=changeit
RETRY_MAX_ATTEMPTS=5
RETRY_BACKOFF_MS=1000
DELIVERY_TIMEOUT_MS=10000
```

### Message Templates
```yaml
templates:
  PACS.002:
    version: "08"
    namespace: "urn:iso:std:iso:20022:tech:xsd:pacs.002.001.08"
    
  PACS.008:
    version: "08"
    namespace: "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"
    
  CAMT.029:
    version: "08"
    namespace: "urn:iso:std:iso:20022:tech:xsd:camt.029.001.08"
```

## APIs

### Health Check
```http
GET /health
Response: 200 OK
{
  "status": "UP",
  "components": {
    "kafka": "UP",
    "g3-host": "UP",
    "cpg": "UP",
    "spanner": "UP"
  }
}
```

### Send Message (Internal)
```http
POST /api/v1/messages/send
Content-Type: application/json

{
  "messageType": "PACS.002",
  "originalMessageId": "MSG123456",
  "status": "ACSC",
  "reasonCode": "0000",
  "recipient": "G3HOST001",
  "priority": "HIGH"
}
```

### Message Status
```http
GET /api/v1/messages/{messageId}/status
Response: 200 OK
{
  "messageId": "MSG123456",
  "status": "DELIVERED",
  "sentAt": "2024-01-15T10:30:00Z",
  "deliveredAt": "2024-01-15T10:30:01Z",
  "attempts": 1
}
```

## Monitoring

### Key Metrics
- `sender.messages.sent.total` - Total messages sent
- `sender.messages.delivered.total` - Successfully delivered messages
- `sender.messages.failed.total` - Failed deliveries
- `sender.delivery.duration.seconds` - Delivery time distribution
- `sender.retry.attempts.total` - Total retry attempts
- `sender.connection.status` - G3 Host connection status

### Critical Alerts
- G3 Host connectivity issues
- Delivery failure rate > 0.1%
- High retry rates
- Certificate expiration warnings
- Message queue backup

### SLA Monitoring
- Average delivery time
- Peak message throughput
- Error rate by message type
- Connection availability

## Security

### MTLS Configuration
- **Client Certificate**: ANZ-issued certificate for G3 Host authentication
- **Server Verification**: Validate G3 Host certificate chain
- **Certificate Rotation**: Automatic certificate renewal
- **Cipher Suites**: Approved encryption algorithms only

### Message Security
- **Digital Signatures**: Sign critical payment messages
- **Encryption**: Sensitive data encryption in transit
- **Audit Trail**: Complete message transmission logging
- **Access Control**: Service-to-service authentication

## Error Handling

### Connection Errors
- **Network Timeout**: Retry with exponential backoff
- **Connection Refused**: Circuit breaker activation
- **Certificate Issues**: Alert operations team immediately

### Message Errors
- **Invalid Format**: Return to sender with error details
- **Duplicate Message**: Log and ignore (idempotent)
- **Rejected by G3 Host**: Forward rejection reason to originator

### Retry Strategy
```yaml
retry:
  max_attempts: 5
  initial_delay: 1000ms
  max_delay: 30000ms
  multiplier: 2.0
  jitter: 0.1
```

## Development

### Local Setup
```bash
cd services/fast-sender-service
./mvnw spring-boot:run -Dspring.profiles.active=local
```

### Testing
```bash
# Unit tests
./mvnw test

# Integration tests with G3 Host simulator
./mvnw integration-test -Dtest.g3host.mock=true

# Security tests
./mvnw test -Dtest=SecurityTest
```

### Mock G3 Host
```bash
# Start mock G3 Host for testing
docker run -p 8443:8443 g3host-simulator:latest
```