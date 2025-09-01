# FAST Non-Value Sender Service - Technical Design

## Business Purpose

The **Fast Non-Value Sender Service** is the critical outbound gateway that delivers administrative and operational messages from the APEAFAST-SG ClearPath Gateway to the CPG/G3 network. This service ensures that bank status changes, settlement notifications, and administrative responses reach external systems reliably and within strict SLA requirements.

## Core Responsibilities

### 1. **Outbound Administrative Messaging**
- Transform internal Avro messages to ISO 20022 XML format
- Generate ADMN.001 (Sign-On) and ADMN.003 (Sign-Off) requests for bank availability changes
- Build compliant XML messages using predefined templates and field mappings

### 2. **Message Delivery Management** 
- Send administrative messages to CPG/G3 via IBM MQ with guaranteed delivery
- Track message delivery status and handle acknowledgments
- Implement retry logic with exponential backoff for failed deliveries

### 3. **Settlement & Notification Processing**
- Process settlement confirmation messages from Availability Service (future)
- Handle emergency alerts and maintenance notifications
- Support priority message routing for time-sensitive communications

### 4. **Quality Assurance & Compliance**
- Validate generated XML against XSD schemas before transmission
- Maintain comprehensive audit trails for all outbound messages
- Ensure idempotency to prevent duplicate message transmission

## Key Features

### ✅ **High-Performance Message Generation**
- **Avro-to-XML Transformation**: Efficient conversion from internal format to ISO 20022 XML
- **Template-Based Generation**: Pre-validated XML templates for consistent message structure
- **Sub-2-Second Delivery**: Complete message generation and delivery within SLA targets

### ✅ **Guaranteed Message Delivery**
- **IBM MQ Integration**: Reliable message queuing with delivery confirmation
- **Retry with Backoff**: Intelligent retry logic for transient failures
- **Multi-Queue Fallback**: Alternative delivery paths for high availability

### ✅ **Real-Time Status Management**
- **Bank Availability Broadcasting**: Immediate notification of bank status changes to CPG
- **Emergency Message Priority**: Fast-track processing for critical alerts
- **Delivery Tracking**: Real-time monitoring of message delivery status

### ✅ **Enterprise Resilience**
- **Circuit Breaker Protection**: Prevent cascade failures during MQ outages
- **Poison Message Handling**: Isolate and investigate failed message generations
- **Comprehensive Audit**: Full traceability for regulatory compliance

## Service Overview

This service acts as the **exit point** for all administrative communications from the ClearPath Gateway to external systems, transforming internal Avro messages back to ISO 20022 XML format and ensuring reliable delivery via IBM MQ. It completes the operational message lifecycle by delivering bank status updates and administrative responses to the CPG network.

## Service Architecture Diagram

```mermaid
graph TB
    subgraph "Internal Services"
        AVAILABILITY_SVC[Fast Availability Service<br/>gRPC Client]
        LIQUIDITY_SVC[Fast Liquidity Service<br/>REST Client]
        KAFKA_TOPICS[Kafka Topics<br/>Non-Value Events]
        INTERNAL_UI[Operational UI<br/>Manual Triggers]
    end
    
    subgraph "Fast Non-Value Sender Service"
        subgraph "Input Layer"
            GRPC_SERVER[gRPC Server<br/>Administrative Requests]
            KAFKA_CONSUMER[Kafka Consumer<br/>Event Processing]
            REST_ENDPOINT[REST Endpoint<br/>Manual Operations]
            SCHEDULER[Task Scheduler<br/>Periodic Checks]
        end
        
        subgraph "Processing Layer"
            MESSAGE_BUILDER[Message Builder<br/>JSON to XML Transform]
            VALIDATION_ENGINE[Validation Engine<br/>XSD Compliance]
            ROUTING_ENGINE[Routing Engine<br/>Target Determination]
            IDEMPOTENCY[Idempotency Repository<br/>Duplicate Prevention]
        end
        
        subgraph "Output Layer"
            MQ_PRODUCER[IBM MQ Producer<br/>Connection Pool]
            DELIVERY_TRACKER[Delivery Tracker<br/>Status Monitoring]
            ACK_PROCESSOR[ACK Processor<br/>Response Handling]
        end
        
        subgraph "Persistence Layer"
            SPANNER_DB[(Cloud Spanner<br/>Message Audit)]
            REDIS_CACHE[(Redis Cache<br/>Status Cache)]
        end
        
        subgraph "Resilience Layer"
            CIRCUIT_BREAKER[Circuit Breaker<br/>MQ Protection]
            RETRY_HANDLER[Retry Handler<br/>Failed Messages]
            DLQ_HANDLER[Dead Letter Queue<br/>S3 Archive]
        end
    end
    
    subgraph "External Systems"
        IBMMQ_OUT[IBM MQ Output Queue<br/>CPG/G3 Network]
        CPG_G3[CPG/G3 Network<br/>ADMN Messages]
    end
    
    %% Input Flow
    AVAILABILITY_SVC --> GRPC_SERVER
    LIQUIDITY_SVC --> REST_ENDPOINT
    KAFKA_TOPICS --> KAFKA_CONSUMER
    INTERNAL_UI --> REST_ENDPOINT
    SCHEDULER --> MESSAGE_BUILDER
    
    %% Processing Flow
    GRPC_SERVER --> MESSAGE_BUILDER
    KAFKA_CONSUMER --> MESSAGE_BUILDER
    REST_ENDPOINT --> MESSAGE_BUILDER
    MESSAGE_BUILDER --> VALIDATION_ENGINE
    VALIDATION_ENGINE --> ROUTING_ENGINE
    ROUTING_ENGINE --> IDEMPOTENCY
    
    %% Output Flow
    IDEMPOTENCY --> MQ_PRODUCER
    MQ_PRODUCER --> DELIVERY_TRACKER
    DELIVERY_TRACKER --> ACK_PROCESSOR
    
    %% External Output
    MQ_PRODUCER --> IBMMQ_OUT
    IBMMQ_OUT --> CPG_G3
    
    %% Persistence
    ROUTING_ENGINE --> SPANNER_DB
    DELIVERY_TRACKER --> REDIS_CACHE
    
    %% Resilience
    MQ_PRODUCER --> CIRCUIT_BREAKER
    DELIVERY_TRACKER --> RETRY_HANDLER
    ACK_PROCESSOR --> DLQ_HANDLER
    
    style MESSAGE_BUILDER fill:#e1f5fe
    style VALIDATION_ENGINE fill:#f3e5f5
    style MQ_PRODUCER fill:#e8f5e8
    style DELIVERY_TRACKER fill:#fff3e0
    style IDEMPOTENCY fill:#fce4ec
```

## Message Processing Flow Diagram

```mermaid
sequenceDiagram
    participant AVAIL as Availability Service
    participant KAFKA as Kafka
    participant NVS as Non-Value Sender
    participant MQ as IBM MQ
    participant CPG as CPG/G3 Network
    participant POISON as Poison Topic
    
    Note over AVAIL,CPG: Bank Status Change Flow
    
    AVAIL->>KAFKA: Publish Avro to bank-availability-requests
    KAFKA->>NVS: Consume Avro Message
    NVS->>NVS: Transform Avro to XML
    NVS->>NVS: Build ADMN.003 Message
    NVS->>NVS: Validate Generated XML
    
    alt XML Generation Success
        NVS->>NVS: Check Idempotency
        NVS->>MQ: Send ADMN.003 to CPG
        MQ->>CPG: Forward Message
        NVS->>NVS: Track Delivery Status
        NVS->>NVS: Audit Success to Spanner
    
    else XML Generation/Validation Failure
        NVS->>POISON: Send to poison-outbound-messages
        NVS->>NVS: Log Error Details
        NVS->>NVS: Audit Failure to Spanner
    end
    
    Note over AVAIL,CPG: Settlement Notification Flow (Future)
    
    AVAIL->>KAFKA: Publish Settlement Avro
    KAFKA->>NVS: Consume Settlement Message
    NVS->>NVS: Transform to Settlement XML
    NVS->>MQ: Send to CPG Settlement Queue
    MQ->>CPG: Forward Settlement
    
    Note over AVAIL,CPG: Response Processing
    
    CPG-->>MQ: ADMN.004 Response (Async)
    MQ-->>NVS: Consume Response
    NVS->>NVS: Update Delivery Status
    NVS->>KAFKA: Publish delivery-status-update event
```

## Message Type Processing Logic

```mermaid
flowchart TD
    START([Request Received]) --> IDENTIFY[Identify Message Type]
    
    IDENTIFY --> BANK_SIGNON{Bank Sign-On<br/>Request}
    IDENTIFY --> BANK_SIGNOFF{Bank Sign-Off<br/>Request}
    IDENTIFY --> SETTLEMENT{Settlement<br/>Confirmation}
    IDENTIFY --> MAINTENANCE{Maintenance<br/>Notification}
    IDENTIFY --> EMERGENCY{Emergency<br/>Alert}
    IDENTIFY --> OTHER{Other Non-Value<br/>Message}
    
    BANK_SIGNON --> BUILD_ADMN001[Build ADMN.001<br/>Sign-On Request]
    BANK_SIGNOFF --> BUILD_ADMN003[Build ADMN.003<br/>Sign-Off Request]
    SETTLEMENT --> BUILD_SETTLEMENT[Build Settlement<br/>Notification Message]
    MAINTENANCE --> BUILD_MAINT[Build Maintenance<br/>Window Message]
    EMERGENCY --> BUILD_EMERGENCY[Build Emergency<br/>Alert Message]
    OTHER --> BUILD_GENERIC[Build Generic<br/>Non-Value Message]
    
    BUILD_ADMN001 --> VALIDATE[XSD Validation]
    BUILD_ADMN003 --> VALIDATE
    BUILD_SETTLEMENT --> VALIDATE
    BUILD_MAINT --> VALIDATE
    BUILD_EMERGENCY --> VALIDATE_PRIORITY[Priority Validation<br/>Emergency Path]
    BUILD_GENERIC --> VALIDATE
    
    VALIDATE --> ROUTE[Route to Target Queue]
    VALIDATE_PRIORITY --> ROUTE_EMERGENCY[Route to Emergency Queue]
    
    ROUTE --> SEND_MQ[Send via IBM MQ]
    ROUTE_EMERGENCY --> SEND_MQ
    
    SEND_MQ --> TRACK[Track Delivery Status]
    TRACK --> AUDIT[Audit to Spanner]
    AUDIT --> END([Processing Complete])
    
    style BUILD_EMERGENCY fill:#ffebee
    style VALIDATE_PRIORITY fill:#ffebee
    style ROUTE_EMERGENCY fill:#ffebee
    style TRACK fill:#e8f5e8
```

## Database Schema Design

### Cloud Spanner Tables

#### 1. NonValueOutboundAudit Table
```sql
CREATE TABLE NonValueOutboundAudit (
    message_id STRING(50) NOT NULL,
    correlation_id STRING(50),
    message_type STRING(20) NOT NULL,
    target_queue STRING(100),
    created_timestamp TIMESTAMP NOT NULL,
    sent_timestamp TIMESTAMP,
    acknowledged_timestamp TIMESTAMP,
    delivery_status STRING(20) NOT NULL,
    retry_count INT64 DEFAULT 0,
    priority_level STRING(10),
    source_service STRING(50),
    message_size_bytes INT64,
    delivery_duration_ms INT64,
    error_details STRING(MAX),
    created_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP()),
    updated_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP())
) PRIMARY KEY (message_id, created_timestamp DESC);

-- Partitioning and TTL
ALTER TABLE NonValueOutboundAudit ADD ROW DELETION POLICY (OLDER_THAN(created_timestamp, INTERVAL 90 DAY));
```

#### 2. DeliveryTracker Table
```sql
CREATE TABLE DeliveryTracker (
    message_id STRING(50) NOT NULL,
    queue_name STRING(100) NOT NULL,
    delivery_attempts INT64 DEFAULT 0,
    last_attempt_timestamp TIMESTAMP,
    next_retry_timestamp TIMESTAMP,
    delivery_status STRING(20) NOT NULL,
    failure_reason STRING(MAX),
    max_retry_count INT64 DEFAULT 3,
    created_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP()),
    updated_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP())
) PRIMARY KEY (message_id);

-- TTL for completed deliveries
ALTER TABLE DeliveryTracker ADD ROW DELETION POLICY (OLDER_THAN(updated_at, INTERVAL 7 DAY));
```

#### 3. MessageTemplates Table
```sql
CREATE TABLE MessageTemplates (
    template_id STRING(50) NOT NULL,
    message_type STRING(20) NOT NULL,
    template_version STRING(10) NOT NULL,
    xsd_schema STRING(MAX),
    xml_template STRING(MAX),
    field_mappings JSON,
    validation_rules JSON,
    is_active BOOL NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP()),
    updated_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP())
) PRIMARY KEY (template_id);
```

### Redis Caching Strategy

```yaml
# Delivery Status Cache
delivery_status_cache:
  key_pattern: "delivery:status:{message_id}"
  ttl: 7200  # 2 hours
  data: |
    {
      "messageId": "NVS001234567",
      "status": "DELIVERED",
      "sentAt": "2024-02-17T10:30:00Z",
      "acknowledgedAt": "2024-02-17T10:30:15Z",
      "deliveryDuration": 15000
    }

# Message Template Cache
template_cache:
  key_pattern: "template:{message_type}:{version}"
  ttl: 86400  # 24 hours
  data: |
    {
      "templateId": "ADMN_001_V1",
      "messageType": "ADMN.001",
      "xmlTemplate": "<admn:SignOnRequest>...",
      "fieldMappings": {...},
      "validationRules": {...}
    }
```

## Performance Requirements & SLA Targets

### Processing Performance Targets
| Metric | Target | Monitoring |
|--------|--------|------------|
| **Message Build Latency** | < 500ms | 95th percentile |
| **MQ Delivery Latency** | < 2 seconds | 95th percentile |
| **Throughput** | 500 messages/minute | Average sustained |
| **Availability** | 99.95% uptime | Monthly |
| **Emergency Message SLA** | < 10 seconds | 100% compliance |
| **Memory Usage** | < 1.5GB heap | Peak usage |

### SLA Breakdown (2-second budget)
```
Total Budget: 2.0 seconds (Normal) / 10.0 seconds (Emergency)
├── Message Building: 200ms
├── XSD Validation: 100ms  
├── Idempotency Check: 50ms
├── Routing Logic: 50ms
├── MQ Connection Setup: 100ms
├── Message Transmission: 800ms
├── Delivery Confirmation: 200ms
├── Status Update: 100ms
├── Audit Logging: 200ms
└── Response Generation: 200ms
```

## Resilience Patterns & Error Handling

### Circuit Breaker Configuration
```yaml
circuit_breakers:
  ibm_mq:
    failure_threshold: 5
    recovery_timeout: 60s
    timeout: 5s
    
  emergency_queue:
    failure_threshold: 2
    recovery_timeout: 30s
    timeout: 3s
```

### Retry Strategy with Exponential Backoff
```yaml
retry_policies:
  normal_messages:
    max_attempts: 3
    initial_interval: 2s
    max_interval: 60s
    multiplier: 2.0
    
  emergency_messages:
    max_attempts: 5
    initial_interval: 1s
    max_interval: 30s
    multiplier: 1.5
    
  settlement_messages:
    max_attempts: 5
    initial_interval: 3s
    max_interval: 120s
    multiplier: 2.0
```

### Multi-Tier Fallback Strategy
```
Primary: IBM MQ Direct Send
    ↓ (on failure)
Fallback-1: IBM MQ Alternative Queue
    ↓ (on failure)  
Fallback-2: Kafka Dead Letter Topic
    ↓ (on failure)
Fallback-3: AWS S3 Emergency Archive
```

## Message Templates & Transformations

### ADMN.001 Sign-On Request Template
```xml
<?xml version="1.0" encoding="UTF-8"?>
<admn:SignOnRequest xmlns:admn="urn:iso:std:iso:20022:tech:xsd:admn.001.001.01">
    <MsgId>${messageId}</MsgId>
    <CreDtTm>${creationDateTime}</CreDtTm>
    <InstgAgt>
        <FinInstnId>
            <BICFI>${instructingAgentBIC}</BICFI>
        </FinInstnId>
    </InstgAgt>
    <InstdAgt>
        <FinInstnId>
            <BICFI>${instructedAgentBIC}</BICFI>
        </FinInstnId>
    </InstdAgt>
    <SgnOnInf>
        <SgnOnTp>${signOnType}</SgnOnTp>
        <EffctvDt>${effectiveDate}</EffctvDt>
        <Rsn>${reason}</Rsn>
    </SgnOnInf>
</admn:SignOnRequest>
```

### ADMN.003 Sign-Off Request Template
```xml
<?xml version="1.0" encoding="UTF-8"?>
<admn:SignOffRequest xmlns:admn="urn:iso:std:iso:20022:tech:xsd:admn.003.001.01">
    <MsgId>${messageId}</MsgId>
    <CreDtTm>${creationDateTime}</CreDtTm>
    <InstgAgt>
        <FinInstnId>
            <BICFI>${instructingAgentBIC}</BICFI>
        </FinInstnId>
    </InstgAgt>
    <InstdAgt>
        <FinInstnId>
            <BICFI>${instructedAgentBIC}</BICFI>
        </FinInstnId>
    </InstdAgt>
    <SgnOffInf>
        <SgnOffTp>${signOffType}</SgnOffTp>
        <EffctvDt>${effectiveDate}</EffctvDt>
        <Rsn>${reason}</Rsn>
        <AnticptdDwnTm>${anticipatedDownTime}</AnticptdDwnTm>
    </SgnOffInf>
</admn:SignOffRequest>
```

## Chaos Engineering Test Scenarios

### IBM MQ Resilience Tests
1. **Queue Manager Failure**: Test MQ connection loss during send
2. **Queue Full**: Test queue capacity limits and backpressure
3. **Slow Network**: Simulate 10-second network delays
4. **Connection Pool Exhaustion**: Test connection pool limits
5. **Message Size Limits**: Test with oversized messages

### Emergency Processing Tests
1. **Emergency Queue Failure**: Test fallback to normal queue
2. **High Priority Flooding**: Test with 1000 emergency messages
3. **Concurrent Emergency Messages**: Test parallel emergency processing
4. **Emergency Timeout**: Test emergency SLA breach scenarios

### Integration Resilience Tests
1. **Availability Service Failure**: Test gRPC client failures
2. **Liquidity Service Timeout**: Test REST client timeouts
3. **Kafka Consumer Lag**: Test event processing delays
4. **Spanner Write Failures**: Test audit logging failures

## Security & Compliance

### Message Security
- **Message Signing**: Digital signatures for all ADMN messages
- **Content Encryption**: AES-256 encryption for sensitive data
- **Transport Security**: TLS 1.3 for all MQ communications
- **Access Control**: Role-based access for administrative operations

### Audit & Compliance
```yaml
audit_requirements:
  - "All outbound administrative messages logged"
  - "Bank status changes tracked with approver identity"
  - "Emergency messages flagged for compliance review"
  - "Settlement confirmations archived for 7 years"
  - "Message delivery failures escalated within 5 minutes"
```

## Monitoring & Observability

### Key Metrics
```yaml
business_metrics:
  - name: "admn_messages_sent_total"
    type: "counter"
    labels: ["message_type", "target_bank", "status"]
    
  - name: "emergency_messages_total"
    type: "counter"
    labels: ["priority", "delivery_status"]
    
  - name: "settlement_confirmations_total"
    type: "counter"
    labels: ["settlement_type", "status"]

technical_metrics:
  - name: "message_delivery_duration_seconds"
    type: "histogram"
    labels: ["message_type", "queue_name"]
    
  - name: "mq_connection_pool_size"
    type: "gauge"
    labels: ["pool_name", "status"]
    
  - name: "delivery_retry_attempts_total"
    type: "counter"
    labels: ["message_type", "retry_reason"]
```

### Alert Rules
```yaml
alerts:
  - name: "EmergencyMessageDelay"
    condition: "emergency_message_delivery > 10s"
    severity: "critical"
    
  - name: "MQConnectionFailure"
    condition: "mq_connection_failures > 5"
    severity: "high"
    
  - name: "DeliveryFailureRate"
    condition: "delivery_failure_rate > 5%"
    severity: "medium"
```

## Avro Schema Location

**ADMN Message Schema:** `services/fast-nonval-sender/src/main/resources/avro/admn-message.avsc`  
*Shared schema for consuming Avro messages and transforming to XML for CPG/G3 delivery*

## Integration APIs

### gRPC Service Interface
```protobuf
service NonValueSender {
  rpc SendBankStatusMessage(BankStatusMessageRequest) returns (MessageResponse);
  rpc SendSettlementConfirmation(SettlementRequest) returns (MessageResponse);
  rpc SendEmergencyAlert(EmergencyAlertRequest) returns (MessageResponse);
  rpc GetDeliveryStatus(DeliveryStatusQuery) returns (DeliveryStatusResponse);
}

message BankStatusMessageRequest {
  string bank_code = 1;
  string status_type = 2;  // SIGN_ON, SIGN_OFF
  string reason = 3;
  google.protobuf.Timestamp effective_from = 4;
  google.protobuf.Timestamp effective_to = 5;
  string priority = 6;     // NORMAL, HIGH, EMERGENCY
}

message MessageResponse {
  string message_id = 1;
  string status = 2;
  string correlation_id = 3;
  google.protobuf.Timestamp sent_at = 4;
}
```

### REST API Endpoints
```yaml
endpoints:
  - path: "/api/v1/send/settlement"
    method: "POST"
    description: "Send settlement confirmation to CPG"
    
  - path: "/api/v1/send/emergency"
    method: "POST"
    description: "Send emergency alert message"
    
  - path: "/api/v1/delivery/status/{messageId}"
    method: "GET"
    description: "Get message delivery status"
    
  - path: "/api/v1/resend/{messageId}"
    method: "POST"
    description: "Retry failed message delivery"
```

## Operational UI Requirements

### Message Delivery Dashboard
- **Live Delivery Queue**: Current messages in transmission
- **Delivery Status Grid**: Success/failure rates by message type
- **Emergency Message Monitor**: Real-time emergency message tracking
- **Failed Message Manager**: Retry and investigate failed deliveries

### Administrative Controls
- **Emergency Message Trigger**: Manual emergency alert sending
- **Bank Status Override**: Manual bank status change messages
- **Message Templates**: Configure and update message templates
- **Queue Management**: Monitor and manage MQ queues
- **Delivery Analytics**: Historical delivery performance reports

