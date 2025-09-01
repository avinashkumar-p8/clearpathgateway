# ClearPath Gateway - System Overview & Integration Architecture

## Complete System Integration Flow

This diagram shows the complete integration architecture for the Singapore ClearPath Gateway, including all service interactions, protocols, and data flows.

## Integration Protocol Summary

| Service | Input Protocol | Output Protocol | Purpose |
|---------|---------------|----------------|---------|
| **Router Service** | IBM MQ (XML) | Kafka (JSON) | Message validation & routing |
| **Inward Processor** | Kafka (JSON) | Kafka (JSON) | Payment processing (4.5s SLA) |
| **Outward Processor** | REST API | Kafka (JSON) | Authorization & risk (5s SLA) |
| **Sender Service** | Kafka (JSON) | IBM MQ (XML) | Message transmission |
| **Liquidity Service** | REST API | Kafka Events | Balance management (REST for WebMethods) |
| **Availability Service** | gRPC | gRPC Streaming | Bank status monitoring |

## Service Responsibilities

### Fast Router Service
- **Input**: IBM MQ Queue (ISO 20022 XML from G3 Host)
- **Processing**: XML validation, JSON transformation, message routing
- **Output**: Kafka topics with unified JSON messages
- **SLA**: < 1 second end-to-end
- **Exception Handling**: Circuit breaker → Spanner fallback → S3 emergency storage

### Fast Inward Clearing Processor  
- **Input**: Kafka (unified JSON from Router)
- **Processing**: Business validation, fraud detection, account validation, payment execution
- **Output**: Kafka response messages to Sender Service
- **SLA**: < 4.5 seconds (regulatory requirement)
- **External Calls**: VAM/MIDANZ (account validation), Liquidity Service (gRPC), Availability Service (gRPC)

### Fast Outward Clearing Processor
- **Input**: REST API (PSP Global, WebMethods, Operations Console)
- **Processing**: Multi-stage authorization, risk assessment, compliance checking
- **Output**: Kafka outbound messages to Sender Service  
- **SLA**: < 5 seconds for complete authorization
- **External Calls**: Liquidity Service (REST), Availability Service (gRPC), Risk Management APIs

### Fast Sender Service
- **Input**: Kafka (outbound messages from processors)
- **Processing**: JSON→XML transformation, message transmission
- **Output**: IBM MQ Queue (ISO 20022 XML to G3 Host)
- **SLA**: < 2 seconds transmission time
- **Exception Handling**: Exponential backoff retry → Spanner queue → S3 dead letter

### Fast Liquidity Service
- **Input**: REST API (primarily WebMethods integration for outward processing)
- **Processing**: Real-time balance checks, liquidity authorization, balance updates
- **Output**: REST responses + Kafka events for balance changes
- **SLA**: < 200 milliseconds (critical for payment flow)
- **Integration**: Optimized for WebMethods SOAP/REST calls

### Fast Availability Service
- **Input**: gRPC calls from other services
- **Processing**: Real-time bank status monitoring, health checks
- **Output**: gRPC responses + streaming status updates
- **SLA**: < 100 milliseconds (high-frequency calls)
- **Monitoring**: 30-second health checks with circuit breaker protection

## Message Flow Examples

### Inward Payment Flow (G3 → Singapore Bank)
1. **G3 Host** sends PACS.008 XML → **IBM MQ Queue**
2. **Router Service** consumes MQ → validates XML → transforms to JSON → publishes to Kafka
3. **Inward Processor** consumes Kafka → validates business rules → checks fraud → validates account → checks liquidity → executes payment → generates response
4. **Sender Service** consumes response → transforms JSON to XML → sends acknowledgment to **IBM MQ Queue** → **G3 Host**

### Outward Payment Flow (Singapore Bank → G3)
1. **PSP Global/WebMethods** calls REST API → **Outward Processor**
2. **Outward Processor** validates → risk assessment → compliance check → liquidity authorization → approves payment
3. **Outward Processor** publishes to Kafka → **Sender Service**
4. **Sender Service** transforms JSON to XML → sends to **IBM MQ Queue** → **G3 Host**

### Liquidity Check Flow (WebMethods Integration)
1. **WebMethods** calls REST API → **Liquidity Service**
2. **Liquidity Service** checks Redis cache → validates against Spanner → returns balance status
3. For balance updates: **Liquidity Service** updates Spanner → publishes Kafka event → updates Redis cache

### Bank Availability Check Flow
1. **Any Service** makes gRPC call → **Availability Service**
2. **Availability Service** checks Redis cache → returns current status (AVAILABLE/DEGRADED/UNAVAILABLE)
3. Background: **Availability Service** continuously monitors bank health → updates cache → publishes status changes

## Technology Stack Summary

### Messaging & Communication
- **IBM MQ**: External G3 Host integration (input/output queues)
- **Apache Kafka**: Internal service communication (event streaming)
- **gRPC**: High-performance service calls (Availability Service)
- **REST API**: External integration (PSP Global, WebMethods)
- **SOAP**: Legacy WebMethods integration

### Data Storage
- **Cloud Spanner**: ACID transactions, audit trails, payment state
- **Redis Cluster**: High-speed caching, real-time data
- **BigQuery**: Analytics, fraud detection, regulatory reporting
- **S3**: Emergency storage, compliance documents, fallback queues

### Infrastructure
- **Java 21**: Virtual threads for high-performance I/O
- **Spring Boot 3.2**: Reactive programming model
- **Docker/Kubernetes**: Containerized deployment
- **Circuit Breakers**: Resilience4j for fault tolerance

## Exception Handling Strategy

### Three-Tier Fallback Pattern
1. **Primary**: Normal service operation (Kafka, Spanner, Redis)
2. **Secondary**: Degraded mode (if Kafka down → use Spanner; if Redis down → use Spanner)
3. **Tertiary**: Emergency mode (if both Kafka & Spanner down → store in S3 for manual recovery)

### SLA Breach Handling
- **Real-time monitoring** with sub-second alerting
- **Automatic escalation** to operations team
- **Circuit breaker** protection for external services
- **Graceful degradation** with reduced feature set
- **Manual override** capabilities for critical payments

## Compliance & Audit
- **Complete audit trail** in Cloud Spanner with immutable records
- **Real-time regulatory reporting** to MAS
- **Fraud detection** with ML models and real-time scoring
- **Sanctions screening** for all cross-border payments
- **Data encryption** at rest and in transit
- **7-year document retention** in S3 with lifecycle policies