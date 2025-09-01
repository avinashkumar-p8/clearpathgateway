# APEAFAST-SG Architecture

## System Overview

The APEAFAST-SG Clear Path Gateway implements a microservices architecture for Singapore's fast payment system. The system is designed to replace GPP G3 with a cloud-native, scalable, and compliant payment processing platform.

## Service Architecture

### Service Interaction Diagram

```mermaid
graph TB
    subgraph "External Systems"
        G3[G3 Host]
        CPG[CPG]
        PSP[PSP Global]
        VAM[VAM]
        MIDANZ[MIDANZ]
    end

    subgraph "Fast Payment Gateway Services"
        Router[fast-router-service]
        Inward[fast-inward-clearing-processor]
        Outward[fast-outward-clearing-processor]
        Sender[fast-sender-service]
        Liquidity[fast-liquidity-service]
        Availability[fast-availability-service]
    end

    subgraph "Infrastructure"
        Kafka[Kafka Event Bus]
        Spanner[Cloud Spanner]
        Redis[Redis Cache]
    end

    G3 -->|PACS Messages| CPG
    CPG -->|Route Messages| Router
    Router -->|CTI/DDI| Inward
    Router -->|Bank Status| Availability
    PSP -->|CTO Requests| Outward

    Inward -->|Responses| Sender
    Outward -->|Auth Check| Liquidity
    Outward -->|Send Messages| Sender
    Sender -->|Messages| CPG

    Inward -->|Account Ops| VAM
    Inward -->|Account Ops| MIDANZ

    All Services -.->|Events| Kafka
    All Services -.->|Data| Spanner
    All Services -.->|Cache| Redis
```

## Service Responsibilities

| Service | Primary Function | SLA Requirement |
|---------|------------------|-----------------|
| **fast-router-service** | Message routing and initial validation | <1s routing |
| **fast-inward-clearing-processor** | CTI/DDI processing | 4.5s end-to-end |
| **fast-outward-clearing-processor** | CTO processing | <2s authorization |
| **fast-sender-service** | G3 Host message transmission | <500ms send |
| **fast-liquidity-service** | Real-time liquidity management | <200ms auth |
| **fast-availability-service** | Bank status management | <100ms status |

## Payment Flow Patterns

### 1. Inward Credit Transfer (CTI)
```
G3 Host → CPG → fast-router-service → fast-inward-clearing-processor
↓
VAM/MIDANZ (account credit) → fast-sender-service → CPG → G3 Host
```

### 2. Outward Credit Transfer (CTO)
```
PSP Global → fast-outward-clearing-processor → fast-liquidity-service (auth)
↓
fast-sender-service → CPG → G3 Host → PACS.002 response
```

### 3. Direct Debit Inward (DDI)
```
G3 Host → CPG → fast-router-service → fast-inward-clearing-processor
↓
fast-liquidity-service (customer check) → VAM/MIDANZ (debit)
↓
fast-sender-service → CPG → G3 Host
```

## Technology Stack

### Platform
- **Cloud Provider**: Google Cloud Platform (GCP)
- **Container Orchestration**: Kubernetes (GKE)
- **Service Mesh**: Istio (planned)
- **CI/CD**: Cloud Build, Cloud Deploy

### Data Layer
- **Primary Database**: Cloud Spanner (ACID transactions)
- **Analytics**: BigQuery (reporting and analytics)
- **Caching**: Redis (session state, performance)
- **Document Store**: Firestore (audit logs)

### Messaging
- **Event Streaming**: Apache Kafka
- **Message Queues**: Cloud Pub/Sub (backup)
- **External Integration**: IBM MQ (G3 Host)

### Application Framework
- **Primary Language**: Java 17 with Spring Boot 3.x
- **Reactive Programming**: Spring WebFlux
- **State Management**: Spring State Machine
- **API Gateway**: Spring Cloud Gateway

### Observability
- **Metrics**: Prometheus + Grafana
- **Tracing**: Jaeger
- **Logging**: ELK Stack (Elasticsearch, Logstash, Kibana)
- **APM**: Google Cloud Operations

## Non-Functional Requirements

### Performance
- **Throughput**: 20+ TPS (combined CTI/CTO/DDI)
- **Latency**: 4.5s SLA for inward payments
- **Horizontal Scaling**: Auto-scaling based on load

### Availability
- **Uptime**: 99.95% target
- **RTO**: 15 minutes (Recovery Time Objective)
- **RPO**: 0 minutes (Recovery Point Objective)

### Security
- **Encryption**: MTLS for all inter-service communication
- **Authentication**: OAuth 2.0 / JWT tokens
- **Authorization**: RBAC (Role-Based Access Control)
- **Network Security**: VPC with private subnets

### Compliance
- **Regulatory**: MAS (Monetary Authority of Singapore)
- **Data Residency**: Singapore region only
- **Audit**: Complete audit trails for all transactions
- **Reporting**: Automated regulatory reports

## Deployment Architecture

### Environments
- **Development**: Local Docker Compose
- **Testing**: GKE staging cluster
- **UAT**: Production-like environment
- **Production**: Multi-zone GKE cluster

### Deployment Strategy
- **Blue-Green Deployment**: Zero-downtime deployments
- **Canary Releases**: Gradual rollout for critical changes
- **Feature Flags**: Runtime feature toggling
- **Database Migrations**: Backward-compatible schema changes

## Data Flow and Integration

### Event-Driven Architecture
- **Event Sourcing**: Payment state changes as events
- **CQRS**: Command Query Responsibility Segregation
- **Saga Pattern**: Distributed transaction management
- **Eventual Consistency**: Between services via events

### API Design
- **REST APIs**: Standard HTTP/JSON APIs
- **OpenAPI 3.0**: API documentation and contracts
- **Versioning**: Semantic versioning for APIs
- **Circuit Breakers**: Resilience patterns

### Integration Patterns
- **Synchronous**: REST APIs for request/response
- **Asynchronous**: Kafka events for loose coupling
- **Batch**: Scheduled jobs for reconciliation
- **Stream Processing**: Real-time event processing

## Security Architecture

### Network Security
- **VPC**: Private network with controlled access
- **Firewall Rules**: Restrictive ingress/egress rules
- **NAT Gateway**: Controlled internet access
- **VPN**: Secure connection to on-premise systems

### Application Security
- **MTLS**: Mutual TLS for service-to-service
- **JWT Tokens**: Stateless authentication
- **API Gateway**: Centralized security enforcement
- **Rate Limiting**: Protection against abuse

### Data Security
- **Encryption at Rest**: Database and file encryption
- **Encryption in Transit**: TLS 1.3 for all communications
- **Key Management**: Cloud KMS for key rotation
- **Secrets Management**: Kubernetes secrets + external secrets

## Monitoring and Observability

### Metrics Collection
- **Application Metrics**: Business and technical metrics
- **Infrastructure Metrics**: CPU, memory, network, disk
- **Custom Metrics**: Payment processing KPIs
- **SLA Monitoring**: Real-time SLA compliance tracking

### Distributed Tracing
- **Request Tracing**: End-to-end payment flow tracing
- **Service Dependencies**: Understanding service interactions
- **Performance Analysis**: Bottleneck identification
- **Error Correlation**: Root cause analysis

### Alerting Strategy
- **Tiered Alerting**: Info, Warning, Critical levels
- **Escalation Policies**: Automated escalation procedures
- **On-Call Rotation**: 24/7 operations support
- **Runbooks**: Automated incident response

## Disaster Recovery

### Backup Strategy
- **Database Backups**: Automated daily backups
- **Cross-Region Replication**: Async replication to DR region
- **Configuration Backups**: Infrastructure as Code
- **Application Artifacts**: Versioned and stored in registry

### Failover Procedures
- **Regional Failover**: Automatic failover for regional outages
- **Service Failover**: Individual service recovery
- **Data Consistency**: Maintaining data integrity during failover
- **Testing**: Regular DR testing and validation