# APEAFAST-SG Clear Path Gateway Solution Design
## Phase 1: Singapore Fast Payment Modernization

## 1. Executive Summary

The APEAFAST-SG Clear Path Gateway is a comprehensive cloud-native payment processing platform that replaces GPP G3 for Singapore's faster payment system. The solution spans both on-premises and cloud regions, providing end-to-end payment processing capabilities with integrated liquidity management, real-time monitoring, and operational excellence.

## 2. Architecture Overview

The solution follows a hybrid architecture model with three distinct regions:

### On-Premise Region:
- PSP Global (Debit/Batch Processing)
- Core banking integration (VAM, MIDANZ, LMS)
- Legacy system interfaces

### Cloud Region:
- PSP APEA FAST - SG Connector
- SG Gateway Services
- Liquidity Management capabilities
- Event processing and monitoring

### External Region:
- G3 Host integration
- CPG connectivity
- External scheme interfaces

## 3. Core Service Architecture

### 3.1 PSP APEA FAST - SG Connector (Cloud Region)

#### Outward Clearing Request Services
- Payment request ingestion from PSP Global
- Business rule validation
- Routing to appropriate clearing services

#### Payment Orchestration
- Payment workflow management
- State management and tracking
- Exception handling coordination

#### Clearing Schemes Services
- Scheme-specific processing logic
- Message transformation and validation
- Protocol adaptation for different payment types

#### Direct Debit Processing
- PACS.003 message handling and validation
- Customer account liquidity verification
- Pre-debit authorization checks
- Account debit processing coordination
- PACS.007 reversal handling
- CAMT.056/029 cancelation processing

#### Liquidity Authorization Services
- Real-time liquidity checking for outward payments
- Customer account balance verification for DDI
- Authorization decision engine
- Hold and retry mechanisms
- Net debit cap impact calculations

#### General Clearing Generation Services
- Message construction for outbound payments
- Format standardization
- Header and metadata management

#### Payment Response
- Response processing from clearing networks
- Status updates and notifications
- Callback management to PSP Global

### 3.2 SG Gateway Services (Cloud Region)

#### SG Fast Router
- Message reception from CPG/G3 Host
- Initial validation and parsing
- Routing decisions based on message type
- Duplicate detection and handling

#### Message ID Generation & Message Enrichment
- Unique message identifier creation
- Message correlation and tracking
- Enrichment with business context
- Audit trail initiation

#### Generate MUID
- Message Unit ID generation
- Cross-reference management
- Message lifecycle tracking

#### Message Handling
- Protocol-specific message processing
- Format conversion and transformation
- Error handling and recovery

#### Timer Handling
- SLA timer management (4.5 seconds for inward)
- Timeout detection and escalation
- Performance monitoring

### 3.3 Business Services Integration

#### PSP APEA FAST - Business Services
- Account validation and verification
- Customer verification processes
- Business rule enforcement
- Fraud checking integration

#### Account Services
- Account lookup and validation
- Account status verification
- Account holder information retrieval

## 4. Data and Event Management

### 4.1 Payment Data Hub
- Centralized payment data management
- Real-time data synchronization
- Data quality and consistency management
- Historical data archival

### 4.2 Event Processing

#### Payment Events
- Real-time payment status events
- Lifecycle event management
- Event correlation and aggregation

#### Payment Message Queue
- Asynchronous message processing
- Message ordering and sequencing
- Retry and dead letter handling

#### Credit Notifications
- Customer notification triggers
- Multi-channel notification support
- Delivery confirmation tracking

#### Data Sync - Account Feed
- Account data synchronization
- Real-time updates from core systems
- Data consistency maintenance

#### Bank Availability
- Participant bank status tracking
- Real-time availability updates
- Service interruption notifications

#### Bank Availability Update
- Automated status refresh
- Manual override capabilities
- Historical availability tracking

### 4.3 Liquidity Management Services

#### Liquidity Tracker/Management
- Real-time balance calculation
- Net debit cap monitoring
- Settlement cycle tracking
- Threshold management

#### Liquidity Authorization Check & Update
- Pre-transaction authorization
- Balance reservation and release
- Authorization audit trail

#### Bank Liquidity Management
- Multi-bank liquidity aggregation
- Cross-border liquidity optimization
- Regulatory compliance monitoring

## 5. External Integration Architecture

### 5.1 G3 Host Integration

#### Inbound Flows:
- PACS.008 (Credit Transfer Initiation)
- PACS.003 (Direct Debit Initiation)
- CAMT.056 (Cancelation Request)
- PACS.007 (Reversal Request)
- SNM messages (Settlement, Bank Status, etc.)

#### Outbound Flows:
- PACS.002 (Payment Status Report - Accept/Reject)
- PACS.008 (Outward Credit Transfer)
- CAMT.029 (Cancelation Response)
- Administrative messages (Sign-on/Sign-off)

**Protocol**: MQ-based messaging via CPG
**SLA**: 4.5 seconds for inward processing (CTI and DDI)

### 5.2 CPG (Clearing Payment Gateway) Integration
- Message routing and transformation
- Security and encryption handling
- Protocol translation
- Connection management with G3 Host

### 5.3 Core Banking Integration

#### VAM (Virtual Account Management)
- Virtual account processing
- Account mapping and resolution
- Balance inquiries and updates

#### MIDANZ
- Core banking transaction processing
- Account posting and settlement
- Real-time balance updates

#### LMS (Liquidity Management System)
- Legacy liquidity interfaces
- Settlement reporting
- Regulatory compliance data

## 6. Operational and Monitoring Services

### 6.1 Real-time Monitoring
- Transaction flow monitoring
- System health dashboards
- Performance metrics tracking
- SLA compliance monitoring

### 6.2 Exception Management
- Failed transaction handling
- Manual intervention interfaces
- Escalation workflows
- Resolution tracking

### 6.3 Audit and Compliance
- Complete transaction audit trails
- Regulatory reporting capabilities
- Data retention management
- Compliance monitoring dashboards

## 7. Payment Flow Implementation

### 7.1 Inward Credit Transfer Flow
1. **Message Reception**: G3 Host → CPG → SG Fast Router
2. **Validation & Enrichment**: Message validation, MUID generation, duplicate check
3. **Business Processing**: Route to PSP APEA FAST Business Services
4. **Account Processing**: Account validation, posting via VAM/MIDANZ
5. **Response Generation**: PACS.002 creation and transmission
6. **Liquidity Update**: Real-time balance adjustment
7. **Notification**: Customer notification via P-ReX

### 7.2 Outward Credit Transfer Flow
1. **Request Reception**: PSP Global → Outward Clearing Request Services
2. **Liquidity Authorization**: Check available liquidity before processing
3. **Message Construction**: Generate PACS.008 via General Clearing Generation Services
4. **Transmission**: Send to G3 Host via CPG
5. **Response Handling**: Process PACS.002 confirmation
6. **Status Update**: Update PSP Global with transaction status
7. **Liquidity Reconciliation**: Final balance adjustment

### 7.3 Direct Debit Inward (DDI) Flow
1. **Message Reception**: G3 Host → CPG → SG Fast Router (PACS.003)
2. **Validation & Processing**: Message validation, MUID generation, scheme validation
3. **Liquidity Check**: Validate customer account has sufficient funds for debit
4. **Authorization**: Pre-authorization check before account debit
5. **Account Debit**: Process customer account debit via VAM/MIDANZ
6. **Response Generation**: Send PACS.002 (acceptance/rejection) to G3 Host
7. **Liquidity Update**: Update net debit cap position (increase available liquidity)
8. **Settlement Processing**: Include in settlement cycle processing
9. **Exception Handling**: Process CAMT.056 cancelation requests if received
10. **Reversal Processing**: Handle PACS.007 reversals and update liquidity accordingly

### 7.4 Exception and Timeout Handling
- Timer Monitoring: Continuous SLA monitoring
- Timeout Detection: Automatic detection of SLA breaches
- Exception Routing: Failed transactions to exception queues
- Manual Intervention: Operator interfaces for resolution
- Retry Mechanisms: Configurable retry logic
- Escalation: Automated escalation for critical issues

## 8. Liquidity Management Implementation

### 8.1 Net Debit Cap Model
- **Real-time Tracking**: Continuous balance calculation including DDI impact
- **Authorization Engine**: Pre-transaction liquidity checks for both outward payments and DDI
- **Threshold Alerts**: Configurable warning levels
- **Settlement Integration**: Twice-daily settlement cycle support
- **DDI Impact**: Direct debit transactions increase available liquidity (customer pays ANZ)

### 8.2 Hold and Retry Mechanism
- **Insufficient Funds**: Automatic payment holding for outward payments
- **DDI Authorization**: Real-time customer balance checking for direct debits
- **Retry Logic**: Configurable retry intervals and attempts
- **Queue Management**: Priority-based processing
- **Manual Override**: Operator intervention capabilities
- **Liquidity Impact**: Proper accounting for DDI vs outward payment liquidity effects

## 9. Technology Stack and Infrastructure

### 9.1 Cloud Infrastructure (GCP)
- **Kubernetes (GKE)**: Container orchestration
- **Service Mesh**: Inter-service communication
- **Load Balancing**: High availability and scaling
- **Auto-scaling**: Dynamic resource management

### 9.2 Data Management
- **Cloud Spanner**: Transactional consistency
- **BigQuery**: Analytics and reporting
- **Kafka**: Event streaming and messaging
- **Redis**: High-speed caching
- **Firestore**: For Audit and ADS data store

### 9.3 Integration Patterns
- **Event-driven Architecture**: Kafka-based pub/sub
- **API Gateway**: Secure API management
- **Circuit Breakers**: Resilience patterns
- **Saga Pattern**: Distributed transaction management

## 10. Operational Excellence

### 10.1 Monitoring and Alerting
- **Real-time Dashboards**: Transaction and system monitoring
- **Proactive Alerting**: Threshold-based notifications
- **Performance Analytics**: SLA and performance tracking
- **Capacity Planning**: Resource utilization monitoring

### 10.2 User Interface Components
- **Liquidity Management Console**: Real-time balance monitoring
- **Transaction Dashboard**: Payment flow visualization
- **Exception Management**: Manual intervention interface
- **Bank Status Management**: Participant availability tracking
- **Reporting Interface**: Business and operational reports

## 11. Security and Compliance

### 11.1 Security Framework
- **End-to-end Encryption**: HTTPS/MTLS throughout
- **Certificate Management**: Automated certificate lifecycle
- **Access Control**: Role-based access management
- **Audit Logging**: Comprehensive audit trails

### 11.2 Regulatory Compliance
- **MAS Compliance**: Singapore regulatory requirements
- **Data Residency**: Singapore data governance
- **Reporting**: Regulatory reporting automation
- **Risk Management**: Real-time risk monitoring

## 12. Migration and Deployment Strategy

### 12.1 Phased Implementation
- **Phase 1A**: Core gateway services deployment
- **Phase 1B**: Liquidity management integration
- **Phase 1C**: Operational UI and monitoring
- **Phase 1D**: Full production cutover

### 12.2 Risk Mitigation
- **Parallel Running**: Validation with existing systems
- **Gradual Cutover**: Phased traffic migration
- **Rollback Procedures**: Comprehensive rollback plans
- **24x7 Support**: Dedicated support during transition

## 13. Success Metrics

### 13.1 Technical KPIs
- **SLA Compliance**: 100% adherence to 4.5-second requirement for both CTI and DDI
- **System Availability**: 99.95% uptime target
- **Transaction Success Rate**: >99.9% success rate across all payment types
- **Processing Capacity**: Support for 20+ TPS with horizontal scaling (CTI, CTO, DDI combined)
- **DDI Processing**: Successful handling of direct debit authorization and settlement

### 13.2 Business KPIs
- **Operational Efficiency**: 50% reduction in manual interventions
- **Time to Market**: Accelerated new feature deployment
- **Cost Optimization**: Cloud-native cost efficiency
- **Regulatory Readiness**: 100% compliance with MAS requirements

This solution design leverages the comprehensive architecture shown in the diagram to deliver a robust, scalable, and compliant payment processing platform for Singapore's faster payment ecosystem.