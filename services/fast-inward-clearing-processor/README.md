# Fast Inward Clearing Processor Service

A Kafka-based clearing processor service that handles CTI (Credit Transfer Inward) and DDI (Direct Debit Inward) processing with 4.5-second SLA compliance for the Singapore Fast Payment system. This service includes comprehensive end-to-end testing capabilities with an integrated Playwright test suite.

## Features

- **Kafka Integration**: Consumes messages from input topics and produces to output topics
- **Complex Avro Serialization**: Supports nested event structures with Header, Body, and Processing Context
- **Advanced Transaction Processing**: Complete transaction lifecycle management with complex data extraction
- **Intelligent Data Mapping**: Extracts payment data from nested structures (Header.UUID, Body.PmtAddRq, Procctxt.PmtDtls)
- **Validation**: Comprehensive field validation and business rule enforcement
- **Enrichment**: Adds metadata, timestamps, and processing information
- **Business Rules**: Configurable business logic and compliance checks with enhanced payment category support
- **Idempotency**: Redis-based duplicate transaction prevention
- **Error Handling**: Retry logic with Dead Letter Queue (DLQ) support
- **Monitoring**: Health checks, metrics, and observability endpoints
- **Containerization**: Docker and Kubernetes deployment support
- **ISO20022 Compliance**: Support for complex payment message structures
- **Comprehensive Testing**: Integrated Playwright test suite with end-to-end validation
- **Avro Schema Management**: Input and Response message schemas with Trailer object support

## Architecture

```
Input Topic (transactions.incoming)
           ↓
   Unified Transaction Consumer
           ↓
   Transaction Processing Pipeline
           ↓
   ├── 1. Idempotency Check
   ├── 2. Message Parsing (Avro → GenericRecord)
   ├── 3. Scheme Validation
   └── 4. Response Creation (with Trailer)
           ↓
   Output Topic (fast-outward-clearing)
           ↓
   Playwright E2E Test Suite
           ↓
   Validation & Assertions
```

## Components

### Core Services

- **UnifiedTransactionConsumer**: Single Kafka consumer with unified processing pipeline
- **TransactionProcessingHandler**: Main orchestrator for the 4-step processing pipeline
- **BusinessProcessingHandler**: Creates ResponseMessage with Trailer object
- **HealthController**: Simple monitoring and health check endpoints

### Models

- **InputMessage**: Avro schema for incoming Kafka messages
- **ResponseMessage**: Avro schema for outgoing Kafka messages with Trailer object
- **ServiceStatus**: Trailer object containing status, statusCode, and statusDesc
- **TransactionMessage**: Internal transaction representation
- **ProcessingResult**: Processing pipeline result with status and errors
- **Complex Event Support**: Header, Body, Processing Context, and ISO20022 message structures

### Configuration

- **KafkaConfig**: Kafka consumer/producer configuration with retry logic
- **RedisConfig**: Redis connection and template configuration

## Configuration

### Application Properties

```yaml
app:
  kafka:
    topics:
      input: transactions.incoming
      output: fast-outward-clearing
    consumer:
      group-id: fast-inward-clearing-processor
      auto-offset-reset: latest
    producer:
      acks: all
      retries: 3
      enable-idempotence: true
  
  idempotency:
    ttl-hours: 24
  
  processing:
    node-id: inward-processor-01
    pipeline:
      - idempotency-check
      - message-parsing
      - scheme-validation
      - response-creation
```

### Kafka Configuration

- **Consumer**: Manual acknowledgment, error handling, retry logic
- **Producer**: Idempotent producer, at-least-once delivery, compression
- **Topics**: Input, output, and DLQ topic configuration

### Redis Configuration

- **Connection**: Standalone Redis with connection pooling
- **Idempotency**: TTL-based duplicate prevention
- **Caching**: Transaction state and metadata storage

### Avro Schema Configuration

The service uses two main Avro schemas:

#### InputMessage.avsc
- **Purpose**: Defines the structure of incoming Kafka messages
- **Location**: `src/main/resources/avro/InputMessage.avsc`
- **Fields**: Header, Body, Procctxt, messages

#### ResponseMessage.avsc
- **Purpose**: Defines the structure of outgoing Kafka messages
- **Location**: `src/main/resources/avro/ResponseMessage.avsc`
- **Fields**: All InputMessage fields + Trailer object
- **Trailer**: Contains ServiceStatus with status, statusCode, and statusDesc

#### Schema Generation
- **Maven Plugin**: `avro-maven-plugin` generates Java classes
- **Output Directory**: `src/main/java/com/anz/fastpayment/inward/avro/`
- **Build Command**: `mvn clean compile`

## Processing Pipeline

The service implements a 4-step processing pipeline:

### 1. Idempotency Check
- **Purpose**: Prevent duplicate message processing
- **Implementation**: Redis-based MUID tracking
- **Behavior**: Logs duplicate messages and skips processing
- **TTL**: 24 hours (configurable)

### 2. Message Parsing
- **Purpose**: Convert Avro messages to internal representation
- **Implementation**: GenericRecord deserialization
- **Output**: TransactionMessage object
- **Error Handling**: Logs parsing failures

### 3. Scheme Validation
- **Purpose**: Validate message structure and business rules
- **Implementation**: Field validation and business rule checks
- **Output**: ValidationResult with success/failure status
- **Error Handling**: Creates failure response with validation errors

### 4. Response Creation
- **Purpose**: Create ResponseMessage with Trailer object
- **Implementation**: Copy original message fields + add Trailer
- **Output**: ResponseMessage with ServiceStatus
- **Success**: Trailer.status = "SUCCESS", statusCode = "200"
- **Failure**: Trailer.status = "FAILED", statusCode = "400", statusDesc = validation errors

## Business Rules

### Validation Rules
- **Field Validation**: Required field presence and format validation
- **Business Logic**: Amount limits, currency validation
- **Compliance**: ISO20022 message structure validation

### Error Handling
- **Validation Failures**: Return FAILED status with error details
- **Parsing Failures**: Log error and skip processing
- **Duplicate Messages**: Log and skip (no response sent)

## Error Handling

### Retry Logic

- Configurable retry attempts (default: 3)
- Exponential backoff strategy
- Non-retryable exception handling

### Dead Letter Queue

- Failed message routing to DLQ
- Error metadata preservation
- Manual reprocessing capability

### Idempotency

- Redis-based duplicate detection
- Configurable TTL for processed transactions
- Prevents duplicate processing

## Monitoring and Health Checks

### Health Endpoints

- `/health` - Basic health status
- `/health/detailed` - Detailed health with metrics
- `/health/ready` - Readiness probe
- `/health/live` - Liveness probe

### Metrics

- Messages processed counter
- Processing latency
- Error rates
- Business rule results

### Observability

- Prometheus metrics export
- Distributed tracing support
- Structured logging with SLF4J

## Deployment

### Docker

```bash
# Build the image
docker build -t fast-inward-clearing-processor .

# Run the container
docker run -p 8080:8080 \
  -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  -e REDIS_HOST=localhost \
  -e REDIS_PORT=6379 \
  fast-inward-clearing-processor
```

### Kubernetes

```bash
# Apply the deployment
kubectl apply -f k8s/deployment.yaml

# Check deployment status
kubectl get pods -n fast-payment -l app=fast-inward-clearing-processor
```

### Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `REDIS_HOST`: Redis server hostname
- `REDIS_PORT`: Redis server port
- `SPRING_PROFILES_ACTIVE`: Spring profile (local/gcp)
- `GCP_PROJECT_ID`: Google Cloud project ID
- `SPANNER_INSTANCE`: Cloud Spanner instance
- `SPANNER_DATABASE`: Cloud Spanner database

## Project Structure

```
fast-inward-clearing-processor/
├── src/
│   ├── main/
│   │   ├── java/                    # Java source code
│   │   │   └── com/anz/fastpayment/inward/
│   │   │       ├── avro/            # Generated Avro classes
│   │   │       ├── consumer/        # Kafka consumers
│   │   │       ├── handler/         # Processing handlers
│   │   │       ├── model/           # Data models
│   │   │       ├── config/          # Configuration classes
│   │   │       └── util/            # Utility classes
│   │   └── resources/
│   │       ├── avro/                # Avro schema files
│   │       ├── application.yml      # Application configuration
│   │       └── db/migration/        # Database migrations
│   └── test/                        # Java unit tests
├── playwright/                      # 🆕 Integrated Playwright test suite
│   ├── tests/                       # Test files organized by category
│   │   ├── smoke/                   # Smoke tests
│   │   ├── e2e/                     # End-to-end tests
│   │   ├── integration/             # Integration tests
│   │   ├── performance/             # Performance tests
│   │   ├── resilience/              # Error handling tests
│   │   ├── compliance/              # Security tests
│   │   ├── functional/              # Business logic tests
│   │   ├── unit/                    # Unit tests
│   │   └── helpers/                 # Test utilities
│   ├── schemas/                     # Test Avro schemas
│   ├── playwright.config.ts         # Playwright configuration
│   └── package.json                 # Node.js dependencies
├── target/                          # Compiled classes and JAR files
├── pom.xml                          # Maven configuration
└── README.md                        # This file
```

## Development

### Prerequisites

- Java 21
- Maven 3.8+
- Node.js 18+ (for Playwright tests)
- Docker
- Kafka cluster
- Redis instance
- Confluent Schema Registry (for Avro testing)

### Building

```bash
# Build the project
mvn clean package

# Run tests
mvn test

# Build Docker image
mvn docker:build
```

### Testing

#### Java Unit Tests

```bash
# Unit tests
mvn test

# Integration tests
mvn verify

# Test with Testcontainers
mvn test -Dtest=*IntegrationTest
```

#### Playwright End-to-End Tests

The service includes a comprehensive Playwright test suite located in the `playwright/` directory:

```bash
# Navigate to Playwright directory
cd playwright

# Install dependencies
npm install

# Run all tests
npm test

# Run specific test suites
npm test -- tests/smoke/smoke.spec.ts
npm test -- tests/e2e/end-to-end-pipeline.spec.ts

# Run with specific browser
npm test -- --project=chromium

# Generate test report
npm test -- --reporter=html
```

#### Test Categories

- **Smoke Tests** (`tests/smoke/`): Basic functionality validation
- **End-to-End Tests** (`tests/e2e/`): Complete pipeline testing
- **Integration Tests** (`tests/integration/`): Kafka integration validation
- **Performance Tests** (`tests/performance/`): Load and performance testing
- **Resilience Tests** (`tests/resilience/`): Error handling and recovery
- **Compliance Tests** (`tests/compliance/`): Security and compliance validation
- **Functional Tests** (`tests/functional/`): Business logic validation
- **Unit Tests** (`tests/unit/`): Individual component testing

#### Test Features

- **Avro Serialization**: Tests Avro message serialization/deserialization
- **Kafka Integration**: Validates message consumption and production
- **Schema Registry**: Tests Confluent Schema Registry integration
- **MUID Filtering**: Ensures test isolation with unique message IDs
- **Topic Management**: Automatic topic clearing and consumer group management
- **Response Validation**: Validates Trailer object and ServiceStatus
- **Error Scenarios**: Tests duplicate messages, validation failures, and parsing errors

## Performance and Scalability

### JVM Optimizations

- ZGC garbage collector
- Virtual threads support
- Memory tuning (512MB - 2GB)
- Preview features enabled

### Kafka Configuration

- Consumer concurrency: 3 partitions
- Producer batching and compression
- Idempotent producer for exactly-once semantics
- Manual acknowledgment for control

### Resource Requirements

- **CPU**: 250m - 1000m
- **Memory**: 512Mi - 2Gi
- **Replicas**: 3 (configurable)
- **Health Checks**: 30s intervals

## Security

### Container Security

- Non-root user execution
- Privilege escalation disabled
- Capability dropping
- Read-only root filesystem (recommended)

### Network Security

- Internal service communication
- Configurable network policies
- TLS encryption support
- Authentication and authorization

## Troubleshooting

### Common Issues

1. **Kafka Connection**: Check broker addresses and network connectivity
2. **Redis Connection**: Verify Redis host/port and authentication
3. **Message Processing**: Check business rule configuration
4. **Performance**: Monitor JVM metrics and Kafka lag

### Logs

```bash
# View application logs
kubectl logs -f deployment/fast-inward-clearing-processor -n fast-payment

# Check health status
curl http://localhost:8080/health/detailed
```

### Metrics

- Prometheus metrics at `/actuator/prometheus`
- Business metrics in health endpoints
- Kafka consumer lag monitoring
- Redis connection status

## Contributing

1. Follow the existing code style and patterns
2. Add comprehensive tests for new features
3. Update documentation for configuration changes
4. Ensure backward compatibility for existing deployments

## License

This project is proprietary to ANZ Bank and subject to internal licensing terms.