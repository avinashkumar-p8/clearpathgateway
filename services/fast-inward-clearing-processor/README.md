# Fast Inward Clearing Processor Service

A Kafka-based clearing processor service that handles CTI (Credit Transfer Inward) and DDI (Direct Debit Inward) processing with 4.5-second SLA compliance for the Singapore Fast Payment system.

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

## Architecture

```
Input Topic (transactions.incoming)
           ↓
   Transaction Consumer
           ↓
   Clearing Processor Service
           ↓
   ├── Validation
   ├── Enrichment
   ├── Business Rules
   └── Idempotency Check
           ↓
   Output Topic (transactions.processed)
           ↓
   Dead Letter Queue (transactions.dlq) [on failure]
```

## Components

### Core Services

- **ClearingProcessorService**: Main business logic for transaction processing
- **TransactionConsumer**: Kafka consumer with error handling and DLQ support
- **HealthController**: Monitoring and health check endpoints

### Models

- **TransactionMessage**: Input transaction representation (extracted from complex nested structure)
- **ProcessedTransactionMessage**: Enriched and processed transaction
- **BusinessRuleResults**: Business rule validation results
- **EnrichmentData**: Additional metadata and processing information
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
      output: transactions.processed
      dlq: transactions.dlq
    consumer:
      max-retries: 3
      retry-delay: 1000
      concurrency: 3
    producer:
      acks: all
      retries: 3
      enable-idempotence: true
  
  business-rules:
    max-amount: SGD 1000000
    high-risk-countries: XX,YY,ZZ
  
  idempotency:
    ttl-hours: 24
  
  processing:
    node-id: inward-processor-01
```

### Kafka Configuration

- **Consumer**: Manual acknowledgment, error handling, retry logic
- **Producer**: Idempotent producer, at-least-once delivery, compression
- **Topics**: Input, output, and DLQ topic configuration

### Redis Configuration

- **Connection**: Standalone Redis with connection pooling
- **Idempotency**: TTL-based duplicate prevention
- **Caching**: Transaction state and metadata storage

## Business Rules

### Amount Limits

- Configurable maximum transaction amounts
- Currency-specific limit enforcement
- Risk-based amount validation

### Compliance Checks

- High-risk country detection
- Transaction type validation
- Priority-based risk assessment

### Risk Scoring

- Amount-based risk calculation
- Transaction type risk factors
- Priority-based risk adjustment
- Automatic blocking for high-risk transactions

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

## Development

### Prerequisites

- Java 21
- Maven 3.8+
- Docker
- Kafka cluster
- Redis instance

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

```bash
# Unit tests
mvn test

# Integration tests
mvn verify

# Test with Testcontainers
mvn test -Dtest=*IntegrationTest
```

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