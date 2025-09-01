# Message Idempotency System for Banking Operations

## Overview

The Message Idempotency System ensures **exactly-once processing** of Kafka messages in the Fast Inward Clearing Processor. This is critical for banking software to prevent duplicate transactions, double processing, and financial inconsistencies.

## Architecture

### Core Components

1. **MessageUniqueId Entity** - JPA entity for storing MUIDs
2. **MessageIdempotencyService** - Core service for idempotency logic
3. **IdempotentTransactionConsumer** - Enhanced Kafka consumer with idempotency
4. **MessageUniqueIdRepository** - Data access layer for MUID operations
5. **IdempotencyHealthController** - Health monitoring and metrics

### Data Flow

```
Kafka Message → Extract MUID → Check Cache → Check Database → Process/Reject
     ↓              ↓           ↓           ↓           ↓
  Consumer    MUID Header   In-Memory   Spanner DB   Business Logic
              or Fallback    Cache       Persistence
```

## Key Features

### 1. Exactly-Once Processing
- **MUID-based deduplication** using Message Unique ID
- **Database persistence** in Google Cloud Spanner
- **In-memory caching** for performance optimization
- **Transaction safety** with proper rollback mechanisms

### 2. Security & Compliance
- **Audit logging** for all operations
- **Sensitive data masking** in logs
- **Parameterized queries** to prevent SQL injection
- **Structured logging** for compliance requirements

### 3. Performance & Scalability
- **Ehcache integration** for fast MUID lookups
- **TTL-based cache eviction** to prevent memory bloat
- **Batch database operations** for efficiency
- **Async processing** where appropriate

### 4. Monitoring & Observability
- **Real-time metrics** via REST endpoints
- **Health checks** for system monitoring
- **Duplicate detection rates** for operational insights
- **Error tracking** and alerting capabilities

## Implementation Details

### MUID Extraction Strategy

```java
private String extractMuid(ConsumerRecord<String, GenericRecord> consumerRecord, String transactionId) {
    // Priority 1: MUID from Kafka headers
    if (consumerRecord.headers() != null) {
        var muidHeader = consumerRecord.headers().lastHeader("muid");
        if (muidHeader != null && muidHeader.value() != null) {
            return new String(muidHeader.value(), StandardCharsets.UTF_8).trim();
        }
    }
    
    // Priority 2: Fallback to transaction ID
    return transactionId != null ? transactionId : "UNKNOWN-" + System.currentTimeMillis();
}
```

### Idempotency Check Flow

```java
@Transactional(propagation = Propagation.REQUIRED)
public boolean isMessageNewAndRegister(String muid, String topic, Integer partition, Long offset, String eventPayload) {
    // 1. Check cache first (fast path)
    if (isMuidInCache(muid)) {
        log.info("Duplicate MUID detected in cache: {} - skipping processing", muid);
        return false;
    }
    
    // 2. Check database (persistent storage)
    if (muidRepository.existsByMuidAndActive(muid)) {
        log.info("Duplicate MUID detected in database: {} - skipping processing", muid);
        addMuidToCache(muid); // Cache for future lookups
        return false;
    }
    
    // 3. Register new MUID
    MessageUniqueId muidRecord = new MessageUniqueId(muid, topic, partition, offset, eventPayload);
    muidRecord.setProcessingStatus("PROCESSING");
    MessageUniqueId savedRecord = muidRepository.save(muidRecord);
    
    // 4. Add to cache
    addMuidToCache(muid);
    
    return true; // Process this message
}
```

### Caching Strategy

- **Cache Type**: Spring Cache with ConcurrentMapCacheManager
- **TTL**: 1 hour (configurable)
- **Max Entries**: 10,000 (configurable)
- **Eviction Policy**: LRU (Least Recently Used)
- **Cache Names**: `muidCache`, `currencyCache`, `countryCache`, `countryByCurrencyCache`

## Database Schema

### message_unique_ids Table

```sql
CREATE TABLE message_unique_ids (
    id INT64 NOT NULL,
    muid STRING(255) NOT NULL,
    topic STRING(100) NOT NULL,
    partition INT64 NOT NULL,
    offset INT64 NOT NULL,
    event_payload STRING(MAX),
    processing_status STRING(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP,
    is_active BOOL NOT NULL
) PRIMARY KEY (id);

-- Indexes for performance
CREATE UNIQUE INDEX idx_message_unique_ids_muid ON message_unique_ids (muid);
CREATE INDEX idx_message_unique_ids_topic_partition ON message_unique_ids (topic, partition);
CREATE INDEX idx_message_unique_ids_created_at ON message_unique_ids (created_at);
CREATE INDEX idx_message_unique_ids_status ON message_unique_ids (processing_status);
```

### Processing Status Values

- `RECEIVED` - Message received, not yet processed
- `PROCESSING` - Message currently being processed
- `COMPLETED` - Message processed successfully
- `PROCESSING_FAILED` - Processing failed, sent to DLQ
- `DUPLICATE_SKIPPED` - Duplicate detected, processing skipped
- `ERROR` - Unexpected error during processing

## Configuration

### Application Properties

```yaml
app:
  idempotency:
    cache:
      muid:
        ttl-seconds: 3600      # 1 hour TTL
        max-entries: 10000     # Max cache entries
        eviction-policy: LRU   # Eviction strategy
    
    database:
      cleanup:
        enabled: true
        retention-days: 90     # Keep records for 90 days
        batch-size: 1000       # Cleanup batch size
        schedule: "0 2 * * *"  # Daily at 2 AM
    
    logging:
      level: INFO
      include-muid: true       # Log MUID for audit
      include-topic: false     # Don't log topic (security)
      include-partition: false # Don't log partition (security)
      include-offset: false    # Don't log offset (security)
    
    monitoring:
      enabled: true
      metrics-prefix: "kafka.idempotency"
      health-check:
        enabled: true
        timeout-seconds: 30
    
    error-handling:
      max-retries: 3
      backoff-multiplier: 2.0
      initial-delay-ms: 1000
    
    security:
      audit-logging: true
      sensitive-data-masking: true
      encryption:
        enabled: false
        algorithm: "AES-256"
```

## API Endpoints

### Health Check

```http
GET /api/v1/idempotency/health
```

**Response:**
```json
{
  "status": "HEALTHY",
  "timestamp": "2024-01-15T10:30:00",
  "service": "Message Idempotency Service",
  "consumerMetrics": {
    "totalMessages": 1500,
    "processedMessages": 1450,
    "duplicateMessages": 50,
    "errorMessages": 0,
    "duplicateDetectionRate": 0.033
  },
  "cacheStatus": "OPERATIONAL"
}
```

### Metrics

```http
GET /api/v1/idempotency/metrics
```

**Response:**
```json
{
  "timestamp": "2024-01-15T10:30:00",
  "consumer": {
    "totalMessages": 1500,
    "processedMessages": 1450,
    "duplicateMessages": 50,
    "errorMessages": 0
  },
  "successRate": 0.967,
  "duplicateRate": 0.033,
  "errorRate": 0.0
}
```

### Cache Management

```http
GET /api/v1/idempotency/cache/clear
```

**Response:**
```json
{
  "status": "SUCCESS",
  "message": "MUID cache cleared successfully",
  "timestamp": "2024-01-15T10:30:00"
}
```

## Monitoring & Metrics

### Key Metrics

1. **Message Processing Rate**
   - Total messages received
   - Successfully processed messages
   - Duplicate detection rate
   - Error rate

2. **Performance Metrics**
   - Cache hit/miss ratios
   - Database query performance
   - Processing latency

3. **Operational Metrics**
   - System health status
   - Cache operational status
   - Database connectivity

### Alerting

- **High duplicate rate** (>5% may indicate system issues)
- **Cache failures** (performance degradation)
- **Database errors** (data integrity issues)
- **Processing failures** (business logic issues)

## Security Considerations

### Data Protection

- **MUID masking** in logs (configurable)
- **Sensitive field filtering** in audit logs
- **Encryption support** for sensitive data
- **Access control** for health endpoints

### Audit Compliance

- **Structured logging** for compliance tools
- **Timestamp tracking** for all operations
- **Status tracking** for message lifecycle
- **Error logging** with context preservation

## Error Handling

### Retry Strategy

- **Exponential backoff** for transient failures
- **Maximum retry limits** to prevent infinite loops
- **Dead Letter Queue** for failed messages
- **Graceful degradation** for non-critical failures

### Failure Scenarios

1. **Cache Failure**
   - Fallback to database-only operations
   - Log performance degradation
   - Continue processing with reduced performance

2. **Database Failure**
   - Reject new messages (fail-fast)
   - Log critical errors
   - Alert operations team

3. **Processing Failure**
   - Update MUID status to ERROR
   - Send message to DLQ
   - Preserve error context for investigation

## Performance Tuning

### Cache Optimization

- **TTL tuning** based on message volume
- **Cache size** based on memory availability
- **Eviction policy** selection (LRU vs LFU)
- **Cache warming** for critical MUIDs

### Database Optimization

- **Index optimization** for query patterns
- **Batch operations** for cleanup tasks
- **Connection pooling** for high throughput
- **Query optimization** for MUID lookups

## Deployment Considerations

### Environment Setup

1. **Database Migration**
   - Run `V1__create_message_unique_ids_table.sql`
   - Verify indexes are created
   - Test with sample data

2. **Configuration**
   - Set appropriate cache sizes
   - Configure cleanup schedules
   - Set monitoring thresholds

3. **Monitoring**
   - Enable health checks
   - Set up alerting
   - Configure log aggregation

### Scaling Considerations

- **Horizontal scaling** with shared cache
- **Database sharding** for high volume
- **Cache clustering** for multi-node deployment
- **Load balancing** for health endpoints

## Testing

### Unit Tests

- **IdempotencyService** - Core logic testing
- **Consumer** - Message processing testing
- **Repository** - Data access testing
- **Controller** - API endpoint testing

### Integration Tests

- **End-to-end** message processing
- **Cache integration** testing
- **Database persistence** testing
- **Error handling** scenarios

### Performance Tests

- **High volume** message processing
- **Cache performance** under load
- **Database performance** with large datasets
- **Memory usage** monitoring

## Troubleshooting

### Common Issues

1. **High Duplicate Rate**
   - Check MUID generation logic
   - Verify cache configuration
   - Review database performance

2. **Cache Failures**
   - Check memory availability
   - Verify cache configuration
   - Review cache eviction policies

3. **Database Errors**
   - Check connection pool settings
   - Verify database connectivity
   - Review query performance

### Debug Commands

```bash
# Check cache status
curl /api/v1/idempotency/health

# View detailed metrics
curl /api/v1/idempotency/metrics

# Clear cache manually
curl /api/v1/idempotency/cache/clear

# Check application logs
tail -f logs/application.log | grep IDEMPOTENCY
```

## Future Enhancements

### Planned Features

1. **Advanced Caching**
   - Redis integration for distributed caching
   - Cache warming strategies
   - Predictive caching

2. **Enhanced Monitoring**
   - Real-time dashboards
   - Predictive analytics
   - Automated alerting

3. **Performance Optimization**
   - Async processing
   - Batch operations
   - Stream processing

4. **Security Enhancements**
   - Encryption at rest
   - Advanced access controls
   - Audit trail enhancements

## Conclusion

The Message Idempotency System provides a robust, scalable, and secure solution for ensuring exactly-once processing of Kafka messages in banking operations. With comprehensive monitoring, error handling, and performance optimization, it meets the critical requirements of financial systems while maintaining operational efficiency.

For questions or support, please refer to the development team or create an issue in the project repository.

