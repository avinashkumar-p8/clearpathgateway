# Event Saving and Idempotency Integration Tests

## Overview
This test suite provides comprehensive integration testing for the event saving and idempotency functionality using the existing Kafka infrastructure and Avro schemas.

## Test Files
- `idempotency-integration.spec.ts` - Main integration test suite

## Prerequisites
1. **Kafka Infrastructure**: Must be running via `npm run docker:up`
2. **Fast Inward Clearing Processor Service**: Must be running on port 8080
3. **Schema Registry**: Must be accessible on port 8081
4. **Kafka Brokers**: Must be accessible on port 9092

## Environment Variables
```bash
SERVICE_URL=http://localhost:8080
KAFKA_BROKERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
```

## Running Tests

### Run All Integration Tests
```bash
npm run test:integration
```

### Run with UI
```bash
npm run test:integration:ui
```

### Run with Headed Browser
```bash
npm run test:integration:headed
```

### Run in Debug Mode
```bash
npm run test:integration:debug
```

## Test Scenarios

### 1. MUID-based Idempotency Processing
- **New MUID Processing**: Produces event with new MUID and verifies successful processing
- **Duplicate Detection**: Sends duplicate MUID and verifies it's skipped
- **Multiple Unique MUIDs**: Processes multiple unique MUIDs correctly
- **Database State Verification**: Validates database state and structured logging
- **Concurrent Processing**: Handles concurrent MUID processing correctly

### 2. Error Handling and Transaction Safety
- **Service Resilience**: Verifies service remains healthy during errors
- **Cache Operations**: Tests idempotency cache operations

## Test Data
Tests use realistic transaction data with:
- Transaction IDs
- Amounts and currencies
- Country codes
- Timestamps
- Metadata

## Assertions
Tests validate:
- ✅ Message processing success
- ✅ Duplicate MUID detection
- ✅ Database persistence
- ✅ Service health endpoints
- ✅ Metrics and monitoring
- ✅ Cache operations
- ✅ Concurrent processing
- ✅ Error handling

## Architecture
- **Kafka Producer**: Sends test messages to input topic
- **Kafka Consumer**: Consumes from output topic for verification
- **Schema Registry**: Manages Avro schemas for message serialization
- **Service Endpoints**: Validates service health and metrics
- **Buffer Serialization**: Properly serializes messages and headers

## Best Practices
- Uses unique group IDs to avoid consumer conflicts
- Proper message serialization with Avro schemas
- Comprehensive error handling and validation
- Realistic test data and scenarios
- Proper cleanup and resource management
- Structured logging and assertions

## Troubleshooting

### Common Issues
1. **Service Not Running**: Ensure Fast Inward Clearing Processor is running
2. **Kafka Connection**: Verify Kafka infrastructure is up
3. **Schema Registry**: Check Schema Registry accessibility
4. **Port Conflicts**: Ensure ports 8080, 8081, 9092 are available

### Debug Commands
```bash
# Check service health
curl http://localhost:8080/api/v1/idempotency/health

# Check Kafka topics
docker exec kafka-test kafka-topics --list --bootstrap-server localhost:9092

# View service logs
docker logs <service-container-id>
```

## Performance Considerations
- Tests include appropriate wait times for message processing
- Concurrent message testing for performance validation
- Resource cleanup after each test
- Efficient consumer group management
