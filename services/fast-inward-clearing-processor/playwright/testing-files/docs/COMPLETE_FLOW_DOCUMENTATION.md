# Complete End-to-End Flow Documentation

## 🏗️ System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   External      │    │   Kafka         │    │   Your          │
│   System        │───▶│   Topics        │───▶│   Application   │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   Schema        │    │   Spanner       │
                       │   Registry      │    │   Database      │
                       │                 │    │                 │
                       └─────────────────┘    └─────────────────┘
```

## 🔄 Complete Flow Steps

### Phase 1: Infrastructure Setup
1. **Docker Services Start**
   - Zookeeper
   - Kafka
   - Schema Registry
   - Spanner Emulator

2. **Spanner Database Initialization**
   - Create instance via REST API
   - Create database with tables
   - No gcloud CLI dependency

3. **Spring Boot Application Start**
   - Auto-register static schemas
   - Start Kafka listeners
   - Connect to Spanner

### Phase 2: Event Processing Flow

#### Step 1: Event Reception
```
External System → Kafka Topic → Your Application
```

**Event Structure:**
```json
{
  "eventId": "avro-schema-event-1234567890",
  "eventType": "AVRO_MESSAGE_SCHEMA",
  "schemaName": "PaymentConfirmationMessage",
  "avroMessageSchema": "{...complex avro schema...}",
  "messageContent": "{...actual message data...}",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "source": "external-payment-system",
  "version": "1.0.0"
}
```

#### Step 2: Schema Registration (PRIORITY)
```java
@KafkaListener(topics = "schema-events")
public void handleSchemaEvent(@Payload String eventPayload) {
    // STEP 1: Extract schema from event
    String avroMessageSchema = eventNode.get("avroMessageSchema").asText();
    String schemaName = eventNode.get("schemaName").asText();
    
    // STEP 2: Auto-register schema FIRST
    boolean schemaRegistered = autoRegisterAvroMessageSchema(schemaName, avroMessageSchema);
    
    // STEP 3: Process message only if schema registered successfully
    if (schemaRegistered) {
        processMessageContent(eventNode);
    }
}
```

#### Step 3: Schema Registration Process
1. **Check Local Cache**
   ```java
   if (registeredSchemas.contains(subjectName)) {
       return true; // Already registered
   }
   ```

2. **Check Schema Registry**
   ```java
   if (schemaExistsInRegistry(subjectName)) {
       registeredSchemas.add(subjectName);
       return true; // Already exists
   }
   ```

3. **Register New Schema**
   ```java
   // POST to Schema Registry
   String registrationUrl = schemaRegistryUrl + "/subjects/" + subjectName + "/versions";
   ResponseEntity<String> response = restTemplate.exchange(registrationUrl, HttpMethod.POST, request, String.class);
   ```

#### Step 4: Message Processing
```java
private void processMessageContent(JsonNode eventNode) {
    // Extract message content
    String messageContent = eventNode.get("messageContent").asText();
    
    // Process the actual message
    // - Validate data
    // - Transform if needed
    // - Store in Spanner
    // - Send to other systems
}
```

## 📊 Data Flow Examples

### Example 1: New Schema Registration
```
Input Event:
{
  "schemaName": "PaymentConfirmationMessage",
  "avroMessageSchema": "{\"type\":\"record\",\"name\":\"PaymentConfirmationMessage\",...}",
  "messageContent": "{\"confirmationId\":\"CONF-123\",\"status\":\"CONFIRMED\",...}"
}

Processing:
1. Extract schema: PaymentConfirmationMessage
2. Check if registered: NO
3. Register schema: ✅ SUCCESS (ID: 2)
4. Process message: ✅ SUCCESS
5. Cache schema: PaymentConfirmationMessage → registered

Result:
- Schema available in Schema Registry
- Message processed successfully
- Future messages with same schema will skip registration
```

### Example 2: Duplicate Schema (Already Registered)
```
Input Event:
{
  "schemaName": "PaymentConfirmationMessage", // Same as before
  "avroMessageSchema": "{...same schema...}",
  "messageContent": "{\"confirmationId\":\"CONF-456\",\"status\":\"PENDING\",...}"
}

Processing:
1. Extract schema: PaymentConfirmationMessage
2. Check if registered: YES (in local cache)
3. Register schema: SKIP (already exists)
4. Process message: ✅ SUCCESS

Result:
- No duplicate registration
- Message processed successfully
- Faster processing (no API call)
```

## 🔧 Configuration Files

### application.yml
```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    producer:
      properties:
        schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8081}
        auto.register.schemas: true
    consumer:
      properties:
        schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8081}
        auto.register.schemas: true
  schema:
    event:
      topic: schema-events
  cloud:
    gcp:
      spanner:
        emulator:
          enabled: true
        emulator-host: ${SPANNER_EMULATOR_HOST:localhost:9010}
```

### docker-compose.yml
```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    ports: ["2181:2181"]
  
  kafka:
    image: confluentinc/cp-kafka:latest
    ports: ["9092:9092"]
    depends_on: [zookeeper]
  
  schema-registry:
    image: confluentinc/cp-schema-registry:latest
    ports: ["8081:8081"]
    depends_on: [kafka]
  
  spanner-emulator:
    image: gcr.io/cloud-spanner-emulator/emulator:latest
    ports: ["9010:9010", "9020:9020"]
```

## 🚀 Automated Test Flow

### run-tests-automated.sh
```bash
#!/bin/bash
# Step 1: Cleanup existing services
docker-compose down --remove-orphans -v

# Step 2: Start Docker infrastructure
docker-compose up -d

# Step 3: Initialize Spanner database
bash scripts/init-spanner-rest.sh

# Step 4: Register static schemas
bash scripts/register-schema.sh

# Step 5: Start Spring Boot application
mvn spring-boot:run -q &

# Step 6: Run Playwright tests
cd playwright && npm test

# Step 7: Cleanup
docker-compose down
```

## 📝 Log Output Examples

### Successful Flow
```
INFO  - Received schema event from topic: schema-events, partition: 0, offset: 123
INFO  - 🔄 STEP 1: Auto-registering Avro message schema FIRST: PaymentConfirmationMessage
INFO  - Auto-registering new Avro message schema: com.anz.fastpayment.inward.avro.PaymentConfirmationMessage
INFO  - ✅ Successfully auto-registered Avro message schema: com.anz.fastpayment.inward.avro.PaymentConfirmationMessage
INFO  - Schema com.anz.fastpayment.inward.avro.PaymentConfirmationMessage registered with ID: 2
INFO  - ✅ STEP 2: Schema registered successfully, now processing message content
INFO  - 📋 Processing message content...
INFO  - Message content: {"confirmationId":"CONF-123","originalPaymentId":"PAY-123456789",...}
INFO  - ✅ Message content processing completed
```

### Duplicate Schema Flow
```
INFO  - Received schema event from topic: schema-events, partition: 0, offset: 124
INFO  - 🔄 STEP 1: Auto-registering Avro message schema FIRST: PaymentConfirmationMessage
INFO  - Schema com.anz.fastpayment.inward.avro.PaymentConfirmationMessage already registered locally, skipping
INFO  - ✅ STEP 2: Schema registered successfully, now processing message content
INFO  - 📋 Processing message content...
INFO  - Message content: {"confirmationId":"CONF-456","originalPaymentId":"PAY-987654321",...}
INFO  - ✅ Message content processing completed
```

## 🛡️ Error Handling

### Schema Registration Failure
```
ERROR - ❌ Failed to register schema com.anz.fastpayment.inward.avro.PaymentConfirmationMessage: 400 Bad Request
WARN  - ⚠️ Schema registration failed, skipping message processing
```

### Message Processing Failure
```
INFO  - ✅ Successfully auto-registered Avro message schema: com.anz.fastpayment.inward.avro.PaymentConfirmationMessage
ERROR - Failed to process message content: Invalid JSON format
```

## 🔍 Monitoring & Observability

### Schema Registry API
```bash
# List all subjects
curl http://localhost:8081/subjects

# Get schema details
curl http://localhost:8081/subjects/com.anz.fastpayment.inward.avro.PaymentConfirmationMessage/versions/latest

# Check compatibility
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "..."}' \
  http://localhost:8081/compatibility/subjects/PaymentConfirmationMessage/versions/latest
```

### Application Metrics
- Schema registration success/failure rates
- Message processing latency
- Cache hit/miss ratios
- Error rates by event type

## 🎯 Key Benefits

1. **🔄 Automatic**: No manual schema registration
2. **🛡️ Safe**: Duplicate prevention and error handling
3. **⚡ Fast**: Local caching for performance
4. **📊 Monitored**: Full logging and observability
5. **🔧 Configurable**: Environment-based configuration
6. **🧪 Testable**: Complete automated test suite
7. **🚀 Scalable**: Handles multiple concurrent events
8. **🔒 Secure**: No manual access to Schema Registry needed

## 🎉 Summary

The complete flow ensures that:
- **Schemas are registered FIRST** before message processing
- **No duplicates** are created in Schema Registry
- **Messages are processed** only after successful schema registration
- **Everything is automated** with no manual intervention
- **Full observability** with comprehensive logging
- **Error handling** prevents data corruption
- **Performance optimization** through local caching
