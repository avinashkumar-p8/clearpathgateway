# 🚀 Kafka + Avro Setup Guide

## 📋 **What This Guide Contains**
- Docker infrastructure setup
- Maven dependencies
- Application configuration
- Avro schema files
- Basic usage commands
- **Testing with Playwright**

## 🐳 **Docker Infrastructure**

### Start Services
```bash
docker-compose up -d
```

**Services:**
- Zookeeper (port 2181)
- Kafka (port 9092)
- Schema Registry (port 8081)

## 📦 **Maven Dependencies**

### Required Dependencies
```xml
<!-- Avro -->
<dependency>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro</artifactId>
    <version>1.11.3</version>
</dependency>

<!-- Confluent Kafka Avro Serializer -->
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.3.0</version>
</dependency>

<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-schema-registry-client</artifactId>
    <version>7.3.0</version>
</dependency>
```

### Confluent Repository
```xml
<repository>
    <id>confluent</id>
    <url>https://packages.confluent.io/maven/</url>
</repository>
```

## ⚙️ **Application Configuration**

### application.yml
```yaml
spring:
  kafka:
    producer:
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
      properties:
        schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8081}
    consumer:
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      properties:
        schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8081}
        specific.avro.reader: true
```

### ⚙️ **SpecificRecord vs GenericRecord Configuration**

The `specific.avro.reader` setting determines how Avro messages are deserialized:

#### **specific.avro.reader: true** (Recommended for this project)
- **Use when**: Your code uses generated Avro classes (SpecificRecord)
- **Benefits**: Type-safe, better performance, direct field access
- **Example**: `UnifiedPaymentMessage message = (UnifiedPaymentMessage) avroRecord;`
- **Current code**: Uses this approach with generated `UnifiedPaymentMessage` and `ProcessedTransactionMessage` classes

#### **specific.avro.reader: false** (GenericRecord approach)
- **Use when**: Working with generic Avro records without generated classes
- **Benefits**: Flexible, works with any Avro schema
- **Example**: `String id = (String) genericRecord.get("transactionId");`
- **Note**: Requires manual field extraction and type casting

### 🔧 **Environment Variable Configuration**

The `SCHEMA_REGISTRY_URL` is parameterized to support different environments:
- **Local Development**: `http://localhost:8081`
- **Docker Compose**: `http://schema-registry:8081`
- **Production**: Set via environment variable or deployment configuration

### 🛠️ **POJO ↔ Avro Utility Examples**

The project includes `AvroConverter` utility class that demonstrates the recommended approach:

#### **Avro to POJO Conversion (Consumer)**
```java
// The converter accepts GenericRecord but casts to SpecificRecord
public static TransactionMessage convertToTransactionMessage(GenericRecord avroRecord) {
    if (avroRecord == null) {
        return null;
    }
    
    try {
        // Cast to the specific Avro class (requires specific.avro.reader: true)
        UnifiedPaymentMessage unifiedMessage = (UnifiedPaymentMessage) avroRecord;
        
        TransactionMessage transactionMessage = new TransactionMessage();
        transactionMessage.setTransactionId(unifiedMessage.getTransactionId());
        transactionMessage.setAmount(BigDecimal.valueOf(unifiedMessage.getAmount()));
        // ... other fields
        return transactionMessage;
    } catch (Exception e) {
        // Handle conversion errors
    }
}
```

#### **POJO to Avro Conversion (Producer)**
```java
// Create Avro messages using generated SpecificRecord classes
public static GenericRecord convertToAvroRecord(ProcessedTransactionMessage processedMessage) {
    // Create Avro ProcessedTransactionMessage using the generated class
    com.anz.fastpayment.inward.avro.ProcessedTransactionMessage avroMessage = 
        new com.anz.fastpayment.inward.avro.ProcessedTransactionMessage();
    
    // Set all fields from the POJO
    avroMessage.setTransactionId(processedMessage.getTransactionId());
    avroMessage.setAmount(processedMessage.getAmount() != null ? 
        processedMessage.getAmount().doubleValue() : 0.0);
    // ... other fields
    return avroMessage;
}
```

**Note**: Even though the method signature returns `GenericRecord`, the implementation creates and returns specific Avro classes. This hybrid approach provides flexibility while maintaining type safety.

## 📋 **Avro Schema Files**

### Location
```
src/main/resources/avro/
├── UnifiedPaymentMessage.avsc
└── ProcessedTransactionMessage.avsc
```

## 🧪 **Testing with Playwright**

### Test Package Location
```
playwright/
├── package.json          # Dependencies and scripts
├── playwright.config.ts  # Playwright configuration
├── tsconfig.json         # TypeScript configuration
├── tests/                # Test files
│   └── kafka-avro.spec.ts
├── README.md             # Test package documentation
└── run-tests.sh         # Test runner script
```

### Quick Test Setup
```bash
cd playwright
npm install
npx playwright install
npm run docker:up
npm test
```

### Test Coverage
- ✅ Kafka connectivity validation
- ✅ Avro message serialization/deserialization
- ✅ Schema Registry integration
- ✅ Message production and consumption
- ✅ Error handling scenarios

## 🚀 **Usage Commands**

### Build Service
```bash
cd services/fast-inward-clearing-processor
mvn clean compile
```

### Run Service
```bash
mvn spring-boot:run
```

### Check Topics
```bash
kafka-topics --list --bootstrap-server localhost:9092
```

### Monitor Schema Registry
```
http://localhost:8081
```

## 📊 **Kafka Topics**
- **Input**: `transactions.incoming`
- **Output**: `transactions.processed`
- **DLQ**: `transactions.dlq`

## ✅ **What You Get**
- Kafka producer with Avro serialization
- Kafka consumer with Avro deserialization
- Schema validation and evolution
- Production-ready configuration
- **Comprehensive test suite** with Playwright
