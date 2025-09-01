# 🧪 Kafka + Avro Test Suite

Playwright-based test suite for validating Kafka functionality with Avro schema integration.

## 📋 **What This Tests**

- ✅ Kafka connectivity and topic management
- ✅ Avro message serialization/deserialization
- ✅ Schema Registry integration
- ✅ Message production and consumption
- ✅ Error handling and edge cases
- ✅ Large message handling

## 🚀 **Quick Start**

### 1. Install Dependencies
```bash
cd playwright
npm install
```

### 2. Install Browsers
```bash
npx playwright install
```

### 3. Start Kafka Infrastructure
```bash
npm run docker:up
```

### 4. Run Tests
```bash
npm test
```

## 📦 **Available Scripts**

- **`npm test`** - Run all tests
- **`npm run test:headed`** - Run tests with browser UI
- **`npm run test:ui`** - Open Playwright UI mode
- **`npm run test:debug`** - Run tests in debug mode
- **`npm run report`** - Show test report
- **`npm run docker:up`** - Start Kafka infrastructure
- **`npm run docker:down`** - Stop Kafka infrastructure
- **`npm run kafka:test`** - Start infrastructure and run tests

## 🧪 **Test Coverage**

### Core Kafka Tests
- Connection validation
- Topic metadata retrieval
- Producer/consumer operations

### Avro Integration Tests
- Schema validation
- Message serialization
- Message deserialization
- Schema Registry connectivity

### Edge Cases
- Large message handling
- Invalid Avro data
- Error scenarios

## 🔧 **Configuration**

- **Kafka**: localhost:9092
- **Schema Registry**: localhost:8081
- **Zookeeper**: localhost:2181
- **Test Topics**: transactions.incoming, transactions.processed

## 📊 **Test Results**

After running tests, view detailed reports:
```bash
npm run report
```

## 🐳 **Dependencies**

- **Playwright**: Browser automation and testing
- **KafkaJS**: Kafka client for Node.js
- **Avsc**: Avro schema handling
- **Docker Compose**: Kafka infrastructure
