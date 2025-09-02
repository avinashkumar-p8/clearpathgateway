# Fast Inward Clearing Processor - Playwright Test Suite

This package contains comprehensive Playwright tests for the `fast-inward-clearing-processor` service, covering unit tests, integration tests, and end-to-end pipeline testing.

## 🏗️ **Test Architecture**

### **Test Categories:**
- **Unit Tests** (`tests/unit/`): Individual component testing
- **Integration Tests** (`tests/integration/`): Kafka and service integration
- **End-to-End Tests** (`tests/e2e/`): Complete pipeline validation

### **Helper Modules:**
- **`kafka-helper.ts`**: Kafka message production/consumption
- **`http-helper.ts`**: REST API testing utilities
- **`test-data.ts`**: Sample Avro messages and test scenarios

## 🚀 **Quick Start**

### **1. Install Dependencies**
```bash
npm install
```

### **2. Install Playwright Browsers**
```bash
npm run install-browsers
```

### **3. Configure Environment**
Create `.env` file with your configuration:
```env
# Kafka Configuration
KAFKA_BROKERS=localhost:9092
INPUT_TOPIC=transactions.incoming
OUTPUT_TOPIC=transactions.processed
DLQ_TOPIC=transactions.dlq

# Service Configuration
BASE_URL=http://localhost:8080

# Test Configuration
TEST_TIMEOUT=30000
```

### **4. Run Tests**

#### **Run All Tests**
```bash
npm test
```

#### **Run Specific Test Categories**
```bash
# Unit tests only
npx playwright test tests/unit/

# Integration tests only
npx playwright test tests/integration/

# E2E tests only
npx playwright test tests/e2e/
```

#### **Run Tests with UI**
```bash
npm run test:ui
```

#### **Run Tests in Debug Mode**
```bash
npm run test:debug
```

#### **Run Tests in Headed Mode**
```bash
npm run test:headed
```

## 📋 **Test Coverage**

### **Unit Tests**
- ✅ Health Controller endpoints
- ✅ Service status responses
- ✅ Error handling

### **Integration Tests**
- ✅ Kafka message processing
- ✅ Message validation pipeline
- ✅ Dead Letter Queue handling
- ✅ Idempotency verification
- ✅ High-volume processing

### **End-to-End Tests**
- ✅ Complete payment flow
- ✅ Validation failure handling
- ✅ Service recovery scenarios
- ✅ Different message types
- ✅ Performance benchmarks

## 🔧 **Test Configuration**

### **Playwright Config**
- **Browsers**: Chrome, Firefox, Safari
- **Parallel Execution**: Enabled
- **Retries**: 2 (CI), 0 (local)
- **Screenshots**: On failure
- **Videos**: Retain on failure
- **Traces**: On first retry

### **Web Server**
- **Command**: `mvn spring-boot:run`
- **URL**: Health check endpoint
- **Timeout**: 120 seconds
- **Reuse**: Enabled (local only)

## 📊 **Test Data**

### **Sample Messages**
The test suite includes comprehensive test data:
- **Valid Payment Messages**: Standard payment structures
- **Invalid Messages**: Missing fields, validation errors
- **Edge Cases**: High values, international payments
- **Error Scenarios**: Network failures, service restarts

### **Avro Schema Validation**
Tests verify:
- Input message structure compliance
- Response message with Trailer object
- ServiceStatus validation results
- Complete data preservation

## 🧪 **Running Specific Tests**

### **Test by Name**
```bash
npx playwright test -g "should process valid payment message"
```

### **Test by File**
```bash
npx playwright test health-controller.spec.ts
```

### **Test with Tags**
```bash
npx playwright test --grep @smoke
npx playwright test --grep @integration
```

## 📈 **Test Reports**

### **HTML Report**
```bash
npm run report
```

### **JUnit XML**
```bash
# Reports saved to test-results/results.xml
```

### **JSON Report**
```bash
# Reports saved to test-results/results.json
```

## 🔍 **Debugging Tests**

### **Debug Mode**
```bash
npm run test:debug
```

### **Code Generation**
```bash
npm run codegen
```

### **Trace Viewer**
```bash
npx playwright show-trace trace.zip
```

## 🐳 **Docker Integration**

### **Prerequisites**
- Kafka cluster running
- Fast Inward Clearing Processor service running
- Network connectivity between containers

### **Environment Variables**
```bash
export KAFKA_BROKERS=kafka:9092
export BASE_URL=http://service:8080
```

## 📝 **Adding New Tests**

### **1. Create Test File**
```typescript
import { test, expect } from '@playwright/test';

test.describe('New Feature Tests', () => {
  test('should test new functionality', async () => {
    // Test implementation
  });
});
```

### **2. Use Helper Functions**
```typescript
import { kafkaHelper, httpHelper, testData } from '../helpers/';

// Use existing test data and utilities
```

### **3. Follow Naming Convention**
- **File**: `feature-name.spec.ts`
- **Test**: `should [expected behavior]`
- **Describe**: Feature or component name

## 🚨 **Troubleshooting**

### **Common Issues**

#### **Service Not Starting**
```bash
# Check if service is running
curl http://localhost:8080/api/v1/health/status

# Check logs
cd ../services/fast-inward-clearing-processor
mvn spring-boot:run
```

#### **Kafka Connection Issues**
```bash
# Verify Kafka is running
docker ps | grep kafka

# Check topic existence
kafka-topics --list --bootstrap-server localhost:9092
```

#### **Test Timeouts**
```bash
# Increase timeout in .env
TEST_TIMEOUT=60000

# Check service performance
# Monitor Kafka lag
# Verify network connectivity
```

### **Debug Commands**
```bash
# Run single test with verbose output
npx playwright test --debug --reporter=verbose

# Check test environment
npx playwright test --reporter=list
```

## 📚 **Additional Resources**

- [Playwright Documentation](https://playwright.dev/)
- [Kafka Testing Guide](https://kafka.js.org/docs/testing)
- [Avro Schema Reference](https://avro.apache.org/docs/current/spec.html)

## 🤝 **Contributing**

1. Follow existing test patterns
2. Add comprehensive test coverage
3. Include edge cases and error scenarios
4. Update documentation for new tests
5. Ensure all tests pass before committing

## 📄 **License**

This test suite is part of the ClearPath Gateway project and follows the same licensing terms.
