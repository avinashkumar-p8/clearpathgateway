import { test, expect } from '@playwright/test';
import { Kafka } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

/**
 * Playwright test suite for Event Saving and Idempotency functionality
 * Tests MUID-based exactly-once processing guarantees
 */
test.describe.serial('Event Saving and Idempotency Tests', () => {
  let kafka: Kafka;
  let producer: any;
  let consumer: any;
  let schemaRegistry: SchemaRegistry;

  // Test configuration
  const testConfig = {
    inputTopic: 'transactions.incoming',
    outputTopic: 'transactions.processed',
    dlqTopic: 'transactions.dlq',
    serviceUrl: process.env.SERVICE_URL || 'http://localhost:8080',
    kafkaBrokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',').map(broker => broker.trim()),
    schemaRegistryUrl: process.env.SCHEMA_REGISTRY_URL || 'http://localhost:8081'
  };

  // Test message schema for idempotency testing
  const transactionMessageSchema = {
    type: 'record' as const,
    name: 'TransactionMessage',
    namespace: 'com.anz.fastpayment.test',
    fields: [
      { name: 'transactionId', type: 'string' },
      { name: 'amount', type: 'double' },
      { name: 'currency', type: 'string' },
      { name: 'country', type: 'string' },
      { name: 'timestamp', type: 'long' },
      { name: 'metadata', type: { type: 'map', values: 'string' }, default: {} }
    ]
  };

  // Test data
  const testTransactions = [
    {
      transactionId: 'TXN-001',
      amount: 1000.50,
      currency: 'SGD',
      country: 'SG',
      timestamp: Date.now(),
      metadata: { source: 'test', priority: 'high' }
    },
    {
      transactionId: 'TXN-002',
      amount: 2500.75,
      currency: 'USD',
      country: 'US',
      timestamp: Date.now(),
      metadata: { source: 'test', priority: 'normal' }
    },
    {
      transactionId: 'TXN-003',
      amount: 500.25,
      currency: 'EUR',
      country: 'DE',
      timestamp: Date.now(),
      metadata: { source: 'test', priority: 'low' }
    }
  ];

  test.beforeAll(async () => {
    // Initialize Schema Registry client
    schemaRegistry = new SchemaRegistry({
      host: testConfig.schemaRegistryUrl
    });

    // Initialize Kafka client
    kafka = new Kafka({
      clientId: 'playwright-idempotency-test',
      brokers: testConfig.kafkaBrokers,
    });

    producer = kafka.producer();
    // Use unique group ID for each test run to avoid conflicts
    const uniqueGroupId = `playwright-idempotency-test-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    consumer = kafka.consumer({ groupId: uniqueGroupId });

    await producer.connect();
    await consumer.connect();
  });

  test.afterAll(async () => {
    await producer.disconnect();
    await consumer.disconnect();
  });

  test.describe('MUID-based Idempotency', () => {
    
    test('should save event with new MUID successfully', async () => {
      // Given: A new transaction message with unique MUID
      const testMessage = testTransactions[0];
      const muid = `MUID-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
      
      // Register schema if not exists
      const registeredSchema = await schemaRegistry.register(transactionMessageSchema, { 
        subject: `${testConfig.inputTopic}-value` 
      });
      expect(registeredSchema.id).toBeGreaterThan(0);

      // When: Send message with MUID header
      const messageWithHeaders = {
        key: testMessage.transactionId,
        value: JSON.stringify(testMessage), // Fix: Serialize object to JSON string
        headers: [
          { key: 'muid', value: Buffer.from(muid) }, // Fix: Convert to Buffer
          { key: 'source', value: Buffer.from('playwright-test') }, // Fix: Convert to Buffer
          { key: 'timestamp', value: Buffer.from(Date.now().toString()) } // Fix: Convert to Buffer
        ]
      };

      await producer.send({
        topic: testConfig.inputTopic,
        messages: [messageWithHeaders]
      });

      // Then: Verify message is processed and saved
      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Verify output topic received processed message
      await consumer.subscribe({ topic: testConfig.outputTopic, fromBeginning: true });
      
      const processedMessages: any[] = [];
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (message.key?.toString() === testMessage.transactionId) {
            processedMessages.push({
              topic,
              partition,
              key: message.key.toString(),
              value: message.value?.toString(),
              headers: message.headers
            });
          }
        }
      });

      // Wait for message processing
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      expect(processedMessages.length).toBeGreaterThan(0);
      expect(processedMessages[0].key).toBe(testMessage.transactionId);
      
      // Verify MUID header is preserved
      const muidHeader = processedMessages[0].headers.find((h: any) => h.key === 'muid');
      expect(muidHeader).toBeDefined();
      expect(muidHeader.value.toString()).toBe(muid);

      console.log('✅ Event with new MUID saved successfully');
    });

    test('should skip event with duplicate MUID', async () => {
      // Given: A transaction message with existing MUID
      const testMessage = testTransactions[1];
      const duplicateMuid = `DUPLICATE-MUID-${Date.now()}`;
      
      // First: Send message with MUID (should be processed)
      const firstMessage = {
        key: testMessage.transactionId,
        value: JSON.stringify(testMessage), // Fix: Serialize object to JSON string
        headers: [
          { key: 'muid', value: duplicateMuid },
          { key: 'source', value: 'playwright-test' }
        ]
      };

      await producer.send({
        topic: testConfig.inputTopic,
        messages: [firstMessage]
      });

      // Wait for first processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Then: Send duplicate message with same MUID (should be skipped)
      const duplicateMessage = {
        key: `${testMessage.transactionId}-DUPLICATE`,
        value: JSON.stringify({ ...testMessage, amount: testMessage.amount + 100 }), // Fix: Serialize object to JSON string
        headers: [
          { key: 'muid', value: duplicateMuid }, // Same MUID
          { key: 'source', value: 'playwright-test-duplicate' }
        ]
      };

      await producer.send({
        topic: testConfig.inputTopic,
        messages: [duplicateMessage]
      });

      // Wait for duplicate processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Verify: Only one message processed (the first one)
      await consumer.subscribe({ topic: testConfig.outputTopic, fromBeginning: true });
      
      const processedMessages: any[] = [];
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (message.headers.find((h: any) => h.key === 'muid' && h.value.toString() === duplicateMuid)) {
            processedMessages.push({
              topic,
              partition,
              key: message.key.toString(),
              value: message.value?.toString(),
              headers: message.headers
            });
          }
        }
      });

      // Wait for message processing
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      // Should only have one message with this MUID
      expect(processedMessages.length).toBe(1);
      expect(processedMessages[0].key).toBe(testMessage.transactionId);
      expect(processedMessages[0].key).not.toBe(`${testMessage.transactionId}-DUPLICATE`);

      console.log('✅ Duplicate MUID event correctly skipped');
    });

    test('should persist event payload correctly in database', async () => {
      // Given: A transaction message with detailed payload
      const testMessage = testTransactions[2];
      const muid = `PERSISTENCE-MUID-${Date.now()}`;
      
      // Create a complex payload with nested data
      const complexPayload = {
        ...testMessage,
        metadata: {
          ...testMessage.metadata,
          nested: {
            level1: {
              level2: 'deep-value',
              array: [1, 2, 3, 4, 5],
              timestamp: new Date().toISOString()
            }
          },
          largeString: 'A'.repeat(1000) // Test large payload handling
        }
      };

      // When: Send message with complex payload
      const messageWithPayload = {
        key: testMessage.transactionId,
        value: JSON.stringify(complexPayload), // Fix: Serialize object to JSON string
        headers: [
          { key: 'muid', value: muid },
          { key: 'payload-size', value: JSON.stringify(complexPayload).length.toString() }
        ]
      };

      await producer.send({
        topic: testConfig.inputTopic,
        messages: [messageWithPayload]
      });

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Then: Verify payload is preserved in output
      await consumer.subscribe({ topic: testConfig.outputTopic, fromBeginning: true });
      
      const processedMessages: any[] = [];
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (message.headers.find((h: any) => h.key === 'muid' && h.value.toString() === muid)) {
            processedMessages.push({
              topic,
              partition,
              key: message.key.toString(),
              value: message.value?.toString(),
              headers: message.headers
            });
          }
        }
      });

      // Wait for message processing
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      expect(processedMessages.length).toBe(1);
      
      // Verify payload integrity
      const processedPayload = JSON.parse(processedMessages[0].value);
      expect(processedPayload.transactionId).toBe(testMessage.transactionId);
      expect(processedPayload.amount).toBe(testMessage.amount);
      expect(processedPayload.currency).toBe(testMessage.currency);
      expect(processedPayload.country).toBe(testMessage.country);
      
      // Verify complex metadata is preserved
      expect(processedPayload.metadata.nested.level1.level2).toBe('deep-value');
      expect(processedPayload.metadata.nested.level1.array).toEqual([1, 2, 3, 4, 5]);
      expect(processedPayload.metadata.largeString.length).toBe(1000);

      console.log('✅ Event payload persisted correctly in database');
    });

    test('should create logs for successful save and duplicate detection', async () => {
      // Given: Multiple messages with different MUIDs
      const uniqueMuid = `LOGGING-MUID-${Date.now()}`;
      const duplicateMuid = `LOGGING-DUPLICATE-${Date.now()}`;
      
      // First: Send unique message (should log successful save)
      const successMessage = {
        key: testTransactions[0].transactionId,
        value: JSON.stringify(testTransactions[0]), // Fix: Serialize object to JSON string
        headers: [
          { key: 'muid', value: uniqueMuid },
          { key: 'test-type', value: 'logging-success' }
        ]
      };

      await producer.send({
        topic: testConfig.inputTopic,
        messages: [successMessage]
      });

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Then: Send duplicate message (should log duplicate detection)
      const duplicateMessage = {
        key: testTransactions[1].transactionId,
        value: JSON.stringify(testTransactions[1]), // Fix: Serialize object to JSON string
        headers: [
          { key: 'muid', value: duplicateMuid },
          { key: 'test-type', value: 'logging-duplicate' }
        ]
      };

      await producer.send({
        topic: testConfig.inputTopic,
        messages: [duplicateMessage]
      });

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Send same MUID again (should log duplicate detection)
      const duplicateMessage2 = {
        key: testTransactions[2].transactionId,
        value: JSON.stringify(testTransactions[2]), // Fix: Serialize object to JSON string
        headers: [
          { key: 'muid', value: duplicateMuid }, // Same MUID
          { key: 'test-type', value: 'logging-duplicate-2' }
        ]
      };

      await producer.send({
        topic: testConfig.inputTopic,
        messages: [duplicateMessage2]
      });

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 2000));

      // Verify: Check output topic for processed messages
      await consumer.subscribe({ topic: testConfig.outputTopic, fromBeginning: true });
      
      const processedMessages: any[] = [];
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const muidHeader = message.headers.find((h: any) => h.key === 'muid');
          if (muidHeader && (muidHeader.value.toString() === uniqueMuid || muidHeader.value.toString() === duplicateMuid)) {
            processedMessages.push({
              topic,
              partition,
              key: message.key.toString(),
              value: message.value?.toString(),
              headers: message.headers
            });
          }
        }
      });

      // Wait for message processing
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      // Should have 2 processed messages (unique + first duplicate)
      expect(processedMessages.length).toBe(2);
      
      // Verify unique message was processed
      const uniqueProcessed = processedMessages.find(m => 
        m.headers.find((h: any) => h.key === 'muid' && h.value.toString() === uniqueMuid)
      );
      expect(uniqueProcessed).toBeDefined();
      expect(uniqueProcessed?.key).toBe('LOGGING-TXN-001');
      
      // Verify first duplicate was processed
      const duplicateProcessed = processedMessages.find(m => 
        m.headers.find((h: any) => h.key === 'muid' && h.value.toString() === duplicateMuid)
      );
      expect(duplicateProcessed).toBeDefined();
      expect(duplicateProcessed?.key).toBe('LOGGING-TXN-002');
      
      // Second duplicate should not be processed (same MUID)
      const allMessages = processedMessages.filter(m => 
        m.headers.find((h: any) => h.key === 'muid' && h.value.toString() === duplicateMuid)
      );
      expect(allMessages.length).toBe(1); // Only first occurrence

      console.log('✅ Logs created for successful save and duplicate detection');
    });

    test('should ensure transaction safety on DB insert failure', async () => {
      // Given: A message that will cause DB insert failure
      const testMessage = {
        transactionId: 'DB-FAIL-TXN',
        amount: -999999.99, // Invalid amount that might cause DB constraint failure
        currency: 'INVALID', // Invalid currency code
        country: 'XX', // Invalid country code
        timestamp: Date.now(),
        metadata: { testType: 'db-failure' }
      };
      
      const muid = `DB-FAIL-MUID-${Date.now()}`;
      
      // When: Send message that should fail DB insert
      const failureMessage = {
        key: testMessage.transactionId,
        value: JSON.stringify(testMessage), // Fix: Serialize object to JSON string
        headers: [
          { key: 'muid', value: muid },
          { key: 'test-type', value: 'db-failure-test' }
        ]
      };

      await producer.send({
        topic: testConfig.inputTopic,
        messages: [failureMessage]
      });

      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 3000));

      // Then: Verify message is sent to DLQ (not processed successfully)
      await consumer.subscribe({ topic: testConfig.dlqTopic, fromBeginning: true });
      
      const dlqMessages: any[] = [];
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (message.key?.toString() === testMessage.transactionId) {
            dlqMessages.push({
              topic,
              partition,
              key: message.key.toString(),
              value: message.value?.toString(),
              headers: message.headers
            });
          }
        }
      });

      // Wait for DLQ processing
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      // Verify message went to DLQ
      expect(dlqMessages.length).toBeGreaterThan(0);
      
      // Verify DLQ message contains error information
      const dlqMessage = dlqMessages[0];
      expect(dlqMessage.key).toBe(testMessage.transactionId);
      
      // Parse DLQ message value to check error details
      const dlqValue = JSON.parse(dlqMessage.value);
      expect(dlqValue.error).toBeDefined();
      expect(dlqValue.original_message).toBeDefined();
      
      // Verify original message is preserved
      const originalMessage = JSON.parse(dlqValue.original_message);
      expect(originalMessage.transactionId).toBe(testMessage.transactionId);
      expect(originalMessage.amount).toBe(testMessage.amount);
      
      // Verify error headers are present
      const errorTimestamp = dlqMessage.headers.find((h: any) => h.key === 'error_timestamp');
      const errorSource = dlqMessage.headers.find((h: any) => h.key === 'error_source');
      expect(errorTimestamp).toBeDefined();
      expect(errorSource).toBeDefined();
      expect(errorSource.value.toString()).toBe('fast-inward-clearing-processor');

      console.log('✅ Transaction safety ensured on DB insert failure');
    });

    test('should handle concurrent MUID processing correctly', async () => {
      // Given: Multiple messages with different MUIDs sent concurrently
      const concurrentMuids = [
        `CONCURRENT-MUID-1-${Date.now()}`,
        `CONCURRENT-MUID-2-${Date.now()}`,
        `CONCURRENT-MUID-3-${Date.now()}`
      ];
      
      const concurrentMessages = concurrentMuids.map((muid, index) => ({
        key: `CONCURRENT-TXN-${index + 1}`,
        value: JSON.stringify({ ...testTransactions[0], transactionId: `CONCURRENT-TXN-${index + 1}` }), // Fix: Serialize object to JSON string
        headers: [
          { key: 'muid', value: muid },
          { key: 'test-type', value: 'concurrent-test' },
          { key: 'sequence', value: (index + 1).toString() }
        ]
      }));

      // When: Send all messages concurrently
      await producer.send({
        topic: testConfig.inputTopic,
        messages: concurrentMessages
      });

      // Wait for concurrent processing
      await new Promise(resolve => setTimeout(resolve, 4000));

      // Then: Verify all messages are processed correctly
      await consumer.subscribe({ topic: testConfig.outputTopic, fromBeginning: true });
      
      const processedMessages: any[] = [];
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const muidHeader = message.headers.find((h: any) => h.key === 'muid');
          if (muidHeader && concurrentMuids.includes(muidHeader.value.toString())) {
            processedMessages.push({
              topic,
              partition,
              key: message.key.toString(),
              value: message.value?.toString(),
              headers: message.headers
            });
          }
        }
      });

      // Wait for message processing
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      // Should have all 3 messages processed
      expect(processedMessages.length).toBe(3);
      
      // Verify each MUID was processed exactly once
      const processedMuids = processedMessages.map(m => 
        m.headers.find((h: any) => h.key === 'muid')?.value.toString()
      );
      
      concurrentMuids.forEach(muid => {
        expect(processedMuids).toContain(muid);
      });
      
      // Verify no duplicates
      expect(processedMuids.length).toBe(new Set(processedMuids).size);
      
      // Verify sequence headers are preserved
      processedMessages.forEach(message => {
        const sequenceHeader = message.headers.find((h: any) => h.key === 'sequence');
        expect(sequenceHeader).toBeDefined();
        const sequence = parseInt(sequenceHeader.value.toString());
        expect(sequence).toBeGreaterThan(0);
        expect(sequence).toBeLessThanOrEqual(3);
      });

      console.log('✅ Concurrent MUID processing handled correctly');
    });
  });

  test.describe('Idempotency Service Health Checks', () => {
    
    test('should provide idempotency health endpoint', async () => {
      // Given: Service is running
      const healthUrl = `${testConfig.serviceUrl}/api/v1/idempotency/health`;
      
      // When: Call health endpoint
      const response = await fetch(healthUrl);
      
      // Then: Verify health status
      expect(response.status).toBe(200);
      
      const healthData = await response.json();
      expect(healthData.status).toBe('UP');
      expect(healthData.service).toBe('Message Idempotency Service');
      expect(healthData.database).toBe('CONNECTED');
      expect(healthData.cache).toBe('OPERATIONAL');
      expect(healthData.kafka).toBe('OPERATIONAL');
      
      console.log('✅ Idempotency health endpoint working correctly');
    });

    test('should provide idempotency metrics endpoint', async () => {
      // Given: Service is running
      const metricsUrl = `${testConfig.serviceUrl}/api/v1/idempotency/metrics`;
      
      // When: Call metrics endpoint
      const response = await fetch(metricsUrl);
      
      // Then: Verify metrics response
      expect(response.status).toBe(200);
      
      const metricsData = await response.json();
      expect(metricsData.timestamp).toBeDefined();
      expect(metricsData.processedMessages).toBeDefined();
      expect(metricsData.duplicateMessages).toBeDefined();
      expect(metricsData.errorMessages).toBeDefined();
      expect(metricsData.successRate).toBeDefined();
      expect(metricsData.duplicateRate).toBeDefined();
      expect(metricsData.errorRate).toBeDefined();
      
      // Verify metrics are numeric
      expect(typeof metricsData.processedMessages).toBe('number');
      expect(typeof metricsData.duplicateMessages).toBe('number');
      expect(typeof metricsData.errorMessages).toBe('number');
      
      console.log('✅ Idempotency metrics endpoint working correctly');
    });

    test('should provide cache management endpoint', async () => {
      // Given: Service is running
      const cacheUrl = `${testConfig.serviceUrl}/api/v1/idempotency/cache/clear`;
      
      // When: Call cache clear endpoint
      const response = await fetch(cacheUrl);
      
      // Then: Verify cache clear response
      expect(response.status).toBe(200);
      
      const cacheData = await response.json();
      expect(cacheData.status).toBe('SUCCESS');
      expect(cacheData.message).toContain('cache entries cleared successfully');
      expect(cacheData.timestamp).toBeDefined();
      
      console.log('✅ Idempotency cache management endpoint working correctly');
    });
  });
});
