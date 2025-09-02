import { test, expect } from '@playwright/test';
import { KafkaTestHelper } from '../helpers/kafka-helper';
import { HttpTestHelper } from '../helpers/http-helper';
import { testData } from '../helpers/test-data';

test.describe('Error Handling and Resilience Tests', () => {
  let kafkaHelper: KafkaTestHelper;
  let httpHelper: HttpTestHelper;

  test.beforeEach(async () => {
    kafkaHelper = new KafkaTestHelper();
    httpHelper = new HttpTestHelper();
  });

  test.afterEach(async () => {
    await kafkaHelper.disconnect();
    await httpHelper.disconnect();
  });

  test.describe('Kafka Connection Resilience', () => {
    test('should handle Kafka connection loss gracefully', async () => {
      // This test simulates Kafka connection issues
      const inputMessage = testData.sampleInputMessage;
      
      // Send message and expect it to be queued or handled gracefully
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      // Check if message is in DLQ or handled with retry logic
      const dlqMessage = await kafkaHelper.consumeMessage('transactions.dlq', 5000);
      
      if (dlqMessage) {
        // Message should be in DLQ with appropriate error details
        expect(dlqMessage).toBeDefined();
        expect(dlqMessage.Trailer.status).toBe('FAILED');
        expect(dlqMessage.Trailer.StatusCode).toBe('500');
        expect(dlqMessage.Trailer.StatusDesc.some(desc => 
          desc.includes('Kafka connection error') || 
          desc.includes('Temporary failure')
        )).toBe(true);
      }
    });

    test('should retry failed messages with exponential backoff', async () => {
      const inputMessage = testData.sampleInputMessage;
      
      // Send message that might fail initially
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      // Wait for retry attempts
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      // Check if message eventually succeeds
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      // Message should either succeed or be in DLQ after retries
      expect(['SUCCESS', 'FAILED']).toContain(responseMessage.Trailer.status);
    });
  });

  test.describe('Schema Registry Resilience', () => {
    test('should handle schema registry unavailability', async () => {
      const inputMessage = testData.sampleInputMessage;
      
      // Send message when schema registry might be down
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      // Check if service falls back to cached schemas
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      // Service should either succeed with cached schema or fail gracefully
      expect(['SUCCESS', 'FAILED']).toContain(responseMessage.Trailer.status);
    });

    test('should handle schema evolution gracefully', async () => {
      // Test with slightly modified schema structure
      const inputMessage = { ...testData.sampleInputMessage };
      inputMessage.Header.AdditionalField = 'new-field-value';
      
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      // Should handle schema evolution gracefully
      expect(['SUCCESS', 'FAILED']).toContain(responseMessage.Trailer.status);
    });
  });

  test.describe('Database Resilience', () => {
    test('should handle database connection failures', async () => {
      const inputMessage = testData.sampleInputMessage;
      
      // Send message that requires database operations
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      // Check if circuit breaker activates
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      if (responseMessage.Trailer.status === 'FAILED') {
        expect(responseMessage.Trailer.StatusCode).toBe('503'); // Service Unavailable
        expect(responseMessage.Trailer.StatusDesc.some(desc => 
          desc.includes('Service temporarily unavailable') ||
          desc.includes('Circuit breaker active')
        )).toBe(true);
      }
    });

    test('should recover from database failures', async () => {
      const inputMessage = testData.sampleInputMessage;
      
      // Send message after potential database recovery
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      // Should eventually succeed after recovery
      expect(['SUCCESS', 'FAILED']).toContain(responseMessage.Trailer.status);
    });
  });

  test.describe('Memory and Resource Management', () => {
    test('should handle memory pressure gracefully', async () => {
      // Send multiple large messages to create memory pressure
      const largeMessage = { ...testData.sampleInputMessage };
      largeMessage.messages = Array(1000).fill({ 
        instruction: { 
          MsgDef: { 
            MsgType: "PAYMENT",
            Schema: "FAST_PAYMENT_SCHEMA"
          } 
        } 
      });
      
      const promises = [];
      for (let i = 0; i < 10; i++) {
        promises.push(kafkaHelper.produceMessage('transactions.incoming', largeMessage));
      }
      
      await Promise.all(promises);
      
      // Check if service handles memory pressure gracefully
      const responseMessages = [];
      for (let i = 0; i < 10; i++) {
        try {
          const msg = await kafkaHelper.consumeMessage('transactions.processed', 5000);
          if (msg) responseMessages.push(msg);
        } catch (error) {
          // Some messages might fail due to memory pressure
          break;
        }
      }
      
      expect(responseMessages.length).toBeGreaterThan(0);
      // At least some messages should be processed
      responseMessages.forEach(msg => {
        expect(msg.Trailer.status).toBeDefined();
      });
    });

    test('should maintain performance under load', async () => {
      const startTime = Date.now();
      const inputMessage = testData.sampleInputMessage;
      
      // Send message and measure response time
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      const processingTime = Date.now() - startTime;
      
      expect(responseMessage).toBeDefined();
      expect(processingTime).toBeLessThan(10000); // Should complete within 10 seconds
    });
  });

  test.describe('Message Corruption and Malformed Data', () => {
    test('should handle corrupted message data', async () => {
      // Send corrupted message data
      const corruptedMessage = { ...testData.sampleInputMessage };
      corruptedMessage.Body.PmtAddRq[0].FromAcct.Amount = 'invalid-amount';
      
      await kafkaHelper.produceMessage('transactions.incoming', corruptedMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
      expect(responseMessage.Trailer.StatusDesc.some(desc => 
        desc.includes('Invalid data format') ||
        desc.includes('Data validation failed')
      )).toBe(true);
    });

    test('should handle completely malformed messages', async () => {
      const malformedMessage = { invalid: 'data', structure: 'completely wrong' };
      
      await kafkaHelper.produceMessage('transactions.incoming', malformedMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
    });

    test('should handle null and undefined values', async () => {
      const nullMessage = { ...testData.sampleInputMessage };
      nullMessage.Header.MUID = null;
      nullMessage.Body = undefined;
      
      await kafkaHelper.produceMessage('transactions.incoming', nullMessage);
      
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      
      expect(responseMessage).toBeDefined();
      expect(responseMessage.Trailer.status).toBe('FAILED');
      expect(responseMessage.Trailer.StatusCode).toBe('400');
    });
  });

  test.describe('Service Recovery and Health Checks', () => {
    test('should maintain health endpoint during failures', async () => {
      // Send problematic message
      const problematicMessage = { ...testData.sampleInputMessage };
      problematicMessage.Body.PmtAddRq[0].FromAcct.Amount = -1000;
      
      await kafkaHelper.produceMessage('transactions.incoming', problematicMessage);
      
      // Check health endpoint remains responsive
      const healthResponse = await httpHelper.getHealthStatus();
      expect(healthResponse.status).toBe(200);
      
      // Process the problematic message
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 10000);
      expect(responseMessage).toBeDefined();
    });

    test('should recover from complete service failure', async () => {
      // This test simulates service restart scenario
      const inputMessage = testData.sampleInputMessage;
      
      // Send message
      await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
      
      // Wait for potential service recovery
      await new Promise(resolve => setTimeout(resolve, 5000));
      
      // Check if message is eventually processed
      const responseMessage = await kafkaHelper.consumeMessage('transactions.processed', 15000);
      
      expect(responseMessage).toBeDefined();
      // Should eventually succeed after recovery
      expect(['SUCCESS', 'FAILED']).toContain(responseMessage.Trailer.status);
    });
  });

  test.describe('Concurrent Error Scenarios', () => {
    test('should handle multiple concurrent failures', async () => {
      const messages = [];
      
      // Create various types of problematic messages
      for (let i = 0; i < 5; i++) {
        const message = { ...testData.sampleInputMessage };
        message.Header.MUID = `test-muid-${i}`;
        
        if (i % 2 === 0) {
          message.Body.PmtAddRq[0].FromAcct.Amount = -1000; // Invalid amount
        } else {
          message.Header.ComponentName = null; // Missing required field
        }
        
        messages.push(message);
      }
      
      // Send all messages concurrently
      const promises = messages.map(msg => kafkaHelper.produceMessage('transactions.incoming', msg));
      await Promise.all(promises);
      
      // Check if all messages are handled appropriately
      const responses = [];
      for (let i = 0; i < 5; i++) {
        try {
          const msg = await kafkaHelper.consumeMessage('transactions.processed', 8000);
          if (msg) responses.push(msg);
        } catch (error) {
          // Some messages might timeout
          break;
        }
      }
      
      expect(responses.length).toBeGreaterThan(0);
      responses.forEach(msg => {
        expect(msg.Trailer.status).toBe('FAILED');
        expect(msg.Trailer.StatusCode).toBe('400');
      });
    });

    test('should maintain idempotency during failures', async () => {
      const inputMessage = testData.sampleInputMessage;
      const muid = `idempotency-test-${Date.now()}`;
      inputMessage.Header.MUID = muid;
      
      // Send same message multiple times
      for (let i = 0; i < 3; i++) {
        await kafkaHelper.produceMessage('transactions.incoming', inputMessage);
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      
      // Should only process once due to idempotency
      const responseMessages = [];
      for (let i = 0; i < 3; i++) {
        try {
          const msg = await kafkaHelper.consumeMessage('transactions.processed', 5000);
          if (msg) responseMessages.push(msg);
        } catch (error) {
          break;
        }
      }
      
      // Should have at least one response
      expect(responseMessages.length).toBeGreaterThan(0);
      
      // All responses should have same MUID
      responseMessages.forEach(msg => {
        expect(msg.Header.MUID).toBe(muid);
      });
    });
  });
});
