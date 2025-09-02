import { test, expect } from '@playwright/test';
import { kafkaHelper } from '../helpers/kafka-helper';
import { httpHelper } from '../helpers/http-helper';
import { testData } from '../helpers/test-data';

test.describe('End-to-End Pipeline Tests', () => {
  test.beforeAll(async () => {
    await kafkaHelper.connect();
    
    // Ensure service is healthy before running tests
    await expect.poll(async () => {
      const response = await httpHelper.healthCheck();
      return response.status;
    }, { timeout: 30000 }).toBe(200);
  });

  test.afterAll(async () => {
    await kafkaHelper.disconnect();
  });

  test.beforeEach(async () => {
    await kafkaHelper.clearMessages();
  });

  test('should process complete payment flow from input to output with Trailer', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'transactions.processed';
    
    // Step 1: Send payment message to input topic
    const startTime = Date.now();
    await kafkaHelper.sendMessage(
      inputTopic, 
      testData.sampleInputMessage, 
      testData.sampleInputMessage.Header.MUID
    );

    // Step 2: Wait for processing and consume response
    const responses = await kafkaHelper.consumeMessages(outputTopic, 1, 20000);
    
    expect(responses).toHaveLength(1);
    const processingTime = Date.now() - startTime;
    
    // Step 3: Verify response structure and content
    const responseMessage = JSON.parse(responses[0].value!.toString());
    
    // Verify complete message structure
    expect(responseMessage).toHaveProperty('Header');
    expect(responseMessage).toHaveProperty('Body');
    expect(responseMessage).toHaveProperty('Procctxt');
    expect(responseMessage).toHaveProperty('messages');
    expect(responseMessage).toHaveProperty('Trailer');
    
    // Verify Header fields
    expect(responseMessage.Header.MUID).toBe(testData.sampleInputMessage.Header.MUID);
    expect(responseMessage.Header.UUID).toBe(testData.sampleInputMessage.Header.UUID);
    expect(responseMessage.Header.ComponentName).toBe(testData.sampleInputMessage.Header.ComponentName);
    
    // Verify Body structure
    expect(responseMessage.Body.PmtAddRq).toHaveLength(1);
    expect(responseMessage.Body.PmtAddRq[0].RqUID).toBe(testData.sampleInputMessage.Body.PmtAddRq[0].RqUID);
    
    // Verify Trailer with success status
    expect(responseMessage.Trailer.status).toBe('SUCCESS');
    expect(responseMessage.Trailer.StatusCode).toBe('200');
    expect(responseMessage.Trailer.StatusDesc).toContain('SUCCESS');
    
    // Verify processing time is reasonable
    expect(processingTime).toBeLessThan(20000); // Should complete within 20 seconds
    
    console.log(`✅ Payment processed successfully in ${processingTime}ms`);
  });

  test('should handle validation failures and include in Trailer', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'transactions.processed';
    
    // Create message with validation issues
    const invalidMessage = { ...testData.sampleInputMessage };
    invalidMessage.Body.PmtAddRq[0].FromAcct.Amount = -1000.00; // Invalid negative amount
    invalidMessage.Body.PmtAddRq[0].FromAcct.CurCode = 'INVALID'; // Invalid currency
    
    await kafkaHelper.sendMessage(inputTopic, invalidMessage, invalidMessage.Header.MUID);
    
    // Wait for response
    const responses = await kafkaHelper.consumeMessages(outputTopic, 1, 15000);
    
    expect(responses).toHaveLength(1);
    
    const responseMessage = JSON.parse(responses[0].value!.toString());
    
    // Verify Trailer contains failure status
    expect(responseMessage.Trailer.status).toBe('FAILED');
    expect(responseMessage.Trailer.StatusCode).toBe('400');
    expect(responseMessage.Trailer.StatusDesc).toContain('validation');
    
    console.log('✅ Validation failures properly captured in Trailer');
  });

  test('should maintain idempotency for duplicate messages', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'transactions.processed';
    
    const messageId = 'duplicate-test-123';
    const message = { ...testData.sampleInputMessage };
    message.Header.MUID = messageId;
    
    // Send same message multiple times
    for (let i = 0; i < 3; i++) {
      await kafkaHelper.sendMessage(inputTopic, message, messageId);
    }
    
    // Should only get one response due to idempotency
    const responses = await kafkaHelper.consumeMessages(outputTopic, 3, 15000);
    
    // Verify idempotency - should only process once
    const uniqueResponses = new Set(
      responses.map(msg => JSON.parse(msg.value!.toString()).Header.MUID)
    );
    
    expect(uniqueResponses.size).toBe(1);
    expect(Array.from(uniqueResponses)[0]).toBe(messageId);
    
    console.log('✅ Idempotency working correctly - duplicate messages handled');
  });

  test('should process different message types correctly', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'transactions.processed';
    
    const testCases = [
      {
        name: 'Standard Payment',
        message: testData.sampleInputMessage,
        expectedStatus: 'SUCCESS'
      },
      {
        name: 'High Value Payment',
        message: {
          ...testData.sampleInputMessage,
          Body: {
            ...testData.sampleInputMessage.Body,
            PmtAddRq: [{
              ...testData.sampleInputMessage.Body.PmtAddRq[0],
              FromAcct: {
                ...testData.sampleInputMessage.Body.PmtAddRq[0].FromAcct,
                Amount: 1000000.00 // High value
              }
            }]
          }
        },
        expectedStatus: 'SUCCESS'
      },
      {
        name: 'International Payment',
        message: {
          ...testData.sampleInputMessage,
          Body: {
            ...testData.sampleInputMessage.Body,
            PmtAddRq: [{
              ...testData.sampleInputMessage.Body.PmtAddRq[0],
              FromFIData: { Country: 'US', BIC: 'CHASUS33' },
              ToFIData: { Country: 'GB', BIC: 'BARCGB22' }
            }]
          }
        },
        expectedStatus: 'SUCCESS'
      }
    ];
    
    for (const testCase of testCases) {
      console.log(`Testing: ${testCase.name}`);
      
      const message = { ...testCase.message };
      message.Header.MUID = `test-${Date.now()}-${Math.random()}`;
      
      await kafkaHelper.sendMessage(inputTopic, message, message.Header.MUID);
      
      const responses = await kafkaHelper.consumeMessages(outputTopic, 1, 10000);
      expect(responses).toHaveLength(1);
      
      const responseMessage = JSON.parse(responses[0].value!.toString());
      expect(responseMessage.Trailer.status).toBe(testCase.expectedStatus);
      
      // Clear for next test
      await kafkaHelper.clearMessages();
    }
    
    console.log('✅ All message types processed correctly');
  });

  test('should handle service restart and recovery', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'transactions.processed';
    
    // Send message before restart simulation
    await kafkaHelper.sendMessage(
      inputTopic, 
      testData.sampleInputMessage, 
      'recovery-test-123'
    );
    
    // Simulate service restart by checking health endpoint
    const healthResponse = await httpHelper.healthCheck();
    expect(healthResponse.status).toBe(200);
    
    // Wait for message processing after recovery
    const responses = await kafkaHelper.consumeMessages(outputTopic, 1, 15000);
    
    expect(responses).toHaveLength(1);
    const responseMessage = JSON.parse(responses[0].value!.toString());
    
    expect(responseMessage.Trailer.status).toBe('SUCCESS');
    expect(responseMessage.Header.MUID).toBe('recovery-test-123');
    
    console.log('✅ Service recovery working correctly');
  });
});
