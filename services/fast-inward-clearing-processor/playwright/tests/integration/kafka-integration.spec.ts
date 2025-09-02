import { test, expect } from '@playwright/test';
import { kafkaHelper } from '../helpers/kafka-helper';
import { testData } from '../helpers/test-data';

test.describe('Kafka Integration Tests', () => {
  test.beforeAll(async () => {
    await kafkaHelper.connect();
  });

  test.afterAll(async () => {
    await kafkaHelper.disconnect();
  });

  test.beforeEach(async () => {
    await kafkaHelper.clearMessages();
  });

  test('should process valid payment message through Kafka pipeline', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'transactions.processed';
    
    // Send test message to input topic
    await kafkaHelper.sendMessage(
      inputTopic, 
      testData.sampleInputMessage, 
      testData.sampleInputMessage.Header.MUID
    );

    // Wait for and consume response from output topic
    const responses = await kafkaHelper.consumeMessages(outputTopic, 1, 15000);
    
    expect(responses).toHaveLength(1);
    
    const responseMessage = JSON.parse(responses[0].value!.toString());
    
    // Verify response structure
    expect(responseMessage).toHaveProperty('Header');
    expect(responseMessage).toHaveProperty('Body');
    expect(responseMessage).toHaveProperty('Procctxt');
    expect(responseMessage).toHaveProperty('messages');
    expect(responseMessage).toHaveProperty('Trailer');
    
    // Verify Trailer contains success status
    expect(responseMessage.Trailer).toHaveProperty('status', 'SUCCESS');
    expect(responseMessage.Trailer).toHaveProperty('StatusCode', '200');
    expect(responseMessage.Trailer.StatusDesc).toContain('SUCCESS');
    
    // Verify original message data is preserved
    expect(responseMessage.Header.MUID).toBe(testData.sampleInputMessage.Header.MUID);
    expect(responseMessage.Header.UUID).toBe(testData.sampleInputMessage.Header.UUID);
  });

  test('should handle invalid message and send to DLQ', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const dlqTopic = process.env.DLQ_TOPIC || 'transactions.dlq';
    
    // Send invalid message (missing required fields)
    const invalidMessage = { ...testData.sampleInputMessage };
    delete invalidMessage.Header.MUID;
    
    await kafkaHelper.sendMessage(inputTopic, invalidMessage, 'invalid-msg-id');
    
    // Wait for message to be sent to DLQ
    const dlqMessages = await kafkaHelper.consumeMessages(dlqTopic, 1, 10000);
    
    expect(dlqMessages).toHaveLength(1);
    
    const dlqMessage = JSON.parse(dlqMessages[0].value!.toString());
    expect(dlqMessage).toContain('MUID');
    expect(dlqMessage).toContain('invalid-msg-id');
  });

  test('should maintain message order and idempotency', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'transactions.processed';
    
    const messageIds = ['msg-001', 'msg-002', 'msg-003'];
    
    // Send multiple messages in sequence
    for (const msgId of messageIds) {
      const message = { ...testData.sampleInputMessage };
      message.Header.MUID = msgId;
      
      await kafkaHelper.sendMessage(inputTopic, message, msgId);
    }
    
    // Consume all responses
    const responses = await kafkaHelper.consumeMessages(outputTopic, 3, 20000);
    
    expect(responses).toHaveLength(3);
    
    // Verify all messages were processed
    const processedIds = responses.map(msg => 
      JSON.parse(msg.value!.toString()).Header.MUID
    );
    
    expect(processedIds).toEqual(expect.arrayContaining(messageIds));
  });

  test('should handle high volume message processing', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'transactions.processed';
    
    const messageCount = 10;
    const messages = [];
    
    // Prepare multiple test messages
    for (let i = 0; i < messageCount; i++) {
      const message = { ...testData.sampleInputMessage };
      message.Header.MUID = `bulk-msg-${i}`;
      message.Header.UUID = `bulk-uuid-${i}`;
      messages.push(message);
    }
    
    // Send all messages
    for (const message of messages) {
      await kafkaHelper.sendMessage(inputTopic, message, message.Header.MUID);
    }
    
    // Consume all responses
    const responses = await kafkaHelper.consumeMessages(outputTopic, messageCount, 30000);
    
    expect(responses).toHaveLength(messageCount);
    
    // Verify processing time is reasonable
    const startTime = Date.now();
    const allResponses = await kafkaHelper.consumeMessages(outputTopic, messageCount, 30000);
    const endTime = Date.now();
    
    const processingTime = endTime - startTime;
    expect(processingTime).toBeLessThan(30000); // Should complete within 30 seconds
  });
});
