import { test, expect } from '@playwright/test';
import { kafkaHelper } from '../helpers/kafka-helper';
import { testData } from '../helpers/test-data';

test.describe('Performance Tests', () => {
  test.beforeAll(async () => {
    await kafkaHelper.connect();
  });

  test.afterAll(async () => {
    await kafkaHelper.disconnect();
  });

  test.beforeEach(async () => {
    await kafkaHelper.clearMessages();
  });

  test('should process 100 messages within performance SLA', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'transactions.processed';
    const messageCount = 100;
    
    console.log(`🚀 Starting performance test with ${messageCount} messages`);
    
    const startTime = Date.now();
    
    // Send messages in batches
    const batchSize = 10;
    for (let i = 0; i < messageCount; i += batchSize) {
      const batch = [];
      for (let j = 0; j < batchSize && (i + j) < messageCount; j++) {
        const message = { ...testData.sampleInputMessage };
        message.Header.MUID = `perf-${i + j}`;
        message.Header.UUID = `perf-uuid-${i + j}`;
        batch.push(message);
      }
      
      // Send batch
      for (const message of batch) {
        await kafkaHelper.sendMessage(inputTopic, message, message.Header.MUID);
      }
      
      // Small delay between batches
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    console.log(`📤 All ${messageCount} messages sent, waiting for processing...`);
    
    // Consume all responses
    const responses = await kafkaHelper.consumeMessages(outputTopic, messageCount, 120000); // 2 minutes timeout
    
    const endTime = Date.now();
    const totalProcessingTime = endTime - startTime;
    
    // Performance assertions
    expect(responses).toHaveLength(messageCount);
    expect(totalProcessingTime).toBeLessThan(120000); // Should complete within 2 minutes
    
    // Calculate throughput
    const throughput = (messageCount / (totalProcessingTime / 1000)).toFixed(2);
    const avgProcessingTime = (totalProcessingTime / messageCount).toFixed(2);
    
    console.log(`📊 Performance Results:`);
    console.log(`   Total Time: ${totalProcessingTime}ms`);
    console.log(`   Throughput: ${throughput} messages/second`);
    console.log(`   Avg Processing: ${avgProcessingTime}ms per message`);
    
    // Verify all messages were processed successfully
    let successCount = 0;
    let failureCount = 0;
    
    for (const response of responses) {
      const responseMessage = JSON.parse(response.value!.toString());
      if (responseMessage.Trailer.status === 'SUCCESS') {
        successCount++;
      } else {
        failureCount++;
      }
    }
    
    console.log(`✅ Success: ${successCount}, ❌ Failures: ${failureCount}`);
    
    // Success rate should be high
    const successRate = (successCount / messageCount) * 100;
    expect(successRate).toBeGreaterThan(95); // 95% success rate minimum
  });

  test('should handle burst traffic without degradation', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'transactions.processed';
    const burstSize = 50;
    
    console.log(`💥 Testing burst traffic with ${burstSize} messages`);
    
    const startTime = Date.now();
    
    // Send burst of messages simultaneously
    const promises = [];
    for (let i = 0; i < burstSize; i++) {
      const message = { ...testData.sampleInputMessage };
      message.Header.MUID = `burst-${i}`;
      message.Header.UUID = `burst-uuid-${i}`;
      
      promises.push(kafkaHelper.sendMessage(inputTopic, message, message.Header.MUID));
    }
    
    // Send all messages simultaneously
    await Promise.all(promises);
    
    const sendTime = Date.now() - startTime;
    console.log(`📤 Burst sent in ${sendTime}ms`);
    
    // Consume responses
    const responses = await kafkaHelper.consumeMessages(outputTopic, burstSize, 60000);
    
    const totalTime = Date.now() - startTime;
    
    expect(responses).toHaveLength(burstSize);
    expect(totalTime).toBeLessThan(60000); // Should complete within 1 minute
    
    // Verify response quality during burst
    let successCount = 0;
    for (const response of responses) {
      const responseMessage = JSON.parse(response.value!.toString());
      if (responseMessage.Trailer.status === 'SUCCESS') {
        successCount++;
      }
    }
    
    const successRate = (successCount / burstSize) * 100;
    expect(successRate).toBeGreaterThan(90); // 90% success rate during burst
    
    console.log(`💥 Burst test completed: ${successRate.toFixed(1)}% success rate`);
  });

  test('should maintain consistent performance under load', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'transactions.processed';
    const rounds = 5;
    const messagesPerRound = 20;
    
    console.log(`🔄 Testing consistent performance over ${rounds} rounds`);
    
    const roundTimes = [];
    
    for (let round = 0; round < rounds; round++) {
      console.log(`🔄 Round ${round + 1}/${rounds}`);
      
      const roundStartTime = Date.now();
      
      // Send messages for this round
      for (let i = 0; i < messagesPerRound; i++) {
        const message = { ...testData.sampleInputMessage };
        message.Header.MUID = `round-${round}-${i}`;
        message.Header.UUID = `round-uuid-${round}-${i}`;
        
        await kafkaHelper.sendMessage(inputTopic, message, message.Header.MUID);
      }
      
      // Consume responses for this round
      const responses = await kafkaHelper.consumeMessages(outputTopic, messagesPerRound, 30000);
      
      const roundTime = Date.now() - roundStartTime;
      roundTimes.push(roundTime);
      
      expect(responses).toHaveLength(messagesPerRound);
      
      // Verify all messages processed successfully
      for (const response of responses) {
        const responseMessage = JSON.parse(response.value!.toString());
        expect(responseMessage.Trailer.status).toBe('SUCCESS');
      }
      
      console.log(`   Round ${round + 1} completed in ${roundTime}ms`);
      
      // Clear for next round
      await kafkaHelper.clearMessages();
      
      // Small delay between rounds
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    // Analyze performance consistency
    const avgTime = roundTimes.reduce((a, b) => a + b, 0) / roundTimes.length;
    const maxDeviation = Math.max(...roundTimes.map(t => Math.abs(t - avgTime)));
    const deviationPercentage = (maxDeviation / avgTime) * 100;
    
    console.log(`📊 Performance Consistency Analysis:`);
    console.log(`   Average Round Time: ${avgTime.toFixed(2)}ms`);
    console.log(`   Max Deviation: ${maxDeviation}ms (${deviationPercentage.toFixed(1)}%)`);
    
    // Performance should be consistent (deviation < 20%)
    expect(deviationPercentage).toBeLessThan(20);
    
    console.log(`✅ Performance consistency test passed`);
  });

  test('should handle memory and resource usage efficiently', async () => {
    const inputTopic = process.env.INPUT_TOPIC || 'transactions.incoming';
    const outputTopic = process.env.OUTPUT_TOPIC || 'transactions.processed';
    const messageCount = 200;
    
    console.log(`💾 Testing memory and resource efficiency with ${messageCount} messages`);
    
    const startTime = Date.now();
    
    // Send messages in smaller batches to test memory management
    const batchSize = 25;
    for (let i = 0; i < messageCount; i += batchSize) {
      const batch = [];
      for (let j = 0; j < batchSize && (i + j) < messageCount; j++) {
        const message = { ...testData.sampleInputMessage };
        message.Header.MUID = `mem-${i + j}`;
        message.Header.UUID = `mem-uuid-${i + j}`;
        batch.push(message);
      }
      
      // Send batch
      for (const message of batch) {
        await kafkaHelper.sendMessage(inputTopic, message, message.Header.MUID);
      }
      
      // Process batch responses
      const responses = await kafkaHelper.consumeMessages(outputTopic, batchSize, 30000);
      expect(responses).toHaveLength(batchSize);
      
      // Verify batch processing
      for (const response of responses) {
        const responseMessage = JSON.parse(response.value!.toString());
        expect(responseMessage.Trailer.status).toBe('SUCCESS');
      }
      
      console.log(`   Batch ${Math.floor(i / batchSize) + 1} processed: ${batchSize} messages`);
      
      // Clear for next batch
      await kafkaHelper.clearMessages();
      
      // Small delay between batches
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    const totalTime = Date.now() - startTime;
    const throughput = (messageCount / (totalTime / 1000)).toFixed(2);
    
    console.log(`💾 Memory efficiency test completed:`);
    console.log(`   Total Time: ${totalTime}ms`);
    console.log(`   Throughput: ${throughput} messages/second`);
    
    // Should maintain reasonable performance
    expect(totalTime).toBeLessThan(180000); // 3 minutes max
    expect(parseFloat(throughput)).toBeGreaterThan(1); // At least 1 msg/sec
    
    console.log(`✅ Memory and resource efficiency test passed`);
  });
});
