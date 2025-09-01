import { test, expect } from '@playwright/test';
import { Kafka } from 'kafkajs';

test.describe('Kafka Debug Test', () => {
  let kafka: Kafka;
  let producer: any;

  test.beforeAll(async () => {
    kafka = new Kafka({
      clientId: 'playwright-debug-test',
      brokers: ['localhost:9092'],
    });

    producer = kafka.producer();
    await producer.connect();
  });

  test.afterAll(async () => {
    await producer.disconnect();
  });

  test('should send simple message without headers', async () => {
    const message = {
      key: 'test-key',
      value: Buffer.from('test-value')
    };

    await producer.send({
      topic: 'transactions.incoming',
      messages: [message]
    });

    console.log('✅ Simple message sent successfully');
  });

  test('should send message with string headers', async () => {
    const message = {
      key: 'test-key-2',
      value: Buffer.from('test-value-2'),
      headers: [
        { key: 'test-header', value: 'test-value' }
      ]
    };

    await producer.send({
      topic: 'transactions.incoming',
      messages: [message]
    });

    console.log('✅ Message with string headers sent successfully');
  });

  test('should send message with buffer headers', async () => {
    const message = {
      key: 'test-key-3',
      value: Buffer.from('test-value-3'),
      headers: [
        { key: 'test-header', value: Buffer.from('test-value') }
      ]
    };

    await producer.send({
      topic: 'transactions.incoming',
      messages: [message]
    });

    console.log('✅ Message with buffer headers sent successfully');
  });
});
