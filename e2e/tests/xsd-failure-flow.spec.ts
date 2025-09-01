import { test, expect } from '@playwright/test';
import { Kafka } from 'kafkajs';

// NOTE: This is a scaffold; wire real brokers and Spanner emulator before running.

test.describe('XSD failure triggers sender flow', () => {
  test('router -> pacs002-requests, sender -> pacs002 + event JSON', async () => {
    // Arrange
    const brokers = process.env.KAFKA_BROKERS?.split(',') ?? ['localhost:29092'];
    const kafka = new Kafka({ clientId: 'e2e-tests', brokers });
    const consumer = kafka.consumer({ groupId: `e2e-xsd-${process.pid}-${Math.random().toString(36).slice(2, 6)}` });

    await consumer.connect();
    await consumer.subscribe({ topic: process.env.PACS002_REQUESTS_TOPIC ?? 'pacs002-requests', fromBeginning: true });

    // Act: Push an invalid XML to ActiveMQ REST plugin (raw body) AFTER subscribe to ensure consumption
    const invalidXml = '<root/>';
    const sendUrlBase = process.env.ACTIVEMQ_ADMIN_URL || 'http://localhost:8161';
    const auth = Buffer.from(`${process.env.ACTIVEMQ_USERNAME || 'admin'}:${process.env.ACTIVEMQ_PASSWORD || 'admin'}`).toString('base64');
    await fetch(`${sendUrlBase}/api/message?destination=queue://payment.inbound`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': `Basic ${auth}` },
      body: new URLSearchParams({ body: invalidXml }).toString()
    });

    let resolved = false;
    const timeoutId = setTimeout(() => { if (!resolved) resolved = true; }, 20000);
    await new Promise<void>((resolve) => {
      consumer.run({
        eachMessage: async ({ message }) => {
          if (!message.value) return;
          const payload = message.value.toString();
          try {
            let obj: any = JSON.parse(payload);
            if (typeof obj === 'string') {
              try { obj = JSON.parse(obj); } catch {}
            }
            if (obj && obj.puid && obj.originalXml === invalidXml && obj.error) {
              if (!resolved) { resolved = true; clearTimeout(timeoutId); resolve(); }
            }
          } catch (e) {
            // ignore parse errors
          }
        },
      }).catch(() => {});
    });
    await consumer.stop();
    await consumer.disconnect();
    expect(resolved).toBeTruthy();
  });
});
