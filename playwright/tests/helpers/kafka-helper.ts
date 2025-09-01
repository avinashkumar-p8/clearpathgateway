import { Kafka, Producer, Consumer, KafkaMessage } from 'kafkajs';
import dotenv from 'dotenv';

dotenv.config();

export class KafkaTestHelper {
  private kafka: Kafka;
  private producer: Producer;
  private consumer: Consumer;
  private messages: KafkaMessage[] = [];

  constructor() {
    this.kafka = new Kafka({
      clientId: 'playwright-test-client',
      brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    });

    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({ groupId: 'playwright-test-group' });
  }

  async connect() {
    await this.producer.connect();
    await this.consumer.connect();
  }

  async disconnect() {
    await this.producer.disconnect();
    await this.consumer.disconnect();
  }

  async sendMessage(topic: string, message: any, key?: string) {
    await this.producer.send({
      topic,
      messages: [{ key, value: JSON.stringify(message) }],
    });
  }

  async consumeMessages(topic: string, count: number = 1, timeout: number = 10000): Promise<KafkaMessage[]> {
    this.messages = [];
    
    await this.consumer.subscribe({ topic, fromBeginning: true });
    
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        this.messages.push(message);
        if (this.messages.length >= count) {
          await this.consumer.stop();
        }
      },
    });

    // Wait for messages or timeout
    const startTime = Date.now();
    while (this.messages.length < count && (Date.now() - startTime) < timeout) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    return this.messages.slice(0, count);
  }

  async clearMessages() {
    this.messages = [];
  }

  getMessages() {
    return this.messages;
  }
}

export const kafkaHelper = new KafkaTestHelper();
