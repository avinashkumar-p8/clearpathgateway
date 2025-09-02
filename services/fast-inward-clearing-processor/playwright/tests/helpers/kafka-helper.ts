import { Kafka, Producer, Consumer, KafkaMessage, Admin } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export class KafkaTestHelper {
  private kafka: Kafka;
  private producer: Producer;
  private consumer: Consumer;
  private admin: Admin;
  private schemaRegistry: SchemaRegistry;
  private isRunning: boolean = false;
  private currentTopic: string | null = null;
  private isConnected: boolean = false;

  constructor() {
    const uniqueId = Math.random().toString(36).substring(7);
    this.kafka = new Kafka({
      clientId: `playwright-test-client-${uniqueId}`,
      brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    });

    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({ 
      groupId: `playwright-test-group-${uniqueId}`,
      sessionTimeout: 30000,
      heartbeatInterval: 3000
    });
    this.admin = this.kafka.admin();

    // Initialize Schema Registry
    this.schemaRegistry = new SchemaRegistry({
      host: process.env.SCHEMA_REGISTRY_URL || 'http://localhost:8081'
    });
  }

  async connect() {
    if (!this.isConnected) {
      await this.producer.connect();
      await this.consumer.connect();
      await this.admin.connect();
      this.isConnected = true;
    }
  }

  async disconnect() {
    if (this.isRunning) {
      await this.consumer.stop();
      this.isRunning = false;
    }
    if (this.isConnected) {
      await this.producer.disconnect();
      await this.consumer.disconnect();
      await this.admin.disconnect();
      this.isConnected = false;
    }
  }

  async sendMessage(topic: string, message: any, key?: string) {
    await this.connect(); // Ensure connection before sending
    
    try {
      // Use the existing schema that's already registered
      const schemaId = await this.schemaRegistry.getLatestSchemaId('com.anz.fastpayment.inward.avro.InputMessage');

      // Serialize message to Avro format
      const avroBuffer = await this.schemaRegistry.encode(schemaId, message);

      await this.producer.send({
        topic,
        messages: [{ key, value: avroBuffer }],
      });
    } catch (error) {
      console.error('Error sending Avro message:', error);
      throw error;
    }
  }

  // Alias for sendMessage to match test expectations
  async produceMessage(topic: string, message: any, key?: string) {
    return this.sendMessage(topic, message, key);
  }

  async consumeMessages(topic: string, count: number = 1, timeout: number = 10000): Promise<KafkaMessage[]> {
    const messages: KafkaMessage[] = [];
    
    await this.connect(); // Ensure connection before consuming
    
    // If we're already running on a different topic, stop first
    if (this.isRunning && this.currentTopic !== topic) {
      await this.consumer.stop();
      this.isRunning = false;
      this.currentTopic = null;
    }
    
    // If not running, subscribe and start
    if (!this.isRunning) {
      await this.consumer.subscribe({ topic, fromBeginning: true });
      this.currentTopic = topic;
      this.isRunning = true;
      
      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          messages.push(message);
          if (messages.length >= count) {
            await this.consumer.stop();
            this.isRunning = false;
            this.currentTopic = null;
          }
        },
      });
    }
    
    // Wait for messages or timeout
    const startTime = Date.now();
    while (messages.length < count && (Date.now() - startTime) < timeout) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    return messages.slice(0, count);
  }

  // Single message consumer to match test expectations with MUID filtering
  async consumeMessage(topic: string, timeout: number = 10000, expectedMUID?: string): Promise<any> {
    const startTime = Date.now();
    
    while (Date.now() - startTime < timeout) {
      const messages = await this.consumeMessages(topic, 1, 2000); // Short timeout for each attempt
      
      if (messages.length > 0) {
        try {
          // Try to deserialize as Avro first
          if (messages[0].value) {
            const buffer = Buffer.from(messages[0].value);
            
            // For response messages, we need to get the ResponseMessage schema
            // The service sends ResponseMessage using the topic-based subject name
            const responseSchemaId = await this.schemaRegistry.getLatestSchemaId(`${topic}-value`);
            const decodedMessage = await this.schemaRegistry.decode(buffer, { schemaId: responseSchemaId });
            
            // If expectedMUID is provided, filter messages to only return matching ones
            if (expectedMUID && decodedMessage.Header && decodedMessage.Header.MUID !== expectedMUID) {
              console.log(`Filtered out message with MUID: ${decodedMessage.Header.MUID}, expected: ${expectedMUID}`);
              continue; // Keep trying to find the right message
            }
            
            return decodedMessage;
          }
        } catch (error) {
          console.log('Avro deserialization failed, trying fallback:', error.message);
          // Fallback to JSON parsing if Avro deserialization fails
          try {
            const jsonMessage = JSON.parse(messages[0].value?.toString() || '{}');
            
            // If expectedMUID is provided, filter messages to only return matching ones
            if (expectedMUID && jsonMessage.Header && jsonMessage.Header.MUID !== expectedMUID) {
              console.log(`Filtered out JSON message with MUID: ${jsonMessage.Header.MUID}, expected: ${expectedMUID}`);
              continue; // Keep trying to find the right message
            }
            
            return jsonMessage;
          } catch (jsonError) {
            console.log('JSON parsing also failed:', jsonError.message);
            return messages[0].value?.toString() || null;
          }
        }
      }
      
      // Wait a bit before trying again
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    return null; // Timeout reached
  }

  async clearMessages() {
    try {
      // Reset consumer state
      if (this.isRunning) {
        await this.consumer.stop();
        this.isRunning = false;
        this.currentTopic = null;
      }

      await this.connect();
      
      // Get all consumer groups and delete them to reset offsets
      const groups = await this.admin.listGroups();
      const testGroups = groups.groups.filter(group => 
        group.groupId.includes('playwright-test') || 
        group.groupId.includes('clear-topic')
      );
      
      for (const group of testGroups) {
        try {
          await this.admin.deleteGroups([group.groupId]);
          console.log(`Deleted consumer group: ${group.groupId}`);
        } catch (error) {
          // Ignore errors - group might not exist
        }
      }
      
      // COMPLETE TOPIC PURGING - Only drain input topic to prevent duplicate processing
      // Don't drain output topic as we need to consume the new responses
      const topics = ['transactions.incoming'];
      
      for (const topic of topics) {
        try {
          // Get topic metadata to find partitions
          const metadata = await this.admin.fetchTopicMetadata({ topics: [topic] });
          const topicMetadata = metadata.topics.find(t => t.name === topic);
          
          if (topicMetadata) {
            // For each partition, get the latest offset and consume all messages
            for (const partition of topicMetadata.partitions) {
              try {
                // Get the latest offset for this partition
                const offsets = await this.admin.fetchTopicOffsets(topic);
                const partitionOffset = offsets.find(o => o.partition === partition.partitionId);
                
                if (partitionOffset && partitionOffset.offset > 0) {
                  // Create a temporary consumer to drain ALL messages from this partition
                  const drainConsumer = this.kafka.consumer({ 
                    groupId: `drain-${topic}-${Date.now()}-${Math.random().toString(36).substring(7)}`,
                    sessionTimeout: 10000,
                    heartbeatInterval: 3000
                  });
                  
                  await drainConsumer.connect();
                  await drainConsumer.subscribe({ topic, fromBeginning: true });
                  
                  // Drain all messages with a timeout
                  const drainPromise = new Promise<void>((resolve) => {
                    let drainedCount = 0;
                    const timeout = setTimeout(() => {
                      resolve();
                    }, 3000); // 3 second timeout
                    
                    drainConsumer.run({
                      eachMessage: async ({ message }) => {
                        drainedCount++;
                        // Stop after draining a reasonable number or timeout
                        if (drainedCount >= 100) {
                          clearTimeout(timeout);
                          resolve();
                        }
                      },
                    });
                  });
                  
                  await drainPromise;
                  await drainConsumer.stop();
                  await drainConsumer.disconnect();
                  
                  console.log(`Drained ${topic} partition ${partition.partitionId}`);
                }
              } catch (error) {
                console.log(`Error draining ${topic} partition ${partition.partitionId}:`, error.message);
              }
            }
          }
        } catch (error) {
          console.log(`Error processing topic ${topic}:`, error.message);
        }
      }
      
      // Also reset offsets for our current consumer group to latest
      try {
        for (const topic of topics) {
          try {
            await this.admin.setOffsets({
              groupId: this.consumer.options.groupId!,
              topic,
              partitions: [{ partition: 0, offset: '-1' }] // -1 means latest
            });
          } catch (error) {
            // Ignore errors - might not have any offsets yet
          }
        }
      } catch (error) {
        // Ignore errors during offset reset
      }
      
    } catch (error) {
      console.log('Topic clearing completed with minor issues:', error.message);
    }
  }

  getMessages() {
    return [];
  }
}

export const kafkaHelper = new KafkaTestHelper();
