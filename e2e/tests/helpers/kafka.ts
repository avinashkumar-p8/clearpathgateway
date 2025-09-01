import { Kafka, Consumer, Producer } from 'kafkajs';

export function createKafka(clientId: string, brokers: string[]) {
  const kafka = new Kafka({ clientId, brokers });
  return {
    consumer: () => kafka.consumer({ groupId: clientId }),
    producer: () => kafka.producer()
  };
}
