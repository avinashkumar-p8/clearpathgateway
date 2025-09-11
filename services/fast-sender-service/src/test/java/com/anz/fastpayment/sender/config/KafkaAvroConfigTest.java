package com.anz.fastpayment.sender.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KafkaAvroConfigTest {
	@Test
	void buildsProducerAndListenerFactory() {
		KafkaAvroConfig cfg = new KafkaAvroConfig();
		org.springframework.test.util.ReflectionTestUtils.setField(cfg, "bootstrapServers", "localhost:9092");
		assertNotNull(cfg.avroProducerFactory());
		assertNotNull(cfg.avroKafkaTemplate());
		assertNotNull(cfg.byteArrayKafkaListenerContainerFactory());
	}
}
