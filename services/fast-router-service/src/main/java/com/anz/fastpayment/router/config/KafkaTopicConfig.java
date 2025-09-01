package com.anz.fastpayment.router.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("local")
public class KafkaTopicConfig {

    @Value("${app.kafka.topics.payment-messages:payment-messages}")
    private String paymentMessagesTopic;

    @Value("${app.kafka.topics.exception-queue:exception-queue}")
    private String exceptionTopic;

    @Value("${app.kafka.topics.bank-availability:bank-availability}")
    private String bankAvailabilityTopic;

    @Value("${app.kafka.topics.pacs002-requests:pacs002-requests}")
    private String pacs002RequestsTopic;

    @Bean
    public NewTopic paymentMessagesTopic() {
        return new NewTopic(paymentMessagesTopic, 1, (short) 1);
    }

    @Bean
    public NewTopic exceptionQueueTopic() {
        return new NewTopic(exceptionTopic, 1, (short) 1);
    }

    @Bean
    public NewTopic bankAvailabilityTopic() {
        return new NewTopic(bankAvailabilityTopic, 1, (short) 1);
    }

    @Bean
    public NewTopic pacs002RequestsTopic() {
        return new NewTopic(pacs002RequestsTopic, 1, (short) 1);
    }
}


