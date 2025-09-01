package com.anz.fastpayment.nonvalsender;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Fast Non-Value Sender Service Application
 * 
 * Handles outbound transmission of all administrative and non-value messages from the 
 * APEAFAST-SG ClearPath Gateway system to the CPG/G3 network. This service processes 
 * bank availability notifications, administrative messages, settlement confirmations, 
 * and other operational messages that do not involve actual payment values.
 * 
 * Key Features:
 * - gRPC server for receiving requests from availability service
 * - REST endpoints for manual administrative operations
 * - Kafka event consumption for automated processing
 * - IBM MQ message production to CPG/G3 network
 * - Message template engine for XML generation
 * - Enterprise-grade delivery tracking and retry mechanisms
 * 
 * @author APEAFAST-SG Team
 * @version 1.0.0
 */
@SpringBootApplication
@EnableKafka
@EnableAsync
@EnableScheduling
public class FastNonValSenderApplication {

    public static void main(String[] args) {
        // Enable virtual threads for high-performance concurrent processing
        System.setProperty("spring.threads.virtual.enabled", "true");
        
        SpringApplication.run(FastNonValSenderApplication.class, args);
    }
}