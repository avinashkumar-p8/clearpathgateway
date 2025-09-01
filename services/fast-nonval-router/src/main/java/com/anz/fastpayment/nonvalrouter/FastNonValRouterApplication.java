package com.anz.fastpayment.nonvalrouter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Fast Non-Value Router Service Application
 * 
 * Handles administrative and non-value payment messages within the APEAFAST-SG ClearPath Gateway.
 * Processes ADMN messages, bank statements (CAMT.053), settlement notifications, and other 
 * operational messages that do not involve actual payment values.
 * 
 * Key Features:
 * - IBM MQ message consumption from CPG/G3 network
 * - gRPC communication with availability service for bank status updates
 * - AWS S3 storage for bank statements and archival
 * - Kafka event publishing for settlement notifications
 * - High-performance message routing with virtual threads
 * - Enterprise-grade resilience patterns and monitoring
 * 
 * @author APEAFAST-SG Team
 * @version 1.0.0
 */
@SpringBootApplication
@EnableKafka
@EnableAsync
@EnableScheduling
public class FastNonValRouterApplication {

    public static void main(String[] args) {
        // Enable virtual threads for high-performance concurrent processing
        System.setProperty("spring.threads.virtual.enabled", "true");
        
        SpringApplication.run(FastNonValRouterApplication.class, args);
    }
}