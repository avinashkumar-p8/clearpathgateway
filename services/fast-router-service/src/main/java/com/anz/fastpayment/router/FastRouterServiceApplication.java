package com.anz.fastpayment.router;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * Fast Router Service Application
 * 
 * Entry point for all messages from CPG/G3 Host.
 * Handles message reception, routing, validation, and enrichment.
 */
@SpringBootApplication
@EnableKafka
public class FastRouterServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FastRouterServiceApplication.class, args);
    }
}