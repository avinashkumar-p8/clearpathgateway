package com.anz.fastpayment.inward;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * Fast Inward Clearing Processor Application
 * 
 * Handles CTI (Credit Transfer Inward) and DDI (Direct Debit Inward) processing
 * with 4.5-second SLA compliance for Singapore Fast Payment system.
 */
@SpringBootApplication(exclude = {
    com.google.cloud.spring.autoconfigure.spanner.GcpSpannerAutoConfiguration.class,
    com.google.cloud.spring.autoconfigure.spanner.SpannerTransactionManagerAutoConfiguration.class,
    org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration.class,
    org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration.class
})
@EnableKafka
public class FastInwardClearingProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(FastInwardClearingProcessorApplication.class, args);
    }
}