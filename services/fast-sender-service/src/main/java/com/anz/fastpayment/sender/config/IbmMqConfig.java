package com.anz.fastpayment.sender.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

// With mq-jms-spring-boot-starter, IBM MQ ConnectionFactory and JmsTemplate are auto-configured
// based on ibm.mq.* properties. This class remains as a profile marker for prod.
@Configuration
@Profile("gcp")
public class IbmMqConfig {
}


