package com.anz.fastpayment.router.config;

import jakarta.jms.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;

import com.ibm.mq.jakarta.jms.MQConnectionFactory;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;

@Configuration
@Profile({"gcp","prod"})
public class IbmMqConfig {

    @Value("${app.mq.host}")
    private String host;

    @Value("${app.mq.port}")
    private int port;

    @Value("${app.mq.channel}")
    private String channel;

    @Value("${app.mq.queue-manager}")
    private String queueManager;

    @Value("${app.mq.user:}")
    private String user;

    @Value("${app.mq.password:}")
    private String password;

    @Bean
    @Primary
    public ConnectionFactory ibmMqConnectionFactory() throws Exception {
        MQConnectionFactory factory = new MQConnectionFactory();
        factory.setHostName(host);
        factory.setPort(port);
        factory.setQueueManager(queueManager);
        factory.setChannel(channel);
        factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        if (user != null && !user.isBlank()) {
            factory.setStringProperty(WMQConstants.USERID, user);
        }
        if (password != null && !password.isBlank()) {
            factory.setStringProperty(WMQConstants.PASSWORD, password);
        }
        factory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
        return factory;
    }

    @Bean
    public JmsListenerContainerFactory<?> jmsListenerContainerFactory(ConnectionFactory connectionFactory) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setSessionTransacted(true);
        factory.setConcurrency("3-10");
        return factory;
    }
}


