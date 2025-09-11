package com.anz.fastpayment.router.messaging;

import com.anz.fastpayment.router.service.RouterOrchestrator;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class ActiveMqMessageListener extends AbstractJmsMessageListener {

    public ActiveMqMessageListener(RouterOrchestrator routerOrchestrator) {
        super(routerOrchestrator);
    }

    @JmsListener(destination = "${app.activemq.input-queue:payment.inbound}")
    public void onMessage(@Payload String payload) {
        handleMessage(payload, "[MQ]");
    }
}


