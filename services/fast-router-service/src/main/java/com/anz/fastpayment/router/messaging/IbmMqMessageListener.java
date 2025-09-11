package com.anz.fastpayment.router.messaging;

import com.anz.fastpayment.router.service.RouterOrchestrator;
import org.springframework.context.annotation.Profile;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Profile({"gcp","prod"})
public class IbmMqMessageListener extends AbstractJmsMessageListener {

    public IbmMqMessageListener(RouterOrchestrator routerOrchestrator) {
        super(routerOrchestrator);
    }

    @JmsListener(destination = "${app.mq.input-queue:payment.inbound}")
    public void onMessage(@Payload String payload) {
        handleMessage(payload, "[IBM-MQ]");
    }
}





