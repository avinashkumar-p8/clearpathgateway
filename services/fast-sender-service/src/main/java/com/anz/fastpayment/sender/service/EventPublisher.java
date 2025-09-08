package com.anz.fastpayment.sender.service;

public interface EventPublisher {
    void publish(String topic, Object payload);
}





