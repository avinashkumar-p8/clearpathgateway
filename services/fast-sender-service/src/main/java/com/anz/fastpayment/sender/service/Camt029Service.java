package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.model.Camt029Request;

public interface Camt029Service {
    String handleCamt029Request(Camt029Request request);
}


