package com.anz.fastpayment.sender.service;

import com.anz.fastpayment.sender.model.Pacs002Request;
import com.anz.fastpayment.sender.model.Pacs002Response;

public interface Pacs002Service {
    Pacs002Response handlePacs002Request(Pacs002Request request);
}
