package com.anz.fastpayment.inward.service;

import com.anz.fastpayment.inward.avro.ProcessedTransactionMessage;
import org.apache.avro.generic.GenericRecord;

/**
 * Simplified Clearing Processor Service for Core Idempotency
 * Handles basic message processing needed for idempotency testing
 */
public interface ClearingProcessorService {

    /**
     * Process Avro transaction message
     * Returns processed message for idempotency verification
     */
    ProcessedTransactionMessage processAvroTransaction(GenericRecord avroMessage) throws Exception;
}
