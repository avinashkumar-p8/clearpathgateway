package com.anz.fastpayment.inward.service.impl;

import com.anz.fastpayment.inward.avro.ProcessedTransactionMessage;
import com.anz.fastpayment.inward.service.ClearingProcessorService;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Simplified Clearing Processor Service Implementation for Core Idempotency
 * Handles basic message processing needed for idempotency testing
 */
@Service
public class ClearingProcessorServiceImpl implements ClearingProcessorService {

    private static final Logger logger = LoggerFactory.getLogger(ClearingProcessorServiceImpl.class);

    @Override
    public ProcessedTransactionMessage processAvroTransaction(GenericRecord avroMessage) throws Exception {
        logger.debug("Processing Avro transaction message for idempotency testing");
        
        try {
            // For idempotency testing, we just need to create a basic processed message
            // The actual business logic is not needed for core idempotency functionality
            
            // Create a simple processed message
            ProcessedTransactionMessage processedMessage = new ProcessedTransactionMessage();
            
            // Set basic fields for idempotency verification
            processedMessage.setTransactionId("PROCESSED-" + String.valueOf(System.currentTimeMillis()));
            processedMessage.setStatus("PROCESSED");
            processedMessage.setProcessingTimestamp(String.valueOf(System.currentTimeMillis()));
            
            logger.debug("Successfully processed Avro transaction message for idempotency testing");
            return processedMessage;
            
        } catch (Exception e) {
            logger.error("Error processing Avro transaction message for idempotency testing", e);
            throw e;
        }
    }
}
