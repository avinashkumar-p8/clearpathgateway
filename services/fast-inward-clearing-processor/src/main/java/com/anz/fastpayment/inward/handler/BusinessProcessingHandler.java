package com.anz.fastpayment.inward.handler;

import com.anz.fastpayment.inward.model.BusinessProcessingResult;

import java.util.Map;

/**
 * Handler for business processing operations
 * Processes validated messages through business logic
 */
public interface BusinessProcessingHandler {
    
    /**
     * Process message through business logic
     * 
     * @param messagePayload Validated message payload
     * @return BusinessProcessingResult containing processed message
     */
    BusinessProcessingResult process(Map<String, Object> messagePayload);
    
    /**
     * Process message with transaction ID
     * 
     * @param messagePayload Validated message payload
     * @param transactionId Transaction identifier for logging
     * @return BusinessProcessingResult containing processed message
     */
    BusinessProcessingResult process(Map<String, Object> messagePayload, String transactionId);
    
    /**
     * Convert message payload to Avro format for processing
     * 
     * @param messagePayload Message payload as Map
     * @return GenericRecord for business processing
     */
    org.apache.avro.generic.GenericRecord convertToAvro(Map<String, Object> messagePayload);
    
    /**
     * Apply business rules to message
     * 
     * @param messagePayload Message payload
     * @return true if business rules pass
     */
    boolean applyBusinessRules(Map<String, Object> messagePayload);
}
