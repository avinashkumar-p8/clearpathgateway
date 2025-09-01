package com.anz.fastpayment.inward.handler;

import com.anz.fastpayment.inward.model.IdempotencyResult;
import com.anz.fastpayment.inward.model.ProcessingContext;

/**
 * Handler for idempotency operations
 * Ensures exactly-once processing of transaction messages
 */
public interface IdempotencyHandler {
    
    /**
     * Check if message is new and register it for processing
     * 
     * @param context Processing context containing message information
     * @return IdempotencyResult indicating if message is new or duplicate
     */
    IdempotencyResult checkAndRegister(ProcessingContext context);
    
    /**
     * Update processing status for a message
     * 
     * @param muid Message Unique ID
     * @param status Processing status (COMPLETED, FAILED, etc.)
     */
    void updateProcessingStatus(String muid, String status);
    
    /**
     * Extract MUID from processing context
     * 
     * @param context Processing context
     * @return Extracted MUID or generated fallback
     */
    String extractMuid(ProcessingContext context);
}
