package com.anz.fastpayment.inward.model;

import com.anz.fastpayment.inward.avro.ProcessedTransactionMessage;

/**
 * Result of business processing operation
 */
public class BusinessProcessingResult {
    
    private final boolean success;
    private final ProcessedTransactionMessage processedMessage;
    private final String errorMessage;
    
    public BusinessProcessingResult(boolean success, ProcessedTransactionMessage processedMessage, String errorMessage) {
        this.success = success;
        this.processedMessage = processedMessage;
        this.errorMessage = errorMessage;
    }
    
    // Factory methods
    public static BusinessProcessingResult success(ProcessedTransactionMessage processedMessage) {
        return new BusinessProcessingResult(true, processedMessage, null);
    }
    
    public static BusinessProcessingResult failed(String errorMessage) {
        return new BusinessProcessingResult(false, null, errorMessage);
    }
    
    // Getters
    public boolean isSuccess() {
        return success;
    }
    
    public ProcessedTransactionMessage getProcessedMessage() {
        return processedMessage;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public boolean hasError() {
        return errorMessage != null;
    }
    
    @Override
    public String toString() {
        return String.format("BusinessProcessingResult{success=%s, errorMessage='%s', processedMessageId='%s'}", 
                           success, errorMessage, 
                           processedMessage != null ? processedMessage.getTransactionId() : "null");
    }
}
