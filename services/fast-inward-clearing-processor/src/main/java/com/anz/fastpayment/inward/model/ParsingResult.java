package com.anz.fastpayment.inward.model;

import java.util.Map;

/**
 * Result of message parsing operation
 */
public class ParsingResult {
    
    private final boolean success;
    private final Map<String, Object> messagePayload;
    private final TransactionMessage transactionMessage;
    private final String errorMessage;
    
    public ParsingResult(boolean success, Map<String, Object> messagePayload, 
                        TransactionMessage transactionMessage, String errorMessage) {
        this.success = success;
        this.messagePayload = messagePayload;
        this.transactionMessage = transactionMessage;
        this.errorMessage = errorMessage;
    }
    
    // Factory methods
    public static ParsingResult success(Map<String, Object> messagePayload, TransactionMessage transactionMessage) {
        return new ParsingResult(true, messagePayload, transactionMessage, null);
    }
    
    public static ParsingResult failed(String errorMessage) {
        return new ParsingResult(false, null, null, errorMessage);
    }
    
    // Getters
    public boolean isSuccess() {
        return success;
    }
    
    public Map<String, Object> getMessagePayload() {
        return messagePayload;
    }
    
    public TransactionMessage getTransactionMessage() {
        return transactionMessage;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public boolean hasError() {
        return errorMessage != null;
    }
    
    @Override
    public String toString() {
        return String.format("ParsingResult{success=%s, errorMessage='%s', payloadSize=%d}", 
                           success, errorMessage, 
                           messagePayload != null ? messagePayload.size() : 0);
    }
}
