package com.anz.fastpayment.inward.model;

/**
 * Result of idempotency check operation
 */
public class IdempotencyResult {
    
    private final String muid;
    private final boolean isNewMessage;
    private final String status;
    private final String errorMessage;
    
    public IdempotencyResult(String muid, boolean isNewMessage) {
        this.muid = muid;
        this.isNewMessage = isNewMessage;
        this.status = isNewMessage ? "NEW" : "DUPLICATE";
        this.errorMessage = null;
    }
    
    public IdempotencyResult(String muid, boolean isNewMessage, String status, String errorMessage) {
        this.muid = muid;
        this.isNewMessage = isNewMessage;
        this.status = status;
        this.errorMessage = errorMessage;
    }
    
    // Factory methods
    public static IdempotencyResult newMessage(String muid) {
        return new IdempotencyResult(muid, true);
    }
    
    public static IdempotencyResult duplicateMessage(String muid) {
        return new IdempotencyResult(muid, false);
    }
    
    public static IdempotencyResult error(String muid, String errorMessage) {
        return new IdempotencyResult(muid, false, "ERROR", errorMessage);
    }
    
    // Getters
    public String getMuid() {
        return muid;
    }
    
    public boolean isNewMessage() {
        return isNewMessage;
    }
    
    public String getStatus() {
        return status;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public boolean hasError() {
        return errorMessage != null;
    }
    
    @Override
    public String toString() {
        return String.format("IdempotencyResult{muid='%s', isNewMessage=%s, status='%s', errorMessage='%s'}", 
                           muid, isNewMessage, status, errorMessage);
    }
}
