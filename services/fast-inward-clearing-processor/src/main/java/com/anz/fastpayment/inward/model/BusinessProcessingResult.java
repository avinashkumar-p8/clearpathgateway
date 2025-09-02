package com.anz.fastpayment.inward.model;

import com.anz.fastpayment.inward.avro.ResponseMessage;

/**
 * Result of business processing operation
 */
public class BusinessProcessingResult {
    
    private final boolean success;
    private final ResponseMessage responseMessage;
    private final String errorMessage;
    
    public BusinessProcessingResult(boolean success, ResponseMessage responseMessage, String errorMessage) {
        this.success = success;
        this.responseMessage = responseMessage;
        this.errorMessage = errorMessage;
    }
    
    // Factory methods
    public static BusinessProcessingResult success(ResponseMessage responseMessage) {
        return new BusinessProcessingResult(true, responseMessage, null);
    }
    
    public static BusinessProcessingResult failed(String errorMessage) {
        return new BusinessProcessingResult(false, null, errorMessage);
    }
    
    // Getters
    public boolean isSuccess() {
        return success;
    }
    
    public ResponseMessage getResponseMessage() {
        return responseMessage;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public boolean hasError() {
        return errorMessage != null;
    }
    
    @Override
    public String toString() {
        return String.format("BusinessProcessingResult{success=%s, errorMessage='%s', responseMessageId='%s'}", 
                           success, errorMessage, 
                           responseMessage != null ? "ResponseMessage" : "null");
    }
}
