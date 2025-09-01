package com.anz.fastpayment.inward.model;

import com.anz.fastpayment.inward.avro.ProcessedTransactionMessage;

import java.util.List;

/**
 * Processing Result for transaction processing pipeline
 * Represents the outcome of processing a transaction message
 */
public class ProcessingResult {
    
    public enum Status {
        SUCCESS,
        DUPLICATE,
        PARSING_FAILED,
        VALIDATION_FAILED,
        BUSINESS_PROCESSING_FAILED,
        SYSTEM_ERROR
    }
    
    private final boolean success;
    private final Status status;
    private final String errorMessage;
    private final List<String> validationErrors;
    private final ProcessedTransactionMessage processedMessage;
    private final String muid;
    private final long processingDurationMs;
    
    private ProcessingResult(Builder builder) {
        this.success = builder.success;
        this.status = builder.status;
        this.errorMessage = builder.errorMessage;
        this.validationErrors = builder.validationErrors;
        this.processedMessage = builder.processedMessage;
        this.muid = builder.muid;
        this.processingDurationMs = builder.processingDurationMs;
    }
    
    // Factory methods for different result types
    public static ProcessingResult success(ProcessedTransactionMessage processedMessage, long processingDurationMs) {
        return new Builder()
                .success(true)
                .status(Status.SUCCESS)
                .processedMessage(processedMessage)
                .processingDurationMs(processingDurationMs)
                .build();
    }
    
    public static ProcessingResult duplicate(String muid, long processingDurationMs) {
        return new Builder()
                .success(false)
                .status(Status.DUPLICATE)
                .muid(muid)
                .processingDurationMs(processingDurationMs)
                .build();
    }
    
    public static ProcessingResult parsingFailed(String errorMessage, long processingDurationMs) {
        return new Builder()
                .success(false)
                .status(Status.PARSING_FAILED)
                .errorMessage(errorMessage)
                .processingDurationMs(processingDurationMs)
                .build();
    }
    
    public static ProcessingResult validationFailed(List<String> validationErrors, long processingDurationMs) {
        return new Builder()
                .success(false)
                .status(Status.VALIDATION_FAILED)
                .validationErrors(validationErrors)
                .processingDurationMs(processingDurationMs)
                .build();
    }
    
    public static ProcessingResult businessProcessingFailed(String errorMessage, long processingDurationMs) {
        return new Builder()
                .success(false)
                .status(Status.BUSINESS_PROCESSING_FAILED)
                .errorMessage(errorMessage)
                .processingDurationMs(processingDurationMs)
                .build();
    }
    
    public static ProcessingResult systemError(Exception exception, long processingDurationMs) {
        return new Builder()
                .success(false)
                .status(Status.SYSTEM_ERROR)
                .errorMessage(exception.getMessage())
                .processingDurationMs(processingDurationMs)
                .build();
    }
    
    // Getters
    public boolean isSuccess() {
        return success;
    }
    
    public Status getStatus() {
        return status;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public List<String> getValidationErrors() {
        return validationErrors;
    }
    
    public ProcessedTransactionMessage getProcessedMessage() {
        return processedMessage;
    }
    
    public String getMuid() {
        return muid;
    }
    
    public long getProcessingDurationMs() {
        return processingDurationMs;
    }
    
    @Override
    public String toString() {
        return String.format("ProcessingResult{success=%s, status=%s, errorMessage='%s', muid='%s', processingDurationMs=%d}", 
                           success, status, errorMessage, muid, processingDurationMs);
    }
    
    // Builder pattern
    public static class Builder {
        private boolean success;
        private Status status;
        private String errorMessage;
        private List<String> validationErrors;
        private ProcessedTransactionMessage processedMessage;
        private String muid;
        private long processingDurationMs;
        
        public Builder success(boolean success) {
            this.success = success;
            return this;
        }
        
        public Builder status(Status status) {
            this.status = status;
            return this;
        }
        
        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }
        
        public Builder validationErrors(List<String> validationErrors) {
            this.validationErrors = validationErrors;
            return this;
        }
        
        public Builder processedMessage(ProcessedTransactionMessage processedMessage) {
            this.processedMessage = processedMessage;
            return this;
        }
        
        public Builder muid(String muid) {
            this.muid = muid;
            return this;
        }
        
        public Builder processingDurationMs(long processingDurationMs) {
            this.processingDurationMs = processingDurationMs;
            return this;
        }
        
        public ProcessingResult build() {
            return new ProcessingResult(this);
        }
    }
}
