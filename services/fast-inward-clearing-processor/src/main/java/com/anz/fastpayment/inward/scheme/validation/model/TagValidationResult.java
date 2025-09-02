package com.anz.fastpayment.inward.scheme.validation.model;

import java.time.LocalDateTime;

/**
 * Tag Validation Result Model for Banking Operations
 * Represents the validation result for a single tag
 */
public class TagValidationResult {
    
    private final String tagName;
    private final String tagValue;
    private final ValidationStatus status;
    private final String errorMessage;
    private final String errorCode;
    private final LocalDateTime timestamp;
    
    public TagValidationResult(String tagName, String tagValue, ValidationStatus status) {
        this(tagName, tagValue, status, null, null);
    }
    
    public TagValidationResult(String tagName, String tagValue, ValidationStatus status, 
                             String errorMessage, String errorCode) {
        this.tagName = tagName;
        this.tagValue = tagValue;
        this.status = status;
        this.errorMessage = errorMessage;
        this.errorCode = errorCode;
        this.timestamp = LocalDateTime.now();
    }
    
    public String getTagName() {
        return tagName;
    }
    
    public String getTagValue() {
        return tagValue;
    }
    
    public ValidationStatus getStatus() {
        return status;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public String getErrorCode() {
        return errorCode;
    }
    
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
    
    public boolean isSuccess() {
        return status == ValidationStatus.SUCCESS;
    }
    
    public boolean isFailure() {
        return status == ValidationStatus.FAILURE;
    }
    
    public boolean isMissing() {
        return status == ValidationStatus.MISSING;
    }
    
    @Override
    public String toString() {
        return String.format("TagValidationResult{tagName='%s', tagValue='%s', status=%s, errorMessage='%s', errorCode='%s'}",
            tagName, tagValue, status, errorMessage, errorCode);
    }
}
