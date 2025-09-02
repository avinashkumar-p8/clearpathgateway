package com.anz.fastpayment.inward.scheme.validation.model;

import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;

/**
 * Validation Result Model for Banking Operations
 * Collects validation results for different tags
 */
public class ValidationResult {
    
    private final String transactionId;
    private final LocalDateTime timestamp;
    private final List<TagValidationResult> tagResults;
    private final boolean overallSuccess;
    
    public ValidationResult(String transactionId, List<TagValidationResult> tagResults) {
        this.transactionId = transactionId;
        this.timestamp = LocalDateTime.now();
        this.tagResults = tagResults != null ? tagResults : new ArrayList<>();
        this.overallSuccess = this.tagResults.stream().allMatch(TagValidationResult::isSuccess);
    }
    
    public String getTransactionId() {
        return transactionId;
    }
    
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
    
    public List<TagValidationResult> getTagResults() {
        return new ArrayList<>(tagResults);
    }
    
    public boolean isOverallSuccess() {
        return overallSuccess;
    }
    
    public boolean hasFailures() {
        return tagResults.stream().anyMatch(result -> !result.isSuccess());
    }
    
    public boolean hasMissingTags() {
        return tagResults.stream().anyMatch(result -> result.getStatus() == ValidationStatus.MISSING);
    }
    
    public List<TagValidationResult> getFailedValidations() {
        return tagResults.stream()
            .filter(result -> !result.isSuccess())
            .toList();
    }
    
    public List<TagValidationResult> getSuccessfulValidations() {
        return tagResults.stream()
            .filter(TagValidationResult::isSuccess)
            .toList();
    }
    
    @Override
    public String toString() {
        return String.format("ValidationResult{transactionId='%s', overallSuccess=%s, tagCount=%d, timestamp=%s}",
            transactionId, overallSuccess, tagResults.size(), timestamp);
    }
}
