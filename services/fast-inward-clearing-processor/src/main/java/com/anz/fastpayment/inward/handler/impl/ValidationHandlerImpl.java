package com.anz.fastpayment.inward.handler.impl;

import com.anz.fastpayment.inward.handler.ValidationHandler;
import com.anz.fastpayment.inward.scheme.validation.model.TagValidationResult;
import com.anz.fastpayment.inward.scheme.validation.model.ValidationResult;
import com.anz.fastpayment.inward.scheme.validation.service.SchemeValidationOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of ValidationHandler
 * Handles message validation operations using existing SchemeValidationOrchestrator
 */
@Service
public class ValidationHandlerImpl implements ValidationHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(ValidationHandlerImpl.class);
    
    private final SchemeValidationOrchestrator validationOrchestrator;
    
    public ValidationHandlerImpl(SchemeValidationOrchestrator validationOrchestrator) {
        this.validationOrchestrator = validationOrchestrator;
    }
    
    @Override
    public ValidationResult validate(Map<String, Object> messagePayload) {
        return validate(messagePayload, "unified-validation-handler");
    }
    
    @Override
    public ValidationResult validate(Map<String, Object> messagePayload, String transactionId) {
        try {
            if (messagePayload == null || messagePayload.isEmpty()) {
                logger.warn("Message payload is null or empty for transaction: {}", transactionId);
                return createEmptyValidationResult(transactionId);
            }
            
            logger.debug("Starting validation for transaction: {} with payload size: {}", 
                        transactionId, messagePayload.size());
            
            // Use existing SchemeValidationOrchestrator for validation
            ValidationResult validationResult = validationOrchestrator.validateMessage(messagePayload, transactionId);
            
            if (validationResult != null) {
                logger.debug("Validation completed for transaction: {} - Success: {}", 
                           transactionId, validationResult.isOverallSuccess());
            } else {
                logger.warn("ValidationOrchestrator returned null result for transaction: {}", transactionId);
                return createEmptyValidationResult(transactionId);
            }
            
            return validationResult;
            
        } catch (Exception e) {
            logger.error("Error during validation for transaction: {} - Error: {}", 
                        transactionId, e.getMessage(), e);
            return createErrorValidationResult(transactionId, e.getMessage());
        }
    }
    
    @Override
    public boolean isValidationSuccessful(ValidationResult validationResult) {
        try {
            if (validationResult == null) {
                logger.warn("ValidationResult is null");
                return false;
            }
            
            boolean isSuccessful = validationResult.isOverallSuccess();
            logger.debug("Validation success check: {} for transaction: {}", 
                        isSuccessful, validationResult.getTransactionId());
            
            return isSuccessful;
            
        } catch (Exception e) {
            logger.error("Error checking validation success: {}", e.getMessage(), e);
            return false;
        }
    }
    
    @Override
    public List<String> getValidationErrors(ValidationResult validationResult) {
        try {
            if (validationResult == null) {
                logger.warn("ValidationResult is null");
                return new ArrayList<>();
            }
            
            List<String> errors = new ArrayList<>();
            
            // Extract errors from failed validations
            if (validationResult.getFailedValidations() != null) {
                for (TagValidationResult failedValidation : validationResult.getFailedValidations()) {
                    String errorMessage = String.format("%s: %s", 
                                                      failedValidation.getTagName(), 
                                                      failedValidation.getErrorMessage());
                    errors.add(errorMessage);
                }
            }
            
            logger.debug("Extracted {} validation errors for transaction: {}", 
                        errors.size(), validationResult.getTransactionId());
            
            return errors;
            
        } catch (Exception e) {
            logger.error("Error extracting validation errors: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }
    
    /**
     * Create empty validation result for null/empty payload
     */
    private ValidationResult createEmptyValidationResult(String transactionId) {
        List<TagValidationResult> tagResults = new ArrayList<>();
        tagResults.add(new TagValidationResult("EMPTY_PAYLOAD", null, 
                                             com.anz.fastpayment.inward.scheme.validation.model.ValidationStatus.FAILURE, 
                                             "Message payload is null or empty", "EMPTY_PAYLOAD"));
        
        return new ValidationResult(transactionId, tagResults);
    }
    
    /**
     * Create error validation result for exceptions
     */
    private ValidationResult createErrorValidationResult(String transactionId, String errorMessage) {
        List<TagValidationResult> tagResults = new ArrayList<>();
        tagResults.add(new TagValidationResult("VALIDATION_ERROR", null, 
                                             com.anz.fastpayment.inward.scheme.validation.model.ValidationStatus.FAILURE, 
                                             "Validation error: " + errorMessage, "VALIDATION_ERROR"));
        
        return new ValidationResult(transactionId, tagResults);
    }
}
