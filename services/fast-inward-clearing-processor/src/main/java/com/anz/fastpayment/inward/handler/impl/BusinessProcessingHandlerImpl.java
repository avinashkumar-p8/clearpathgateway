package com.anz.fastpayment.inward.handler.impl;

import com.anz.fastpayment.inward.avro.ResponseMessage;
import com.anz.fastpayment.inward.handler.BusinessProcessingHandler;
import com.anz.fastpayment.inward.model.BusinessProcessingResult;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * BusinessProcessingHandler Implementation
 * Creates ResponseMessage with Trailer containing validation status
 */
@Service
public class BusinessProcessingHandlerImpl implements BusinessProcessingHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(BusinessProcessingHandlerImpl.class);
    
    @Override
    public BusinessProcessingResult process(Map<String, Object> messagePayload) {
        return process(messagePayload, "unified-business-processor");
    }
    
    @Override
    public BusinessProcessingResult process(Map<String, Object> messagePayload, String transactionId) {
        try {
            if (messagePayload == null || messagePayload.isEmpty()) {
                logger.warn("Message payload is null or empty for transaction: {}", transactionId);
                return BusinessProcessingResult.failed("Message payload is null or empty");
            }
            
            logger.debug("Starting business processing for transaction: {} with payload size: {}", 
                        transactionId, messagePayload.size());
            
            // For now, just return success without complex business processing
            // This can be enhanced later when business logic is needed
            
            // Create ResponseMessage with Trailer
            ResponseMessage response = createResponseMessage(messagePayload, transactionId, true, null);
            
            logger.info("Business processing completed successfully for transaction: {} (simplified implementation)", transactionId);
            return BusinessProcessingResult.success(response);
            
        } catch (Exception e) {
            logger.error("Error during business processing for transaction: {} - Error: {}", 
                        transactionId, e.getMessage(), e);
            return BusinessProcessingResult.failed("Business processing failed: " + e.getMessage());
        }
    }
    
    /**
     * Create ResponseMessage with Trailer object
     */
    private ResponseMessage createResponseMessage(Map<String, Object> messagePayload, 
                                                String transactionId, 
                                                boolean isSuccess, 
                                                String[] validationErrors) {
        try {
            ResponseMessage response = new ResponseMessage();
            
            // For now, create a minimal response with just the Trailer
            // The actual message content will be populated when we have the full Avro message
            // TODO: This needs to be updated to work with the full GenericRecord
            
            logger.debug("Created ResponseMessage for transaction: {} with status: {}", 
                        transactionId, isSuccess ? "SUCCESS" : "FAILED");
            
            // Create Trailer with ServiceStatus
            com.anz.fastpayment.inward.avro.ServiceStatus trailer = new com.anz.fastpayment.inward.avro.ServiceStatus();
            
            if (isSuccess) {
                trailer.setStatus("SUCCESS");
                trailer.setStatusCode("200");
                trailer.setStatusDesc(java.util.Arrays.asList("SUCCESS"));
            } else {
                trailer.setStatus("FAILED");
                trailer.setStatusCode("400");
                if (validationErrors != null && validationErrors.length > 0) {
                    trailer.setStatusDesc(java.util.Arrays.asList(validationErrors));
                } else {
                    trailer.setStatusDesc(java.util.Arrays.asList("Business processing failed"));
                }
            }
            
            response.setTrailer(trailer);
            
            logger.debug("Created ResponseMessage for transaction: {} with status: {}", 
                        transactionId, trailer.getStatus());
            
            return response;
            
        } catch (Exception e) {
            logger.error("Error creating ResponseMessage for transaction: {} - Error: {}", 
                        transactionId, e.getMessage(), e);
            return null;
        }
    }
    
    @Override
    public org.apache.avro.generic.GenericRecord convertToAvro(Map<String, Object> messagePayload) {
        // TODO: Implement conversion from Map to GenericRecord
        logger.debug("convertToAvro called - returning null (not implemented yet)");
        return null;
    }
    
    @Override
    public boolean applyBusinessRules(Map<String, Object> messagePayload) {
        // TODO: Implement business rules validation
        logger.debug("applyBusinessRules called - returning true (not implemented yet)");
        return true;
    }
    
    @Override
    public BusinessProcessingResult processWithAvro(org.apache.avro.generic.GenericRecord avroMessage, String transactionId) {
        try {
            logger.debug("Processing Avro message for transaction: {}", transactionId);
            
            // Create ResponseMessage with Trailer using the full Avro message
            ResponseMessage response = createResponseMessageFromAvro(avroMessage, transactionId, true, new String[0]);
            
            if (response != null) {
                logger.info("Avro message processing completed successfully for transaction: {}", transactionId);
                return BusinessProcessingResult.success(response);
            } else {
                logger.error("Failed to create response for transaction: {}", transactionId);
                return BusinessProcessingResult.failed("Failed to create response message");
            }
            
        } catch (Exception e) {
            logger.error("Error processing Avro message for transaction: {} - Error: {}", 
                        transactionId, e.getMessage(), e);
            return BusinessProcessingResult.failed("Avro message processing failed: " + e.getMessage());
        }
    }
    
    /**
     * Create a ResponseMessage by preserving the exact original message structure
     * This method uses Avro's built-in conversion capabilities
     */
    private ResponseMessage createResponseMessageFromOriginal(org.apache.avro.generic.GenericRecord avroMessage,
                                                              String transactionId,
                                                              boolean isSuccess,
                                                              String[] validationErrors) {
        try {
            // Create a new ResponseMessage using Avro's GenericRecord approach
            // This preserves the EXACT structure of the original message regardless of schema
            org.apache.avro.generic.GenericRecord responseRecord = new org.apache.avro.generic.GenericData.Record(ResponseMessage.getClassSchema());

            // Copy ALL fields from the original GenericRecord to the response GenericRecord
            // This works with ANY schema structure at runtime
            for (org.apache.avro.Schema.Field field : avroMessage.getSchema().getFields()) {
                String fieldName = field.name();
                Object fieldValue = avroMessage.get(fieldName);

                // Copy the field value directly - this preserves the exact structure
                responseRecord.put(fieldName, fieldValue);
            }

            // Add the Trailer field (this is the only new field we're adding)
            com.anz.fastpayment.inward.avro.ServiceStatus trailer = new com.anz.fastpayment.inward.avro.ServiceStatus();

            if (isSuccess) {
                trailer.setStatus("SUCCESS");
                trailer.setStatusCode("200");
                trailer.setStatusDesc(java.util.Arrays.asList("SUCCESS"));
            } else {
                trailer.setStatus("FAILED");
                trailer.setStatusCode("400");
                if (validationErrors != null && validationErrors.length > 0) {
                    trailer.setStatusDesc(java.util.Arrays.asList(validationErrors));
                } else {
                    trailer.setStatusDesc(java.util.Arrays.asList("Business processing failed"));
                }
            }

            responseRecord.put("Trailer", trailer);

            // Use Avro's built-in conversion to create ResponseMessage from GenericRecord
            // This is the proper way to handle ANY schema at runtime
            org.apache.avro.specific.SpecificData specificData = org.apache.avro.specific.SpecificData.get();
            ResponseMessage response = (ResponseMessage) specificData.deepCopy(ResponseMessage.getClassSchema(), responseRecord);

            logger.debug("Created ResponseMessage preserving original structure for transaction: {} with status: {}", 
                        transactionId, isSuccess ? "SUCCESS" : "FAILED");

            return response;

        } catch (Exception e) {
            logger.error("Error creating ResponseMessage from original message for transaction: {} - Error: {}", 
                        transactionId, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Create ResponseMessage with Trailer object from GenericRecord
     * This method now uses the proper Avro conversion approach
     */
    private ResponseMessage createResponseMessageFromAvro(org.apache.avro.generic.GenericRecord avroMessage, 
                                                        String transactionId, 
                                                        boolean isSuccess, 
                                                        String[] validationErrors) {
        // Use the new approach that leverages Avro's built-in conversion
        return createResponseMessageFromOriginal(avroMessage, transactionId, isSuccess, validationErrors);
    }
    
    @Override
    public BusinessProcessingResult createFailureResponse(org.apache.avro.generic.GenericRecord avroMessage, 
                                                         String transactionId, 
                                                         java.util.List<String> validationErrors) {
        try {
            logger.debug("Creating failure response for transaction: {} with {} validation errors", 
                        transactionId, validationErrors.size());
            
            // Convert List<String> to String[] for the existing method
            String[] errorsArray = validationErrors.toArray(new String[0]);
            ResponseMessage response = createResponseMessageFromOriginal(avroMessage, transactionId, false, errorsArray);
            
            if (response != null) {
                logger.info("Created failure response for transaction: {} with validation errors", transactionId);
                return BusinessProcessingResult.success(response);
            } else {
                logger.error("Failed to create failure response for transaction: {}", transactionId);
                return BusinessProcessingResult.failed("Failed to create failure response");
            }
            
        } catch (Exception e) {
            logger.error("Error creating failure response for transaction: {} - Error: {}", 
                        transactionId, e.getMessage(), e);
            return BusinessProcessingResult.failed("Error creating failure response: " + e.getMessage());
        }
    }
    
    @Override
    public BusinessProcessingResult createSuccessResponse(org.apache.avro.generic.GenericRecord avroMessage, 
                                                         String transactionId) {
        try {
            logger.debug("Creating success response for transaction: {}", transactionId);
            
            ResponseMessage response = createResponseMessageFromOriginal(avroMessage, transactionId, true, null);
            
            if (response != null) {
                logger.info("Created success response for transaction: {}", transactionId);
                return BusinessProcessingResult.success(response);
            } else {
                logger.error("Failed to create success response for transaction: {}", transactionId);
                return BusinessProcessingResult.failed("Failed to create success response");
            }
            
        } catch (Exception e) {
            logger.error("Error creating success response for transaction: {} - Error: {}", 
                        transactionId, e.getMessage(), e);
            return BusinessProcessingResult.failed("Error creating success response: " + e.getMessage());
        }
    }
}