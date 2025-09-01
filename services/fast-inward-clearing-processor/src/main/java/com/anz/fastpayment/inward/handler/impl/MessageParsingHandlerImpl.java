package com.anz.fastpayment.inward.handler.impl;

import com.anz.fastpayment.inward.handler.MessageParsingHandler;
import com.anz.fastpayment.inward.model.ParsingResult;
import com.anz.fastpayment.inward.model.ProcessingContext;
import com.anz.fastpayment.inward.model.TransactionMessage;
import com.anz.fastpayment.inward.scheme.validation.util.AvroMessageParser;
import com.anz.fastpayment.inward.util.AvroConverter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of MessageParsingHandler
 * Handles message parsing and enrichment operations
 */
@Service
public class MessageParsingHandlerImpl implements MessageParsingHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageParsingHandlerImpl.class);
    
    @Override
    public ParsingResult parseAndEnrich(ProcessingContext context) {
        try {
            logger.debug("Starting message parsing and enrichment for transaction: {}", context.getTransactionId());
            
            // Step 1: Extract message section from Avro message
            Map<String, Object> messagePayload = extractMessageSection(context);
            
            if (messagePayload.isEmpty()) {
                logger.warn("Failed to extract message section from Avro message for transaction: {}", context.getTransactionId());
                return ParsingResult.failed("Failed to extract message section from Avro message");
            }
            
            logger.debug("Extracted message payload with {} entries for transaction: {}", 
                        messagePayload.size(), context.getTransactionId());
            
            // Step 2: Convert Avro message to TransactionMessage
            TransactionMessage transactionMessage = convertToTransactionMessage(context);
            
            if (transactionMessage == null) {
                logger.warn("Failed to convert Avro message to TransactionMessage for transaction: {}", context.getTransactionId());
                return ParsingResult.failed("Failed to convert Avro message to TransactionMessage");
            }
            
            // Step 3: Enrich transaction message with metadata
            enrichWithMetadata(transactionMessage, context);
            
            logger.info("Successfully parsed and enriched message for transaction: {}", context.getTransactionId());
            return ParsingResult.success(messagePayload, transactionMessage);
            
        } catch (Exception e) {
            logger.error("Error during message parsing and enrichment for transaction: {} - Error: {}", 
                        context.getTransactionId(), e.getMessage(), e);
            return ParsingResult.failed("Message parsing failed: " + e.getMessage());
        }
    }
    
    @Override
    public Map<String, Object> extractMessageSection(ProcessingContext context) {
        try {
            GenericRecord avroMessage = context.getAvroMessage();
            
            if (avroMessage == null) {
                logger.warn("Avro message is null for transaction: {}", context.getTransactionId());
                return new HashMap<>();
            }
            
            // Extract only the 'messages' array from the Avro message
            Object messagesArray = avroMessage.get("messages");
            
            if (messagesArray != null && messagesArray instanceof java.util.Collection) {
                java.util.Collection<?> messages = (java.util.Collection<?>) messagesArray;
                
                if (!messages.isEmpty()) {
                    // Extract only the first message (messages[0])
                    Object firstMessage = messages.iterator().next();
                    
                    if (firstMessage instanceof GenericRecord) {
                        logger.debug("Successfully extracted first message from messages array for transaction: {}", 
                                   context.getTransactionId());
                        return AvroMessageParser.convertToMap((GenericRecord) firstMessage);
                    } else {
                        logger.warn("First message is not a GenericRecord: {} for transaction: {}", 
                                  firstMessage.getClass().getSimpleName(), context.getTransactionId());
                        return new HashMap<>();
                    }
                } else {
                    logger.warn("Messages array is empty for transaction: {}", context.getTransactionId());
                    return new HashMap<>();
                }
            } else {
                logger.warn("No 'messages' section found in Avro message or not a collection for transaction: {}", 
                          context.getTransactionId());
                return new HashMap<>();
            }
            
        } catch (Exception e) {
            logger.error("Error extracting Message section from Avro message for transaction: {} - Error: {}", 
                        context.getTransactionId(), e.getMessage(), e);
            return new HashMap<>();
        }
    }
    
    @Override
    public TransactionMessage convertToTransactionMessage(ProcessingContext context) {
        try {
            GenericRecord avroMessage = context.getAvroMessage();
            
            if (avroMessage == null) {
                logger.warn("Avro message is null for transaction: {}", context.getTransactionId());
                return null;
            }
            
            // Use existing AvroConverter to convert to TransactionMessage
            TransactionMessage transactionMessage = AvroConverter.convertToTransactionMessage(avroMessage);
            
            if (transactionMessage != null) {
                logger.debug("Successfully converted Avro message to TransactionMessage for transaction: {}", 
                           context.getTransactionId());
            } else {
                logger.warn("AvroConverter returned null TransactionMessage for transaction: {}", context.getTransactionId());
            }
            
            return transactionMessage;
            
        } catch (Exception e) {
            logger.error("Error converting Avro message to TransactionMessage for transaction: {} - Error: {}", 
                        context.getTransactionId(), e.getMessage(), e);
            return null;
        }
    }
    
    @Override
    public void enrichWithMetadata(TransactionMessage transactionMessage, ProcessingContext context) {
        try {
            if (transactionMessage == null) {
                logger.warn("Cannot enrich null TransactionMessage for transaction: {}", context.getTransactionId());
                return;
            }
            
            // Add basic metadata that exists in TransactionMessage
            if (transactionMessage.getTimestamp() == null) {
                transactionMessage.setTimestamp(LocalDateTime.now());
            }
            
            // Add reference if not present
            if (transactionMessage.getReference() == null || transactionMessage.getReference().trim().isEmpty()) {
                String reference = "REF-" + context.getTransactionId() + "-" + System.currentTimeMillis();
                transactionMessage.setReference(reference);
            }
            
            // Add priority if not present
            if (transactionMessage.getPriority() == null || transactionMessage.getPriority().trim().isEmpty()) {
                transactionMessage.setPriority("NORMAL");
            }
            
            logger.debug("Successfully enriched TransactionMessage with basic metadata for transaction: {}", 
                       context.getTransactionId());
            
        } catch (Exception e) {
            logger.error("Error enriching TransactionMessage with metadata for transaction: {} - Error: {}", 
                        context.getTransactionId(), e.getMessage(), e);
        }
    }
}
