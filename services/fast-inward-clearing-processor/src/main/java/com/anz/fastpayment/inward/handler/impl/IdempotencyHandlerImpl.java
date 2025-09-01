package com.anz.fastpayment.inward.handler.impl;

import com.anz.fastpayment.inward.handler.IdempotencyHandler;
import com.anz.fastpayment.inward.model.IdempotencyResult;
import com.anz.fastpayment.inward.model.ProcessingContext;
import com.anz.fastpayment.inward.scheme.validation.service.MessageIdempotencyService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

/**
 * Implementation of IdempotencyHandler
 * Handles idempotency operations using existing MessageIdempotencyService
 */
@Service
public class IdempotencyHandlerImpl implements IdempotencyHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(IdempotencyHandlerImpl.class);
    
    private final MessageIdempotencyService idempotencyService;
    
    public IdempotencyHandlerImpl(MessageIdempotencyService idempotencyService) {
        this.idempotencyService = idempotencyService;
    }
    
    @Override
    public IdempotencyResult checkAndRegister(ProcessingContext context) {
        try {
            String muid = extractMuid(context);
            
            logger.debug("Checking idempotency for MUID: {} in transaction: {}", muid, context.getTransactionId());
            
            // Check if message is new and register it
            boolean isNewMessage = idempotencyService.isMessageNewAndRegister(
                muid,
                context.getTopic(),
                context.getPartition(),
                context.getOffset(),
                context.getPayloadAsString()
            );
            
            if (isNewMessage) {
                logger.info("New message registered for MUID: {} in transaction: {}", muid, context.getTransactionId());
                return IdempotencyResult.newMessage(muid);
            } else {
                logger.info("Duplicate message detected for MUID: {} in transaction: {}", muid, context.getTransactionId());
                return IdempotencyResult.duplicateMessage(muid);
            }
            
        } catch (Exception e) {
            logger.error("Error during idempotency check for transaction: {} - Error: {}", 
                        context.getTransactionId(), e.getMessage(), e);
            return IdempotencyResult.error("UNKNOWN", "Idempotency check failed: " + e.getMessage());
        }
    }
    
    @Override
    public void updateProcessingStatus(String muid, String status) {
        try {
            if (muid == null || muid.trim().isEmpty()) {
                logger.warn("Cannot update processing status: MUID is null or empty");
                return;
            }
            
            if (status == null || status.trim().isEmpty()) {
                logger.warn("Cannot update processing status: Status is null or empty for MUID: {}", muid);
                return;
            }
            
            idempotencyService.updateProcessingStatus(muid, status);
            logger.debug("Updated processing status for MUID: {} to status: {}", muid, status);
            
        } catch (Exception e) {
            logger.error("Error updating processing status for MUID: {} to status: {} - Error: {}", 
                        muid, status, e.getMessage(), e);
        }
    }
    
    @Override
    public String extractMuid(ProcessingContext context) {
        try {
            ConsumerRecord<String, ?> consumerRecord = context.getConsumerRecord();
            
            // Priority 1: Extract MUID from Kafka headers
            if (consumerRecord.headers() != null) {
                Header muidHeader = consumerRecord.headers().lastHeader("muid");
                if (muidHeader != null) {
                    String muid = new String(muidHeader.value(), StandardCharsets.UTF_8).trim();
                    if (!muid.isEmpty()) {
                        logger.debug("Extracted MUID from headers: {} for transaction: {}", muid, context.getTransactionId());
                        return muid;
                    }
                }
            }
            
            // Priority 2: Use transaction ID as fallback
            String transactionId = context.getTransactionId();
            if (transactionId != null && !transactionId.trim().isEmpty()) {
                String fallbackMuid = "MUID-" + transactionId;
                logger.debug("Using transaction ID as MUID fallback: {} for transaction: {}", fallbackMuid, transactionId);
                return fallbackMuid;
            }
            
            // Priority 3: Generate fallback MUID
            String generatedMuid = "MUID-" + System.currentTimeMillis() + "-" + 
                                 context.getPartition() + "-" + context.getOffset();
            logger.debug("Generated fallback MUID: {} for transaction: {}", generatedMuid, context.getTransactionId());
            return generatedMuid;
            
        } catch (Exception e) {
            logger.error("Error extracting MUID for transaction: {} - Error: {}", 
                        context.getTransactionId(), e.getMessage(), e);
            
            // Final fallback
            return "MUID-ERROR-" + System.currentTimeMillis();
        }
    }
}
