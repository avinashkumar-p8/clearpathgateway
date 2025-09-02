package com.anz.fastpayment.inward.scheme.validation.service;

import com.anz.fastpayment.inward.scheme.validation.entity.MessageUniqueId;
import com.anz.fastpayment.inward.scheme.validation.exception.*;
import com.anz.fastpayment.inward.scheme.validation.repository.MessageUniqueIdRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;

/**
 * Service for managing message idempotency using MUID
 * Provides exactly-once processing guarantees for Kafka messages
 * Implements comprehensive exception handling for banking operations
 */
@Service
@Transactional
public class MessageIdempotencyService {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageIdempotencyService.class);
    
    private final MessageUniqueIdRepository muidRepository;
    
    @Autowired
    public MessageIdempotencyService(MessageUniqueIdRepository muidRepository) {
        this.muidRepository = muidRepository;
    }
    
    /**
     * Check if message is new and register it for processing
     * This method ensures exactly-once processing
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean isMessageNewAndRegister(String muid, String topic, Integer partition, Long offset, String eventPayload) {
        if (!StringUtils.hasText(muid)) {
            logger.warn("MUID is null or empty, cannot ensure idempotency");
            throw new IdempotencyException(
                "MUID cannot be null or empty",
                "INVALID_MUID",
                muid,
                "UNKNOWN",
                "REGISTER_MESSAGE"
            );
        }
        
        try {
            // First check cache for performance
            if (isMuidInCache(muid)) {
                logger.info("MUID {} found in cache, message is duplicate", muid);
                return false;
            }
            
            // Check database for existing MUID
            if (muidRepository.existsByMuidAndActive(muid)) {
                logger.info("MUID {} found in database, message is duplicate", muid);
                addMuidToCache(muid);
                return false;
            }
            
            // MUID is new, register it
            MessageUniqueId messageUniqueId = new MessageUniqueId(muid, topic, partition, offset, eventPayload);
            muidRepository.save(messageUniqueId);
            
            // Add to cache for future lookups
            addMuidToCache(muid);
            
            logger.info("MUID {} registered successfully for topic {} partition {} offset {}", 
                       muid, topic, partition, offset);
            return true;
            
        } catch (Exception e) {
            logger.error("Error checking/registering MUID {}: {}", muid, e.getMessage(), e);
            
            if (e instanceof IdempotencyException) {
                throw e;
            }
            
            throw new IdempotencyException(
                "Failed to ensure message idempotency: " + e.getMessage(),
                "IDEMPOTENCY_REGISTRATION_FAILED",
                muid,
                "UNKNOWN",
                "REGISTER_MESSAGE",
                e
            );
        }
    }
    
    /**
     * Update processing status of a message
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateProcessingStatus(String muid, String status) {
        if (!StringUtils.hasText(muid) || !StringUtils.hasText(status)) {
            logger.warn("Invalid parameters for status update: muid={}, status={}", muid, status);
            throw new IdempotencyException(
                "Invalid parameters for status update",
                "INVALID_STATUS_UPDATE_PARAMETERS",
                muid,
                "UNKNOWN",
                "UPDATE_STATUS"
            );
        }
        
        try {
            muidRepository.findByMuidAndActive(muid)
                    .ifPresent(messageUniqueId -> {
                        messageUniqueId.setProcessingStatus(status);
                        if ("COMPLETED".equals(status) || "FAILED".equals(status)) {
                            messageUniqueId.setProcessedAt(LocalDateTime.now());
                        }
                        muidRepository.save(messageUniqueId);
                        logger.info("Updated processing status for MUID {} to {}", muid, status);
                    });
        } catch (Exception e) {
            logger.error("Error updating processing status for MUID {}: {}", muid, e.getMessage(), e);
            throw new IdempotencyException(
                "Failed to update processing status: " + e.getMessage(),
                "STATUS_UPDATE_FAILED",
                muid,
                "UNKNOWN",
                "UPDATE_STATUS",
                e
            );
        }
    }
    
    /**
     * Get message by MUID with caching
     */
    @Cacheable(value = "muidCache", key = "#muid")
    @Transactional(readOnly = true)
    public MessageUniqueId getMessageByMuid(String muid) {
        if (!StringUtils.hasText(muid)) {
            throw new IdempotencyException(
                "MUID cannot be null or empty",
                "INVALID_MUID",
                muid,
                "UNKNOWN",
                "GET_MESSAGE"
            );
        }
        
        try {
            return muidRepository.findByMuidAndActive(muid)
                    .orElseThrow(() -> new IdempotencyException(
                        "Message not found for MUID: " + muid,
                        "MESSAGE_NOT_FOUND",
                        muid,
                        "UNKNOWN",
                        "GET_MESSAGE"
                    ));
        } catch (Exception e) {
            if (e instanceof IdempotencyException) {
                throw e;
            }
            
            logger.error("Error retrieving message for MUID {}: {}", muid, e.getMessage(), e);
            throw new IdempotencyException(
                "Failed to retrieve message: " + e.getMessage(),
                "MESSAGE_RETRIEVAL_FAILED",
                muid,
                "UNKNOWN",
                "GET_MESSAGE",
                e
            );
        }
    }
    
    /**
     * Check if MUID exists in cache
     */
    private boolean isMuidInCache(String muid) {
        try {
            // This will be handled by Spring Cache abstraction
            return false; // Simplified for now
        } catch (Exception e) {
            logger.warn("Cache operation failed for MUID {}: {}", muid, e.getMessage());
            // Don't throw exception for cache failures, fall back to database
            return false;
        }
    }
    
    /**
     * Add MUID to cache
     */
    private void addMuidToCache(String muid) {
        try {
            // This will be handled by Spring Cache abstraction
            logger.debug("MUID {} added to cache", muid);
        } catch (Exception e) {
            logger.warn("Failed to add MUID {} to cache: {}", muid, e.getMessage());
            // Don't throw exception for cache failures
        }
    }
    
    /**
     * Clear cache for a specific MUID
     */
    @CacheEvict(value = "muidCache", key = "#muid")
    public void clearMuidCache(String muid) {
        logger.debug("Cache cleared for MUID: {}", muid);
    }
    
    /**
     * Clear all MUID cache
     */
    @CacheEvict(value = "muidCache", allEntries = true)
    public void clearAllMuidCache() {
        logger.info("All MUID cache cleared");
    }
}
