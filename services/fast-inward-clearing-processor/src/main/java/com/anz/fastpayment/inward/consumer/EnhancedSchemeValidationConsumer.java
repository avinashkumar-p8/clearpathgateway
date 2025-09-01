package com.anz.fastpayment.inward.consumer;

import com.anz.fastpayment.inward.avro.ProcessedTransactionMessage;
import com.anz.fastpayment.inward.scheme.validation.service.MessageIdempotencyService;
import com.anz.fastpayment.inward.scheme.validation.service.SchemeValidationOrchestrator;
import com.anz.fastpayment.inward.scheme.validation.util.AvroMessageParser;
import com.anz.fastpayment.inward.util.JsonFlattener;
import com.anz.fastpayment.inward.scheme.validation.model.ValidationResult;
import com.anz.fastpayment.inward.service.ClearingProcessorService;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enhanced Scheme Validation Consumer for Banking Operations
 * Integrates idempotency, Avro parsing, and parallel validation
 * Uses existing infrastructure and follows banking software best practices
 */
@Component
public class EnhancedSchemeValidationConsumer {
    
    private static final Logger log = LoggerFactory.getLogger(EnhancedSchemeValidationConsumer.class);
    
    private final MessageIdempotencyService idempotencyService;
    private final SchemeValidationOrchestrator validationOrchestrator;
    private final ClearingProcessorService clearingProcessorService;
    
    @Value("${kafka.output.topic:fast-outward-clearing}")
    private String outputTopic;
    
    @Value("${kafka.dlq.topic:fast-inward-clearing-dlq}")
    private String dlqTopic;
    
    @Autowired
    private KafkaProducer<String, ProcessedTransactionMessage> kafkaProducer;
    
    @Autowired
    private KafkaProducer<String, String> dlqProducer;
    
    // Metrics counters
    private final AtomicLong messagesReceived = new AtomicLong(0);
    private final AtomicLong messagesProcessed = new AtomicLong(0);
    private final AtomicLong validationErrors = new AtomicLong(0);
    private final AtomicLong duplicateMuidCount = new AtomicLong(0);
    private final AtomicLong cacheHits = new AtomicLong(0);
    private final AtomicLong cacheMisses = new AtomicLong(0);
    
    // In-memory storage for testing duplicate detection
    private final java.util.Set<String> processedMuids = java.util.concurrent.ConcurrentHashMap.newKeySet();
    
    @Autowired
    public EnhancedSchemeValidationConsumer(MessageIdempotencyService idempotencyService,
                                         SchemeValidationOrchestrator validationOrchestrator,
                                         ClearingProcessorService clearingProcessorService) {
        this.idempotencyService = idempotencyService;
        this.validationOrchestrator = validationOrchestrator;
        this.clearingProcessorService = clearingProcessorService;
    }
    
    /**
     * Main consumer method with integrated idempotency and validation
     * 
     * DISABLED: This consumer has been replaced by UnifiedTransactionConsumer
     * to prevent conflicts and enable the new unified architecture
     */
    // @KafkaListener(topics = "#{inputTopic}")
    @Transactional
    public void consumeAndValidateMessage(ConsumerRecord<String, GenericRecord> consumerRecord,
                                        Acknowledgment acknowledgment) {
        
        String transactionId = consumerRecord.key();
        
        try {
            // Step 1: Extract MUID and check idempotency
            String muid = extractMuid(consumerRecord, transactionId);
            
            log.info("Processing message: MUID={}, TransactionId={}", muid, transactionId);
            
            // Check idempotency BEFORE processing
            boolean isNewMessage = idempotencyService.isMessageNewAndRegister(
                muid, consumerRecord.topic(), consumerRecord.partition(), 
                consumerRecord.offset(), consumerRecord.value().toString());
            
            if (!isNewMessage) {
                // Duplicate detected - skip processing
                duplicateMuidCount.incrementAndGet();
                idempotencyService.updateProcessingStatus(muid, "DUPLICATE_SKIPPED");
                log.info("Duplicate message detected for MUID {}, skipping processing", muid);
                acknowledgment.acknowledge();
                return;
            }
            
            // Step 2: Convert only Message section from Avro message to Map for validation
            Map<String, Object> messagePayload = extractMessageSection(consumerRecord.value());
            
            if (messagePayload.isEmpty()) {
                log.warn("Failed to convert Avro message to Map for MUID: {}", muid);
                idempotencyService.updateProcessingStatus(muid, "PARSING_FAILED");
                sendToDLQ(transactionId, "Failed to parse Avro message", muid);
                acknowledgment.acknowledge();
                return;
            }
            
            log.debug("Converted Avro message to Map with {} entries for validation from message with MUID: {}", 
                messagePayload.size(), muid);
            
            // Step 3-4: Perform parallel validation using existing framework
            ValidationResult validationResult = validationOrchestrator.validateMessage(messagePayload, transactionId);
            
            // Step 6: Collect and process validation results
            if (!validationResult.isOverallSuccess()) {
                validationErrors.incrementAndGet();
                idempotencyService.updateProcessingStatus(muid, "VALIDATION_FAILED");
                
                log.warn("Validation failed for MUID: {}. Failed validations: {}", 
                    muid, validationResult.getFailedValidations().size());
                
                // Send to DLQ with validation details
                String validationError = buildValidationErrorMessage(validationResult);
                sendToDLQ(transactionId, validationError, muid);
                
                acknowledgment.acknowledge();
                return;
            }
            
            // Step 5: Process message if validation passes
            ProcessedTransactionMessage result = clearingProcessorService.processAvroTransaction(consumerRecord.value());
            
            // Update processing status
            idempotencyService.updateProcessingStatus(muid, "COMPLETED");
            
            // Send to output topic
            sendToOutputTopic(transactionId, result);
            
            messagesProcessed.incrementAndGet();
            log.info("Message processing completed successfully for MUID: {}", muid);
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            validationErrors.incrementAndGet();
            log.error("Error processing message for transaction: {}", transactionId, e);
            
            // Send to DLQ
            sendToDLQ(transactionId, "Processing error: " + e.getMessage(), "UNKNOWN");
            
            // Acknowledge to prevent reprocessing
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * Extract only the Message section from the full Avro message
     */
    private Map<String, Object> extractMessageSection(GenericRecord avroMessage) {
        try {
            // Extract only the 'messages' array from the Avro message
            Object messagesArray = avroMessage.get("messages");
            
            if (messagesArray != null && messagesArray instanceof java.util.Collection) {
                java.util.Collection<?> messages = (java.util.Collection<?>) messagesArray;
                
                if (!messages.isEmpty()) {
                    // Extract only the first message (messages[0])
                    Object firstMessage = messages.iterator().next();
                    
                    if (firstMessage instanceof GenericRecord) {
                        log.debug("Successfully extracted first message from messages array");
                        return AvroMessageParser.convertToMap((GenericRecord) firstMessage);
                    } else {
                        log.warn("First message is not a GenericRecord: {}", firstMessage.getClass().getSimpleName());
                        return new HashMap<>();
                    }
                } else {
                    log.warn("Messages array is empty");
                    return new HashMap<>();
                }
            } else {
                log.warn("No 'messages' section found in Avro message or not a collection");
                return new HashMap<>();
            }
        } catch (Exception e) {
            log.error("Error extracting Message section from Avro message", e);
            return new HashMap<>();
        }
    }
    
    /**
     * Extract MUID from message headers or generate fallback
     */
    private String extractMuid(ConsumerRecord<String, GenericRecord> consumerRecord, String transactionId) {
        // Try to extract MUID from headers first
        for (org.apache.kafka.common.header.Header header : consumerRecord.headers()) {
            if ("muid".equalsIgnoreCase(header.key())) {
                return new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
            }
        }
        
        // Fallback: use transaction ID or generate new MUID
        if (transactionId != null && !transactionId.trim().isEmpty()) {
            return "MUID-" + transactionId;
        }
        
        return "MUID-" + System.currentTimeMillis() + "-" + java.util.UUID.randomUUID().toString().substring(0, 8);
    }
    
    /**
     * Build validation error message for DLQ
     */
    private String buildValidationErrorMessage(ValidationResult validationResult) {
        StringBuilder errorMsg = new StringBuilder("Validation failed: ");
        
        validationResult.getFailedValidations().forEach(failure -> {
            errorMsg.append(failure.getTagName())
                   .append("(")
                   .append(failure.getErrorCode())
                   .append("): ")
                   .append(failure.getErrorMessage())
                   .append("; ");
        });
        
        return errorMsg.toString();
    }
    
    /**
     * Send message to output topic
     */
    private void sendToOutputTopic(String transactionId, ProcessedTransactionMessage result) {
        try {
            ProducerRecord<String, ProcessedTransactionMessage> record = 
                new ProducerRecord<>(outputTopic, transactionId, result);
            
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message to output topic: {}", exception.getMessage());
                } else {
                    log.debug("Message sent to output topic: {}", metadata);
                }
            });
            
        } catch (Exception e) {
            log.error("Error sending message to output topic", e);
        }
    }
    
    /**
     * Send message to Dead Letter Queue
     */
    private void sendToDLQ(String transactionId, String errorMessage, String muid) {
        try {
            String dlqMessage = String.format("Transaction: %s, MUID: %s, Error: %s", 
                transactionId, muid, errorMessage);
            
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(dlqTopic, transactionId, dlqMessage);
            
            dlqProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message to DLQ: {}", exception.getMessage());
                } else {
                    log.debug("Message sent to DLQ: {}", metadata);
                }
            });
            
        } catch (Exception e) {
            log.error("Error sending message to DLQ", e);
        }
    }
    
    /**
     * Get consumer metrics
     */
    public Map<String, Long> getMetrics() {
        return Map.of(
            "messagesReceived", messagesReceived.get(),
            "messagesProcessed", messagesProcessed.get(),
            "validationErrors", validationErrors.get(),
            "duplicateMuidCount", duplicateMuidCount.get(),
            "cacheHits", cacheHits.get(),
            "cacheMisses", cacheMisses.get()
        );
    }
    
    /**
     * Process a message with enhanced validation (for testing purposes)
     * 
     * @param muid Message Unique ID
     * @param payload Message payload as HashMap
     * @param schema Avro schema reference
     */
    public void processMessage(String muid, Map<String, Object> payload, String schema) {
        log.info("Processing enhanced validation message with MUID: {} and schema: {}", muid, schema);
        
        try {
            messagesReceived.incrementAndGet();
            
            // Check idempotency first
            if (isDuplicateMessage(muid)) {
                log.info("Duplicate message detected with MUID: {}. Skipping processing.", muid);
                duplicateMuidCount.incrementAndGet();
                return;
            }
            
            // Perform enhanced validation using orchestrator with original payload
            ValidationResult validationResult = validationOrchestrator.validateMessage(payload, muid);
            
            // Store message for idempotency
            storeMessage(muid, payload, schema);
            
            // Update metrics
            messagesProcessed.incrementAndGet();
            
            log.info("Enhanced validation completed for MUID: {}. Success: {}", 
                muid, validationResult.isOverallSuccess());
            
        } catch (Exception e) {
            validationErrors.incrementAndGet();
            log.error("Error processing enhanced validation message with MUID: {}", muid, e);
            throw new RuntimeException("Enhanced validation processing failed", e);
        }
    }
    
    /**
     * Check if message is duplicate (for testing purposes)
     */
    private boolean isDuplicateMessage(String muid) {
        // Simple in-memory check for testing
        // In production, this would use the idempotency service
        boolean isDuplicate = processedMuids.contains(muid);
        if (isDuplicate) {
            log.debug("Duplicate MUID detected: {}", muid);
        }
        return isDuplicate;
    }
    
    /**
     * Store message for idempotency (for testing purposes)
     */
    private void storeMessage(String muid, Map<String, Object> payload, String schema) {
        // Simple in-memory storage for testing
        // In production, this would use the idempotency service
        processedMuids.add(muid);
        log.debug("Storing message with MUID: {} for idempotency", muid);
    }
    
    /**
     * Convert payload for validation using JsonFlattener
     * This flattens nested JSON structures into flat key-value pairs with dot notation
     */
    private Map<String, String> convertPayloadForValidation(Map<String, Object> payload) {
        log.info("=== ORIGINAL PAYLOAD (Map<String, Object>) ===");
        payload.forEach((key, value) -> {
            log.info("Key: '{}' -> Value: '{}' (Type: {})", key, value, value != null ? value.getClass().getSimpleName() : "null");
        });
        
        // Use JsonFlattener to convert nested structure to flat HashMap
        Map<String, String> validationPayload = JsonFlattener.flatten(payload);
        
        log.info("=== FLATTENED VALIDATION PAYLOAD (Map<String, String>) ===");
        log.info("Total flattened keys: {}", validationPayload.size());
        validationPayload.forEach((key, value) -> {
            log.info("Key: '{}' -> Value: '{}'", key, value);
        });
        
        // Log summary of flattened structure
        log.info("=== FLATTENED STRUCTURE SUMMARY ===");
        log.info(JsonFlattener.getSummary(validationPayload));
        
        return validationPayload;
    }
    
    /**
     * Reset duplicate detection for testing
     */
    public void resetDuplicateDetection() {
        processedMuids.clear();
        log.info("Duplicate detection reset for testing");
    }
}
