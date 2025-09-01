package com.anz.fastpayment.inward.consumer;

import com.anz.fastpayment.inward.handler.TransactionProcessingHandler;
import com.anz.fastpayment.inward.model.ProcessingResult;
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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unified Transaction Consumer
 * Single consumer that handles all transaction processing through the pipeline
 * 
 * NOTE: This consumer is currently DISABLED for Phase 1
 * It will be enabled in Phase 3 after all handlers are implemented
 */
@Component
public class UnifiedTransactionConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(UnifiedTransactionConsumer.class);
    
    private final TransactionProcessingHandler processingHandler;
    
    @Value("${kafka.output.topic:fast-outward-clearing}")
    private String outputTopic;
    
    @Value("${kafka.dlq.topic:fast-inward-clearing-dlq}")
    private String dlqTopic;
    
    @Autowired
    private KafkaProducer<String, com.anz.fastpayment.inward.avro.ProcessedTransactionMessage> kafkaProducer;
    
    @Autowired
    private KafkaProducer<String, String> dlqProducer;
    
    // Metrics counters
    private final AtomicLong messagesReceived = new AtomicLong(0);
    private final AtomicLong messagesProcessed = new AtomicLong(0);
    private final AtomicLong messagesFailed = new AtomicLong(0);
    private final AtomicLong duplicateMessages = new AtomicLong(0);
    private final AtomicLong validationFailures = new AtomicLong(0);
    private final AtomicLong businessProcessingFailures = new AtomicLong(0);
    private final AtomicLong systemErrors = new AtomicLong(0);
    
    public UnifiedTransactionConsumer(TransactionProcessingHandler processingHandler) {
        this.processingHandler = processingHandler;
    }
    
    /**
     * Main consumer method for processing transaction messages
     * 
     * ENABLED: This is the new unified consumer that replaces all other consumers
     * Uses the complete handler pipeline for processing
     */
    @KafkaListener(
        topics = "#{inputTopic}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeTransaction(ConsumerRecord<String, GenericRecord> consumerRecord,
                                 Acknowledgment acknowledgment) {
        
        String transactionId = consumerRecord.key();
        long messageNumber = messagesReceived.incrementAndGet();
        
        logger.info("Processing message #{} - Transaction: {} from partition: {}, offset: {}", 
                   messageNumber, transactionId, consumerRecord.partition(), consumerRecord.offset());
        
        try {
            // Process through the unified pipeline
            ProcessingResult result = processingHandler.handleTransaction(consumerRecord);
            
            // Handle the result
            if (result.isSuccess()) {
                // Send to output topic
                sendToOutputTopic(result);
                messagesProcessed.incrementAndGet();
                logger.info("Message #{} processed successfully - Transaction: {}", messageNumber, transactionId);
            } else {
                // Handle different failure types
                handleProcessingFailure(result, transactionId, messageNumber);
            }
            
        } catch (Exception e) {
            // Handle unexpected errors
            systemErrors.incrementAndGet();
            logger.error("Unexpected error processing message #{} - Transaction: {} - Error: {}", 
                        messageNumber, transactionId, e.getMessage(), e);
            
            // Send to DLQ
            sendToDeadLetterQueue(transactionId, "System error: " + e.getMessage(), "UNKNOWN");
        } finally {
            // Always acknowledge the message
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * Handle processing failures based on result type
     */
    private void handleProcessingFailure(ProcessingResult result, String transactionId, long messageNumber) {
        messagesFailed.incrementAndGet();
        
        switch (result.getStatus()) {
            case DUPLICATE:
                duplicateMessages.incrementAndGet();
                logger.info("Duplicate message detected for Transaction: {} - Message #{}", transactionId, messageNumber);
                break;
                
            case VALIDATION_FAILED:
                validationFailures.incrementAndGet();
                logger.warn("Validation failed for Transaction: {} - Message #{} - Errors: {}", 
                           transactionId, messageNumber, result.getValidationErrors());
                sendToDeadLetterQueue(transactionId, "Validation failed: " + result.getValidationErrors(), result.getMuid());
                break;
                
            case BUSINESS_PROCESSING_FAILED:
                businessProcessingFailures.incrementAndGet();
                logger.warn("Business processing failed for Transaction: {} - Message #{} - Error: {}", 
                           transactionId, messageNumber, result.getErrorMessage());
                sendToDeadLetterQueue(transactionId, "Business processing failed: " + result.getErrorMessage(), result.getMuid());
                break;
                
            case PARSING_FAILED:
                logger.warn("Parsing failed for Transaction: {} - Message #{} - Error: {}", 
                           transactionId, messageNumber, result.getErrorMessage());
                sendToDeadLetterQueue(transactionId, "Parsing failed: " + result.getErrorMessage(), result.getMuid());
                break;
                
            case SYSTEM_ERROR:
                systemErrors.incrementAndGet();
                logger.error("System error for Transaction: {} - Message #{} - Error: {}", 
                           transactionId, messageNumber, result.getErrorMessage());
                sendToDeadLetterQueue(transactionId, "System error: " + result.getErrorMessage(), result.getMuid());
                break;
                
            default:
                logger.warn("Unknown processing result for Transaction: {} - Message #{} - Status: {}", 
                           transactionId, messageNumber, result.getStatus());
                sendToDeadLetterQueue(transactionId, "Unknown processing result: " + result.getStatus(), result.getMuid());
        }
    }
    
    /**
     * Send processed message to output topic
     */
    private void sendToOutputTopic(ProcessingResult result) {
        try {
            if (result.getProcessedMessage() == null) {
                logger.error("Cannot send null processed message to output topic");
                return;
            }
            
            ProducerRecord<String, com.anz.fastpayment.inward.avro.ProcessedTransactionMessage> record = 
                new ProducerRecord<>(outputTopic, result.getProcessedMessage().getTransactionId(), result.getProcessedMessage());
            
            // Add headers
            addSafeHeader(record, "processed_at", String.valueOf(System.currentTimeMillis()));
            addSafeHeader(record, "source_service", "unified-transaction-consumer");
            addSafeHeader(record, "processing_duration_ms", String.valueOf(result.getProcessingDurationMs()));
            
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send message to output topic: {}", exception.getMessage(), exception);
                } else {
                    logger.debug("Message sent to output topic: Topic={}, Partition={}, Offset={}", 
                               metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
            
        } catch (Exception e) {
            logger.error("Error sending message to output topic - Error: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Send failed message to Dead Letter Queue
     */
    private void sendToDeadLetterQueue(String transactionId, String errorMessage, String muid) {
        try {
            String dlqMessage = String.format("Transaction: %s, MUID: %s, Error: %s", 
                                            transactionId, muid, errorMessage);
            
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(dlqTopic, transactionId, dlqMessage);
            
            // Add error context headers
            addSafeHeader(record, "error_timestamp", String.valueOf(System.currentTimeMillis()));
            addSafeHeader(record, "error_source", "unified-transaction-consumer");
            addSafeHeader(record, "muid", muid);
            
            dlqProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send message to DLQ: {}", exception.getMessage(), exception);
                } else {
                    logger.debug("Message sent to DLQ: Topic={}, Partition={}, Offset={}", 
                               metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
            
        } catch (Exception e) {
            logger.error("Error sending message to DLQ - Transaction: {} - Error: {}", 
                        transactionId, e.getMessage(), e);
        }
    }
    
    /**
     * Add safe header to producer record
     */
    private void addSafeHeader(ProducerRecord<?, ?> record, String key, String value) {
        try {
            if (value != null) {
                record.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            logger.warn("Failed to add header {}={}: {}", key, value, e.getMessage());
        }
    }
    
    /**
     * Get consumer metrics
     */
    public java.util.Map<String, Long> getMetrics() {
        return java.util.Map.of(
            "messagesReceived", messagesReceived.get(),
            "messagesProcessed", messagesProcessed.get(),
            "messagesFailed", messagesFailed.get(),
            "duplicateMessages", duplicateMessages.get(),
            "validationFailures", validationFailures.get(),
            "businessProcessingFailures", businessProcessingFailures.get(),
            "systemErrors", systemErrors.get()
        );
    }
    
    /**
     * Reset consumer metrics
     */
    public void resetMetrics() {
        messagesReceived.set(0);
        messagesProcessed.set(0);
        messagesFailed.set(0);
        duplicateMessages.set(0);
        validationFailures.set(0);
        businessProcessingFailures.set(0);
        systemErrors.set(0);
        logger.info("Consumer metrics reset");
    }
    
    // Getters for metrics (required by health controller)
    public long getMessagesReceived() { return messagesReceived.get(); }
    public long getMessagesProcessed() { return messagesProcessed.get(); }
    public long getDuplicateMessages() { return duplicateMessages.get(); }
    public long getValidationFailures() { return validationFailures.get(); }
    public long getBusinessProcessingFailures() { return businessProcessingFailures.get(); }
    public long getSystemErrors() { return systemErrors.get(); }
}
