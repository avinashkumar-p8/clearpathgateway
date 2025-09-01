package com.anz.fastpayment.inward.consumer;

import com.anz.fastpayment.inward.avro.ProcessedTransactionMessage;
import com.anz.fastpayment.inward.scheme.validation.service.MessageIdempotencyService;
import com.anz.fastpayment.inward.service.ClearingProcessorService;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
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
 * Enhanced Kafka consumer with MUID-based idempotency
 * Ensures exactly-once processing of transaction messages
 */
@Component
public class IdempotentTransactionConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(IdempotentTransactionConsumer.class);
    
    private final MessageIdempotencyService idempotencyService;
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
    private final AtomicLong processedMessageCounter = new AtomicLong(0);
    private final AtomicLong duplicateMessageCounter = new AtomicLong(0);
    private final AtomicLong errorMessageCounter = new AtomicLong(0);
    
    @Autowired
    public IdempotentTransactionConsumer(MessageIdempotencyService idempotencyService, 
                                       ClearingProcessorService clearingProcessorService) {
        this.idempotencyService = idempotencyService;
        this.clearingProcessorService = clearingProcessorService;
    }
    
    /**
     * Main consumer method with idempotency enforcement
     */
    // DISABLED: This consumer conflicts with EnhancedSchemeValidationConsumer
    // @KafkaListener(topics = "#{inputTopic}")
    @Transactional
    public void consumeTransaction(ConsumerRecord<String, GenericRecord> consumerRecord, 
                                 Acknowledgment acknowledgment) {
        
        String transactionId = consumerRecord.key();
        String topic = consumerRecord.topic();
        Integer partition = consumerRecord.partition();
        Long offset = consumerRecord.offset();
        
        try {
            // Extract MUID from headers or generate fallback
            String muid = extractMuid(consumerRecord, transactionId);
            
            logger.info("Processing message: MUID={}, Topic={}, Partition={}, Offset={}", 
                       muid, topic, partition, offset);
            
            // Check idempotency BEFORE processing
            boolean isNewMessage = idempotencyService.isMessageNewAndRegister(
                muid, topic, partition, offset, consumerRecord.value().toString());
            
            if (!isNewMessage) {
                // Duplicate detected - skip processing
                duplicateMessageCounter.incrementAndGet();
                idempotencyService.updateProcessingStatus(muid, "DUPLICATE_SKIPPED");
                logger.info("Duplicate message detected for MUID {}, skipping processing", muid);
                acknowledgment.acknowledge();
                return;
            }
            
            // Process the message (only if new)
            ProcessedTransactionMessage result = clearingProcessorService.processAvroTransaction(consumerRecord.value());
            
            // Update processing status
            idempotencyService.updateProcessingStatus(muid, "COMPLETED");
            
            // Send to output topic
            sendToOutputTopic(transactionId, result);
            
            // Update metrics
            processedMessageCounter.incrementAndGet();
            
            logger.info("Successfully processed message: MUID={}, TransactionId={}", muid, transactionId);
            
        } catch (Exception e) {
            // Error handling
            errorMessageCounter.incrementAndGet();
            logger.error("Error processing message: Topic={}, Partition={}, Offset={}, Error={}", 
                        topic, partition, offset, e.getMessage(), e);
            
            // Send to Dead Letter Queue
            sendToDeadLetterQueue(consumerRecord, e.getMessage());
            
            // Update processing status if possible
            try {
                String muid = extractMuid(consumerRecord, transactionId);
                if (muid != null) {
                    idempotencyService.updateProcessingStatus(muid, "FAILED");
                }
            } catch (Exception statusUpdateError) {
                logger.warn("Failed to update processing status: {}", statusUpdateError.getMessage());
            }
        } finally {
            // Always acknowledge the message
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * Extract MUID from Kafka headers or generate fallback
     */
    private String extractMuid(ConsumerRecord<String, GenericRecord> consumerRecord, String transactionId) {
        // Priority 1: MUID from Kafka headers
        if (consumerRecord.headers() != null) {
            var muidHeader = consumerRecord.headers().lastHeader("muid");
            if (muidHeader != null) {
                String muid = new String(muidHeader.value(), StandardCharsets.UTF_8).trim();
                if (!muid.isEmpty()) {
                    return muid;
                }
            }
        }
        
        // Priority 2: Fallback to transaction ID
        if (transactionId != null && !transactionId.trim().isEmpty()) {
            return transactionId;
        }
        
        // Priority 3: Generate fallback MUID
        return "UNKNOWN-" + System.currentTimeMillis() + "-" + consumerRecord.partition() + "-" + consumerRecord.offset();
    }
    
    /**
     * Send processed message to output topic
     */
    private void sendToOutputTopic(String transactionId, ProcessedTransactionMessage result) {
        try {
            ProducerRecord<String, ProcessedTransactionMessage> record = 
                new ProducerRecord<>(outputTopic, transactionId, result);
            
            // Add safe headers
            addSafeHeader(record, "processed_at", String.valueOf(System.currentTimeMillis()));
            addSafeHeader(record, "source_service", "fast-inward-clearing-processor");
            
            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send message to output topic: {}", exception.getMessage(), exception);
                } else {
                    logger.debug("Message sent to output topic: Topic={}, Partition={}, Offset={}", 
                               metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
            
        } catch (Exception e) {
            logger.error("Error sending message to output topic: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Send failed message to Dead Letter Queue
     */
    private void sendToDeadLetterQueue(ConsumerRecord<String, GenericRecord> consumerRecord, String errorMessage) {
        try {
            // Create DLQ record with error information
            String dlqKey = consumerRecord.key() != null ? consumerRecord.key() : "UNKNOWN";
            String dlqValue = String.format("{\"error\": \"%s\", \"original_message\": \"%s\"}", 
                                          errorMessage, consumerRecord.value().toString());
            
            ProducerRecord<String, String> dlqRecord = 
                new ProducerRecord<>(dlqTopic, dlqKey, dlqValue);
            
            // Add error context headers
            addSafeHeader(dlqRecord, "error_timestamp", String.valueOf(System.currentTimeMillis()));
            addSafeHeader(dlqRecord, "error_source", "fast-inward-clearing-processor");
            addSafeHeader(dlqRecord, "original_topic", consumerRecord.topic());
            addSafeHeader(dlqRecord, "original_partition", String.valueOf(consumerRecord.partition()));
            addSafeHeader(dlqRecord, "original_offset", String.valueOf(consumerRecord.offset()));
            
            dlqProducer.send(dlqRecord, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send message to DLQ: {}", exception.getMessage(), exception);
                } else {
                    logger.info("Message sent to DLQ: Topic={}, Partition={}, Offset={}", 
                              metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
            
        } catch (Exception e) {
            logger.error("Error sending message to DLQ: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Add safe header to producer record
     */
    private void addSafeHeader(ProducerRecord<?, ?> record, String key, String value) {
        try {
            Header header = new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
            record.headers().add(header);
        } catch (Exception e) {
            logger.warn("Failed to add header {}={}: {}", key, value, e.getMessage());
        }
    }
    
    /**
     * Get processing metrics
     */
    public Metrics getMetrics() {
        return new Metrics(
            processedMessageCounter.get(),
            duplicateMessageCounter.get(),
            errorMessageCounter.get()
        );
    }
    
    /**
     * Metrics data class
     */
    public static class Metrics {
        private final long processedMessages;
        private final long duplicateMessages;
        private final long errorMessages;
        
        public Metrics(long processedMessages, long duplicateMessages, long errorMessages) {
            this.processedMessages = processedMessages;
            this.duplicateMessages = duplicateMessages;
            this.errorMessages = errorMessages;
        }
        
        // Getters
        public long getProcessedMessages() { return processedMessages; }
        public long getDuplicateMessages() { return duplicateMessages; }
        public long getErrorMessages() { return errorMessages; }
        
        @Override
        public String toString() {
            return String.format("Metrics{processed=%d, duplicates=%d, errors=%d}", 
                               processedMessages, duplicateMessages, errorMessages);
        }
    }
}
