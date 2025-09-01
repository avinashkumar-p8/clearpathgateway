package com.anz.fastpayment.inward.consumer;

import com.anz.fastpayment.inward.avro.ProcessedTransactionMessage;
import com.anz.fastpayment.inward.service.ClearingProcessorService;
import com.anz.fastpayment.inward.util.AvroConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transaction Consumer with Avro Support
 * Consumes Avro transaction messages from input topic, processes them, and produces Avro results
 */
@Component
public class TransactionConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);
    private static final AtomicLong messageCounter = new AtomicLong(0);
    
    // Metrics for monitoring
    private static final AtomicLong processedMessageCounter = new AtomicLong(0);
    private static final AtomicLong errorMessageCounter = new AtomicLong(0);
    private static final AtomicLong nullMessageCounter = new AtomicLong(0);
    private static final AtomicLong invalidTypeCounter = new AtomicLong(0);
    private static final AtomicLong dlqSendFailureCounter = new AtomicLong(0);
    private static final AtomicLong outputSendFailureCounter = new AtomicLong(0);

    @Autowired
    private ClearingProcessorService clearingProcessorService;

    @Autowired
    private KafkaTemplate<String, GenericRecord> kafkaTemplate;

    @Autowired
    @Qualifier("outputTopic")
    private String outputTopic;

    @Autowired
    @Qualifier("dlqTopic")
    private String dlqTopic;

    /**
     * Kafka Listener for consuming Avro transaction messages
     * 
     * @param consumerRecord The consumed Kafka record with Avro message
     * @param acknowledgment The acknowledgment for manual commit
     */
    // DISABLED: This consumer conflicts with EnhancedSchemeValidationConsumer
    // @KafkaListener(
    //     topics = "#{inputTopic}",
    //     groupId = "${spring.kafka.consumer.group-id}",
    //     containerFactory = "kafkaListenerContainerFactory"
    // )
    public void consumeTransaction(
            @Payload ConsumerRecord<String, GenericRecord> consumerRecord,
            Acknowledgment acknowledgment) {
        
        String transactionId = consumerRecord.key();
        GenericRecord avroMessage = consumerRecord.value();
        long messageNumber = messageCounter.incrementAndGet();
        
        logger.info("Consuming Avro message #{} - Transaction: {} from partition: {}, offset: {}", 
            messageNumber, transactionId, consumerRecord.partition(), consumerRecord.offset());

        try {
            // Null check for avroMessage
            if (avroMessage == null) {
                nullMessageCounter.incrementAndGet();
                logger.error("Received null Avro message - Transaction: {}, Message #{}", transactionId, messageNumber);
                // Acknowledge to prevent reprocessing of null messages
                acknowledgment.acknowledge();
                return;
            }

            // Process the Avro transaction message
            ProcessedTransactionMessage processedMessage = clearingProcessorService.processAvroTransaction(avroMessage);
            
            // Null check for processedMessage
            if (processedMessage == null) {
                nullMessageCounter.incrementAndGet();
                logger.error("Processing returned null result - Transaction: {}, Message #{}", transactionId, messageNumber);
                // Send to DLQ and acknowledge
                sendToDeadLetterQueue(transactionId, avroMessage, new RuntimeException("Processing returned null result"));
                acknowledgment.acknowledge();
                return;
            }

            // Validate that processedMessage is a GenericRecord before casting
            if (!(processedMessage instanceof GenericRecord)) {
                invalidTypeCounter.incrementAndGet();
                logger.error("Processed message is not a GenericRecord - Transaction: {}, Message #{}, Type: {}", 
                    transactionId, messageNumber, processedMessage.getClass().getSimpleName());
                // Send to DLQ and acknowledge
                sendToDeadLetterQueue(transactionId, avroMessage, 
                    new RuntimeException("Processed message is not a GenericRecord: " + processedMessage.getClass().getSimpleName()));
                acknowledgment.acknowledge();
                return;
            }
            
            // Send to output topic
            sendToOutputTopic(transactionId, processedMessage);
            
            // Acknowledge the message
            acknowledgment.acknowledge();
            
            processedMessageCounter.incrementAndGet();
            logger.info("Avro message #{} processed successfully - Transaction: {}", messageNumber, transactionId);
            
        } catch (Exception e) {
            errorMessageCounter.incrementAndGet();
            logger.error("Error processing Avro message #{} - Transaction: {} - Error: {}", 
                messageNumber, transactionId, e.getMessage(), e);
            
            try {
                // Send to Dead Letter Queue
                sendToDeadLetterQueue(transactionId, avroMessage, e);
            } catch (Exception dlqException) {
                logger.error("Failed to send message to DLQ - Transaction: {}, Message #{}", transactionId, messageNumber, dlqException);
                dlqSendFailureCounter.incrementAndGet();
            }
            
            // Acknowledge the message to prevent reprocessing
            acknowledgment.acknowledge();
        }
    }

    /**
     * Send processed Avro message to output topic
     */
    private void sendToOutputTopic(String transactionId, ProcessedTransactionMessage processedMessage) {
        try {
            // Validate input parameters
            if (processedMessage == null) {
                logger.error("Cannot send null processed message to output topic - Transaction: {}", transactionId);
                return;
            }

            // Safe casting with validation
            if (!(processedMessage instanceof GenericRecord)) {
                logger.error("Processed message is not a GenericRecord - Transaction: {}, Type: {}", 
                    transactionId, processedMessage.getClass().getSimpleName());
                return;
            }

            GenericRecord avroRecord = (GenericRecord) processedMessage;
            
            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(
                outputTopic, 
                transactionId, 
                avroRecord
            );
            
            // Add headers for tracing with null safety
            addSafeHeader(record, "processingNodeId", processedMessage.getProcessingNodeId());
            addSafeHeader(record, "status", processedMessage.getStatus());
            
            // Send synchronously to ensure errors are caught and handled
            var result = kafkaTemplate.send(record).get(30, TimeUnit.SECONDS);
            logger.debug("Avro message sent to output topic successfully - Transaction: {}, Partition: {}, Offset: {}", 
                transactionId, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                
        } catch (ExecutionException e) {
            outputSendFailureCounter.incrementAndGet();
            logger.error("Error sending Avro message to output topic - Transaction: {} - Execution failed: {}", 
                transactionId, e.getMessage(), e);
            // Log the root cause if available
            if (e.getCause() != null) {
                logger.error("Root cause of output send failure for Transaction: {}", transactionId, e.getCause());
            }
        } catch (InterruptedException e) {
            outputSendFailureCounter.incrementAndGet();
            logger.error("Error sending Avro message to output topic - Transaction: {} - Operation interrupted: {}", 
                transactionId, e.getMessage(), e);
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            outputSendFailureCounter.incrementAndGet();
            logger.error("Error sending Avro message to output topic - Transaction: {} - Operation timed out: {}", 
                transactionId, e.getMessage(), e);
        } catch (Exception e) {
            outputSendFailureCounter.incrementAndGet();
            logger.error("Error sending Avro message to output topic - Transaction: {} - Unexpected error: {}", 
                transactionId, e.getMessage(), e);
        }
    }

    /**
     * Send failed Avro message to Dead Letter Queue
     */
    private void sendToDeadLetterQueue(String transactionId, GenericRecord avroMessage, Exception error) {
        try {
            // Validate input parameters
            if (avroMessage == null) {
                logger.error("Cannot send null avroMessage to DLQ - Transaction: {}", transactionId);
                return;
            }

            if (error == null) {
                logger.error("Cannot send message to DLQ with null error - Transaction: {}", transactionId);
                return;
            }

            // Create a simple error message for DLQ
            ProcessedTransactionMessage errorMessage = createAvroErrorMessage(avroMessage, error);
            
            // Validate the error message
            if (errorMessage == null) {
                logger.error("Failed to create error message for DLQ - Transaction: {}", transactionId);
                return;
            }

            // Safe casting with validation
            if (!(errorMessage instanceof GenericRecord)) {
                logger.error("Error message is not a GenericRecord - Transaction: {}, Type: {}", 
                    transactionId, errorMessage.getClass().getSimpleName());
                return;
            }

            GenericRecord avroRecord = (GenericRecord) errorMessage;
            
            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(
                dlqTopic, 
                transactionId, 
                avroRecord
            );
            
            // Add error headers with null safety
            addSafeHeader(record, "error", error.getMessage());
            addSafeHeader(record, "errorType", error.getClass().getSimpleName());
            
            // Send synchronously to ensure errors are caught and handled
            var result = kafkaTemplate.send(record).get(30, TimeUnit.SECONDS);
            logger.info("Avro message sent to DLQ successfully - Transaction: {}, Partition: {}, Offset: {}", 
                transactionId, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                
        } catch (ExecutionException e) {
            dlqSendFailureCounter.incrementAndGet();
            logger.error("Error sending Avro message to DLQ - Transaction: {} - Execution failed: {}", 
                transactionId, e.getMessage(), e);
            // Don't throw here to avoid infinite loop, but log the root cause
            if (e.getCause() != null) {
                logger.error("Root cause of DLQ send failure for Transaction: {}", transactionId, e.getCause());
            }
        } catch (InterruptedException e) {
            dlqSendFailureCounter.incrementAndGet();
            logger.error("Error sending Avro message to DLQ - Transaction: {} - Operation interrupted: {}", 
                transactionId, e.getMessage(), e);
            Thread.currentThread().interrupt();
            // Don't throw here to avoid infinite loop
        } catch (TimeoutException e) {
            dlqSendFailureCounter.incrementAndGet();
            logger.error("Error sending Avro message to DLQ - Transaction: {} - Operation timed out: {}", 
                transactionId, e.getMessage(), e);
            // Don't throw here to avoid infinite loop
        } catch (Exception e) {
            dlqSendFailureCounter.incrementAndGet();
            logger.error("Error sending Avro message to DLQ - Transaction: {} - Unexpected error: {}", 
                transactionId, e.getMessage(), e);
            // Don't throw here to avoid infinite loop
        }
    }

    /**
     * Create Avro error message for DLQ
     */
    private com.anz.fastpayment.inward.avro.ProcessedTransactionMessage createAvroErrorMessage(GenericRecord avroMessage, Exception error) {
        try {
            // Create a simple Avro error message
            com.anz.fastpayment.inward.avro.ProcessedTransactionMessage errorMessage = new com.anz.fastpayment.inward.avro.ProcessedTransactionMessage();
            errorMessage.setStatus("FAILED");
            errorMessage.setProcessingNodeId("DLQ");
            
            // Extract basic information from Avro message if possible
            if (avroMessage != null) {
                try {
                    // Extract transaction ID from Avro message using updated converter
                    String extractedId = AvroConverter.extractTransactionId(avroMessage);
                    if (extractedId != null && !"UNKNOWN".equals(extractedId)) {
                        errorMessage.setTransactionId(extractedId);
                    } else {
                        errorMessage.setTransactionId("UNKNOWN");
                    }
                } catch (Exception e) {
                    logger.warn("Could not extract information from Avro message for error handling", e);
                    errorMessage.setTransactionId("UNKNOWN");
                }
            } else {
                errorMessage.setTransactionId("UNKNOWN");
            }
            
            // Set error details
            if (error != null) {
                errorMessage.setErrorMessage(error.getMessage());
            }
            
            return errorMessage;
        } catch (Exception e) {
            logger.error("Error creating Avro error message", e);
            // Return a basic error message
            com.anz.fastpayment.inward.avro.ProcessedTransactionMessage basicError = new com.anz.fastpayment.inward.avro.ProcessedTransactionMessage();
            basicError.setStatus("FAILED");
            basicError.setProcessingNodeId("DLQ");
            basicError.setTransactionId("UNKNOWN");
            basicError.setErrorMessage("Failed to create error message");
            return basicError;
        }
    }

    /**
     * Helper method to add headers safely, handling null values
     */
    private void addSafeHeader(ProducerRecord<String, GenericRecord> record, String key, Object value) {
        if (value != null) {
            record.headers().add(key, value.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Get current message counter for monitoring
     */
    public long getMessageCounter() {
        return messageCounter.get();
    }

    /**
     * Get processed message counter for monitoring
     */
    public long getProcessedMessageCounter() {
        return processedMessageCounter.get();
    }

    /**
     * Get error message counter for monitoring
     */
    public long getErrorMessageCounter() {
        return errorMessageCounter.get();
    }

    /**
     * Get null message counter for monitoring
     */
    public long getNullMessageCounter() {
        return nullMessageCounter.get();
    }

    /**
     * Get invalid type counter for monitoring
     */
    public long getInvalidTypeCounter() {
        return invalidTypeCounter.get();
    }

    /**
     * Get DLQ send failure counter for monitoring
     */
    public long getDlqSendFailureCounter() {
        return dlqSendFailureCounter.get();
    }

    /**
     * Get output send failure counter for monitoring
     */
    public long getOutputSendFailureCounter() {
        return outputSendFailureCounter.get();
    }
}
