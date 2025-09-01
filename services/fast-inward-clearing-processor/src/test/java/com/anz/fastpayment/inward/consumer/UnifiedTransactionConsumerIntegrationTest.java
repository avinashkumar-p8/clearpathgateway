package com.anz.fastpayment.inward.consumer;

import com.anz.fastpayment.inward.avro.ProcessedTransactionMessage;
import com.anz.fastpayment.inward.handler.TransactionProcessingHandler;
import com.anz.fastpayment.inward.model.ProcessingResult;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.nio.charset.StandardCharsets;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for UnifiedTransactionConsumer
 * Tests the complete processing pipeline with the new schema
 */
@ExtendWith(MockitoExtension.class)
class UnifiedTransactionConsumerIntegrationTest {

    @Mock
    private TransactionProcessingHandler processingHandler;
    
    @Mock
    private KafkaProducer<String, ProcessedTransactionMessage> kafkaProducer;
    
    @Mock
    private KafkaProducer<String, String> dlqProducer;
    
    @Mock
    private Acknowledgment acknowledgment;
    
    private UnifiedTransactionConsumer consumer;
    
    @BeforeEach
    void setUp() {
        consumer = new UnifiedTransactionConsumer(processingHandler);
        // Inject mocked producers using reflection for testing
        try {
            var kafkaProducerField = UnifiedTransactionConsumer.class.getDeclaredField("kafkaProducer");
            kafkaProducerField.setAccessible(true);
            kafkaProducerField.set(consumer, kafkaProducer);
            
            var dlqProducerField = UnifiedTransactionConsumer.class.getDeclaredField("dlqProducer");
            dlqProducerField.setAccessible(true);
            dlqProducerField.set(consumer, dlqProducer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject mocked producers", e);
        }
    }
    
    @Test
    void testSuccessfulMessageProcessing() {
        // Given
        String transactionId = "TXN-12345";
        String muid = "MUID-12345";
        
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId, muid);
        ProcessingResult successResult = ProcessingResult.success(createTestProcessedMessage(transactionId), 150L);
        
        when(processingHandler.handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class))).thenReturn(successResult);
        
        // When
        consumer.consumeTransaction(consumerRecord, acknowledgment);
        
        // Then
        verify(processingHandler).handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class));
        verify(kafkaProducer).send(any(), any());
        verify(acknowledgment).acknowledge();
        verify(dlqProducer, never()).send(any(), any());
        
        // Verify metrics
        assertEquals(1, consumer.getMessagesReceived());
        assertEquals(1, consumer.getMessagesProcessed());
        assertEquals(0, consumer.getSystemErrors());
    }
    
    @Test
    void testDuplicateMessageHandling() {
        // Given
        String transactionId = "TXN-12345";
        String muid = "MUID-12345";
        
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId, muid);
        ProcessingResult duplicateResult = ProcessingResult.duplicate(muid, 50L);
        
        when(processingHandler.handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class))).thenReturn(duplicateResult);
        
        // When
        consumer.consumeTransaction(consumerRecord, acknowledgment);
        
        // Then
        verify(processingHandler).handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class));
        verify(kafkaProducer, never()).send(any(), any());
        verify(dlqProducer, never()).send(any(), any());
        verify(acknowledgment).acknowledge();
        
        // Verify metrics
        assertEquals(1, consumer.getMessagesReceived());
        assertEquals(0, consumer.getMessagesProcessed());
        assertEquals(1, consumer.getDuplicateMessages());
    }
    
    @Test
    void testValidationFailureHandling() {
        // Given
        String transactionId = "TXN-12345";
        String muid = "MUID-12345";
        
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId, muid);
        ProcessingResult validationFailureResult = ProcessingResult.validationFailed(
            java.util.List.of("Invalid currency code", "Missing beneficiary name"), 100L);
        
        when(processingHandler.handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class))).thenReturn(validationFailureResult);
        
        // When
        consumer.consumeTransaction(consumerRecord, acknowledgment);
        
        // Then
        verify(processingHandler).handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class));
        verify(kafkaProducer, never()).send(any(), any());
        verify(dlqProducer).send(any(), any());
        verify(acknowledgment).acknowledge();
        
        // Verify metrics
        assertEquals(1, consumer.getMessagesReceived());
        assertEquals(0, consumer.getMessagesProcessed());
        assertEquals(1, consumer.getValidationFailures());
    }
    
    @Test
    void testBusinessProcessingFailureHandling() {
        // Given
        String transactionId = "TXN-12345";
        String muid = "MUID-12345";
        
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId, muid);
        ProcessingResult businessFailureResult = ProcessingResult.businessProcessingFailed(
            "Insufficient funds", 200L);
        
        when(processingHandler.handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class))).thenReturn(businessFailureResult);
        
        // When
        consumer.consumeTransaction(consumerRecord, acknowledgment);
        
        // Then
        verify(processingHandler).handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class));
        verify(kafkaProducer, never()).send(any(), any());
        verify(dlqProducer).send(any(), any());
        verify(acknowledgment).acknowledge();
        
        // Verify metrics
        assertEquals(1, consumer.getMessagesReceived());
        assertEquals(0, consumer.getMessagesProcessed());
        assertEquals(1, consumer.getBusinessProcessingFailures());
    }
    
    @Test
    void testSystemErrorHandling() {
        // Given
        String transactionId = "TXN-12345";
        String muid = "MUID-12345";
        
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId, muid);
        ProcessingResult systemErrorResult = ProcessingResult.systemError(
            new RuntimeException("Database connection failed"), 300L);
        
        when(processingHandler.handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class))).thenReturn(systemErrorResult);
        
        // When
        consumer.consumeTransaction(consumerRecord, acknowledgment);
        
        // Then
        verify(processingHandler).handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class));
        verify(kafkaProducer, never()).send(any(), any());
        verify(dlqProducer).send(any(), any());
        verify(acknowledgment).acknowledge();
        
        // Verify metrics
        assertEquals(1, consumer.getMessagesReceived());
        assertEquals(0, consumer.getMessagesProcessed());
        assertEquals(1, consumer.getSystemErrors());
    }
    
    @Test
    void testHandlerExceptionHandling() {
        // Given
        String transactionId = "TXN-12345";
        String muid = "MUID-12345";
        
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId, muid);
        
        when(processingHandler.handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class))).thenThrow(new RuntimeException("Handler exception"));
        
        // When
        consumer.consumeTransaction(consumerRecord, acknowledgment);
        
        // Then
        verify(processingHandler).handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class));
        verify(kafkaProducer, never()).send(any(), any());
        verify(dlqProducer).send(any(), any());
        verify(acknowledgment).acknowledge();
        
        // Verify metrics
        assertEquals(1, consumer.getMessagesReceived());
        assertEquals(0, consumer.getMessagesProcessed());
        assertEquals(1, consumer.getSystemErrors());
    }
    
    @Test
    void testMessageWithHeaders() {
        // Given
        String transactionId = "TXN-12345";
        String muid = "MUID-12345";
        
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecordWithHeaders(transactionId, muid);
        ProcessingResult successResult = ProcessingResult.success(createTestProcessedMessage(transactionId), 150L);
        
        when(processingHandler.handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class))).thenReturn(successResult);
        
        // When
        consumer.consumeTransaction(consumerRecord, acknowledgment);
        
        // Then
        verify(processingHandler).handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class));
        verify(kafkaProducer).send(any(), any());
        verify(acknowledgment).acknowledge();
        
        // Verify metrics
        assertEquals(1, consumer.getMessagesReceived());
        assertEquals(1, consumer.getMessagesProcessed());
    }
    
    @Test
    void testMultipleMessageProcessing() {
        // Given
        String[] transactionIds = {"TXN-001", "TXN-002", "TXN-003"};
        ProcessingResult successResult = ProcessingResult.success(createTestProcessedMessage("TXN-001"), 150L);
        
        when(processingHandler.handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class))).thenReturn(successResult);
        
        // When
        for (String transactionId : transactionIds) {
            ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId, "MUID-" + transactionId);
            consumer.consumeTransaction(consumerRecord, acknowledgment);
        }
        
        // Then
        verify(processingHandler, times(3)).handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class));
        verify(kafkaProducer, times(3)).send(any(), any());
        verify(acknowledgment, times(3)).acknowledge();
        
        // Verify metrics
        assertEquals(3, consumer.getMessagesReceived());
        assertEquals(3, consumer.getMessagesProcessed());
    }
    
    @Test
    void testMetricsReset() {
        // Given
        String transactionId = "TXN-12345";
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId, "MUID-12345");
        ProcessingResult successResult = ProcessingResult.success(createTestProcessedMessage(transactionId), 150L);
        
        when(processingHandler.handleTransaction(any(com.anz.fastpayment.inward.model.ProcessingContext.class))).thenReturn(successResult);
        
        // Process a message first
        consumer.consumeTransaction(consumerRecord, acknowledgment);
        
        // Verify metrics are set
        assertEquals(1, consumer.getMessagesReceived());
        assertEquals(1, consumer.getMessagesProcessed());
        
        // When - Reset metrics
        consumer.resetMetrics();
        
        // Then - Verify metrics are reset
        assertEquals(0, consumer.getMessagesReceived());
        assertEquals(0, consumer.getMessagesProcessed());
        assertEquals(0, consumer.getDuplicateMessages());
        assertEquals(0, consumer.getValidationFailures());
        assertEquals(0, consumer.getBusinessProcessingFailures());
        assertEquals(0, consumer.getSystemErrors());
    }
    
    // Helper methods
    
    private ConsumerRecord<String, GenericRecord> createTestConsumerRecord(String transactionId, String muid) {
        GenericRecord avroMessage = createTestAvroMessage(transactionId);
        return new ConsumerRecord<>("test-topic", 0, 0L, transactionId, avroMessage);
    }
    
    private ConsumerRecord<String, GenericRecord> createTestConsumerRecordWithHeaders(String transactionId, String muid) {
        GenericRecord avroMessage = createTestAvroMessage(transactionId);
        RecordHeaders headers = new RecordHeaders();
        headers.add("muid", muid.getBytes(StandardCharsets.UTF_8));
        headers.add("source", "test-source".getBytes(StandardCharsets.UTF_8));
        
        ConsumerRecord<String, GenericRecord> record = new ConsumerRecord<>("test-topic", 0, 0L, transactionId, avroMessage);
        for (org.apache.kafka.common.header.Header header : headers) {
            record.headers().add(header);
        }
        return record;
    }
    
    private GenericRecord createTestAvroMessage(String transactionId) {
        // Create a mock GenericRecord that represents the UnifiedPaymentMessage
        // In a real test, you would create an actual UnifiedPaymentMessage instance
        return mock(GenericRecord.class);
    }
    
    private ProcessedTransactionMessage createTestProcessedMessage(String transactionId) {
        ProcessedTransactionMessage message = new ProcessedTransactionMessage();
        message.setTransactionId(transactionId);
        message.setStatus("COMPLETED");
        message.setProcessingNodeId("unified-transaction-consumer");
        message.setErrorMessage(null);
        return message;
    }
}
