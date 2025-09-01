package com.anz.fastpayment.inward.handler;

import com.anz.fastpayment.inward.avro.ProcessedTransactionMessage;
import com.anz.fastpayment.inward.handler.impl.TransactionProcessingHandlerImpl;
import com.anz.fastpayment.inward.model.*;
import com.anz.fastpayment.inward.scheme.validation.model.ValidationResult;
import com.anz.fastpayment.inward.scheme.validation.model.TagValidationResult;
import com.anz.fastpayment.inward.scheme.validation.model.ValidationStatus;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for TransactionProcessingHandler
 * Tests the complete processing pipeline with all handlers
 */
@ExtendWith(MockitoExtension.class)
class TransactionProcessingHandlerIntegrationTest {

    @Mock
    private IdempotencyHandler idempotencyHandler;
    
    @Mock
    private MessageParsingHandler parsingHandler;
    
    @Mock
    private ValidationHandler validationHandler;
    
    @Mock
    private BusinessProcessingHandler businessHandler;
    
    private TransactionProcessingHandler processingHandler;
    
    @BeforeEach
    void setUp() {
        processingHandler = new TransactionProcessingHandlerImpl(
            idempotencyHandler, parsingHandler, validationHandler, businessHandler);
    }
    
    @Test
    void testCompleteSuccessfulProcessingPipeline() {
        // Given
        String transactionId = "TXN-12345";
        String muid = "MUID-12345";
        
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        // Mock idempotency check - new message
        IdempotencyResult idempotencyResult = IdempotencyResult.newMessage(muid);
        when(idempotencyHandler.checkAndRegister(context)).thenReturn(idempotencyResult);
        
        // Mock parsing - successful
        Map<String, Object> messagePayload = createTestMessagePayload();
        TransactionMessage transactionMessage = createTestTransactionMessage(transactionId);
        ParsingResult parsingResult = ParsingResult.success(messagePayload, transactionMessage);
        when(parsingHandler.parseAndEnrich(context)).thenReturn(parsingResult);
        
        // Mock validation - successful
        ValidationResult validationResult = createSuccessfulValidationResult(transactionId);
        when(validationHandler.validate(messagePayload, transactionId)).thenReturn(validationResult);
        when(validationHandler.isValidationSuccessful(validationResult)).thenReturn(true);
        
        // Mock business processing - successful
        ProcessedTransactionMessage processedMessage = createTestProcessedMessage(transactionId);
        BusinessProcessingResult businessResult = BusinessProcessingResult.success(processedMessage);
        when(businessHandler.process(messagePayload, transactionId)).thenReturn(businessResult);
        
        // When
        ProcessingResult result = processingHandler.handleTransaction(context);
        
        // Then
        assertTrue(result.isSuccess());
        assertEquals("COMPLETED", result.getStatus());
        assertEquals(processedMessage, result.getProcessedMessage());
        assertTrue(result.getProcessingDurationMs() > 0);
        
        // Verify all handlers were called
        verify(idempotencyHandler).checkAndRegister(context);
        verify(parsingHandler).parseAndEnrich(context);
        verify(validationHandler).validate(messagePayload, transactionId);
        verify(validationHandler).isValidationSuccessful(validationResult);
        verify(businessHandler).process(messagePayload, transactionId);
        verify(idempotencyHandler).updateProcessingStatus(muid, "COMPLETED");
    }
    
    @Test
    void testDuplicateMessageHandling() {
        // Given
        String transactionId = "TXN-12345";
        String muid = "MUID-12345";
        
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        // Mock idempotency check - duplicate message
        IdempotencyResult idempotencyResult = IdempotencyResult.duplicateMessage(muid);
        when(idempotencyHandler.checkAndRegister(context)).thenReturn(idempotencyResult);
        
        // When
        ProcessingResult result = processingHandler.handleTransaction(context);
        
        // Then
        assertFalse(result.isSuccess());
        assertEquals("DUPLICATE_SKIPPED", result.getStatus());
        assertEquals(muid, result.getMuid());
        assertTrue(result.getProcessingDurationMs() > 0);
        
        // Verify only idempotency handler was called
        verify(idempotencyHandler).checkAndRegister(context);
        verify(parsingHandler, never()).parseAndEnrich(any());
        verify(validationHandler, never()).validate(any(), any());
        verify(businessHandler, never()).process(any(), any());
    }
    
    @Test
    void testParsingFailureHandling() {
        // Given
        String transactionId = "TXN-12345";
        String muid = "MUID-12345";
        
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        // Mock idempotency check - new message
        IdempotencyResult idempotencyResult = IdempotencyResult.newMessage(muid);
        when(idempotencyHandler.checkAndRegister(context)).thenReturn(idempotencyResult);
        
        // Mock parsing - failure
        ParsingResult parsingResult = ParsingResult.failed("Failed to parse Avro message");
        when(parsingHandler.parseAndEnrich(context)).thenReturn(parsingResult);
        
        // When
        ProcessingResult result = processingHandler.handleTransaction(context);
        
        // Then
        assertFalse(result.isSuccess());
        assertEquals("PARSING_FAILED", result.getStatus());
        assertEquals("Failed to parse Avro message", result.getErrorMessage());
        assertTrue(result.getProcessingDurationMs() > 0);
        
        // Verify idempotency and parsing handlers were called
        verify(idempotencyHandler).checkAndRegister(context);
        verify(parsingHandler).parseAndEnrich(context);
        verify(validationHandler, never()).validate(any(), any());
        verify(businessHandler, never()).process(any(), any());
    }
    
    @Test
    void testValidationFailureHandling() {
        // Given
        String transactionId = "TXN-12345";
        String muid = "MUID-12345";
        
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        // Mock idempotency check - new message
        IdempotencyResult idempotencyResult = IdempotencyResult.newMessage(muid);
        when(idempotencyHandler.checkAndRegister(context)).thenReturn(idempotencyResult);
        
        // Mock parsing - successful
        Map<String, Object> messagePayload = createTestMessagePayload();
        TransactionMessage transactionMessage = createTestTransactionMessage(transactionId);
        ParsingResult parsingResult = ParsingResult.success(messagePayload, transactionMessage);
        when(parsingHandler.parseAndEnrich(context)).thenReturn(parsingResult);
        
        // Mock validation - failure
        ValidationResult validationResult = createFailedValidationResult(transactionId);
        when(validationHandler.validate(messagePayload, transactionId)).thenReturn(validationResult);
        when(validationHandler.isValidationSuccessful(validationResult)).thenReturn(false);
        when(validationHandler.getValidationErrors(validationResult)).thenReturn(
            List.of("Invalid currency code", "Missing beneficiary name"));
        
        // When
        ProcessingResult result = processingHandler.handleTransaction(context);
        
        // Then
        assertFalse(result.isSuccess());
        assertEquals("VALIDATION_FAILED", result.getStatus());
        assertEquals("Validation failed", result.getErrorMessage());
        assertEquals(2, result.getValidationErrors().size());
        assertTrue(result.getValidationErrors().contains("Invalid currency code"));
        assertTrue(result.getValidationErrors().contains("Missing beneficiary name"));
        assertTrue(result.getProcessingDurationMs() > 0);
        
        // Verify handlers were called up to validation
        verify(idempotencyHandler).checkAndRegister(context);
        verify(parsingHandler).parseAndEnrich(context);
        verify(validationHandler).validate(messagePayload, transactionId);
        verify(validationHandler).isValidationSuccessful(validationResult);
        verify(validationHandler).getValidationErrors(validationResult);
        verify(businessHandler, never()).process(any(), any());
    }
    
    @Test
    void testBusinessProcessingFailureHandling() {
        // Given
        String transactionId = "TXN-12345";
        String muid = "MUID-12345";
        
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        // Mock idempotency check - new message
        IdempotencyResult idempotencyResult = IdempotencyResult.newMessage(muid);
        when(idempotencyHandler.checkAndRegister(context)).thenReturn(idempotencyResult);
        
        // Mock parsing - successful
        Map<String, Object> messagePayload = createTestMessagePayload();
        TransactionMessage transactionMessage = createTestTransactionMessage(transactionId);
        ParsingResult parsingResult = ParsingResult.success(messagePayload, transactionMessage);
        when(parsingHandler.parseAndEnrich(context)).thenReturn(parsingResult);
        
        // Mock validation - successful
        ValidationResult validationResult = createSuccessfulValidationResult(transactionId);
        when(validationHandler.validate(messagePayload, transactionId)).thenReturn(validationResult);
        when(validationHandler.isValidationSuccessful(validationResult)).thenReturn(true);
        
        // Mock business processing - failure
        BusinessProcessingResult businessResult = BusinessProcessingResult.failed("Insufficient funds");
        when(businessHandler.process(messagePayload, transactionId)).thenReturn(businessResult);
        
        // When
        ProcessingResult result = processingHandler.handleTransaction(context);
        
        // Then
        assertFalse(result.isSuccess());
        assertEquals("BUSINESS_PROCESSING_FAILED", result.getStatus());
        assertEquals("Insufficient funds", result.getErrorMessage());
        assertTrue(result.getProcessingDurationMs() > 0);
        
        // Verify all handlers were called
        verify(idempotencyHandler).checkAndRegister(context);
        verify(parsingHandler).parseAndEnrich(context);
        verify(validationHandler).validate(messagePayload, transactionId);
        verify(validationHandler).isValidationSuccessful(validationResult);
        verify(businessHandler).process(messagePayload, transactionId);
    }
    
    @Test
    void testSystemErrorHandling() {
        // Given
        String transactionId = "TXN-12345";
        String muid = "MUID-12345";
        
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        // Mock idempotency check - new message
        IdempotencyResult idempotencyResult = IdempotencyResult.newMessage(muid);
        when(idempotencyHandler.checkAndRegister(context)).thenReturn(idempotencyResult);
        
        // Mock parsing - successful
        Map<String, Object> messagePayload = createTestMessagePayload();
        TransactionMessage transactionMessage = createTestTransactionMessage(transactionId);
        ParsingResult parsingResult = ParsingResult.success(messagePayload, transactionMessage);
        when(parsingHandler.parseAndEnrich(context)).thenReturn(parsingResult);
        
        // Mock validation - successful
        ValidationResult validationResult = createSuccessfulValidationResult(transactionId);
        when(validationHandler.validate(messagePayload, transactionId)).thenReturn(validationResult);
        when(validationHandler.isValidationSuccessful(validationResult)).thenReturn(true);
        
        // Mock business processing - system error
        when(businessHandler.process(messagePayload, transactionId)).thenThrow(new RuntimeException("Database connection failed"));
        
        // When
        ProcessingResult result = processingHandler.handleTransaction(context);
        
        // Then
        assertFalse(result.isSuccess());
        assertEquals("SYSTEM_ERROR", result.getStatus());
        assertEquals("Database connection failed", result.getErrorMessage());
        assertTrue(result.getProcessingDurationMs() > 0);
        
        // Verify all handlers were called
        verify(idempotencyHandler).checkAndRegister(context);
        verify(parsingHandler).parseAndEnrich(context);
        verify(validationHandler).validate(messagePayload, transactionId);
        verify(validationHandler).isValidationSuccessful(validationResult);
        verify(businessHandler).process(messagePayload, transactionId);
        verify(idempotencyHandler).updateProcessingStatus(muid, "FAILED");
    }
    
    @Test
    void testProcessingMetrics() {
        // Given
        String transactionId = "TXN-12345";
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        // Mock successful processing
        IdempotencyResult idempotencyResult = IdempotencyResult.newMessage("MUID-12345");
        when(idempotencyHandler.checkAndRegister(context)).thenReturn(idempotencyResult);
        
        Map<String, Object> messagePayload = createTestMessagePayload();
        TransactionMessage transactionMessage = createTestTransactionMessage(transactionId);
        ParsingResult parsingResult = ParsingResult.success(messagePayload, transactionMessage);
        when(parsingHandler.parseAndEnrich(context)).thenReturn(parsingResult);
        
        ValidationResult validationResult = createSuccessfulValidationResult(transactionId);
        when(validationHandler.validate(messagePayload, transactionId)).thenReturn(validationResult);
        when(validationHandler.isValidationSuccessful(validationResult)).thenReturn(true);
        
        ProcessedTransactionMessage processedMessage = createTestProcessedMessage(transactionId);
        BusinessProcessingResult businessResult = BusinessProcessingResult.success(processedMessage);
        when(businessHandler.process(messagePayload, transactionId)).thenReturn(businessResult);
        
        // When - Process multiple messages
        processingHandler.handleTransaction(context);
        processingHandler.handleTransaction(context);
        
        // Then - Check metrics
        Map<String, Long> metrics = processingHandler.getProcessingMetrics();
        assertEquals(2L, metrics.get("totalMessagesProcessed"));
        assertEquals(2L, metrics.get("successfulMessages"));
        assertEquals(0L, metrics.get("duplicateMessages"));
        assertEquals(0L, metrics.get("parsingFailures"));
        assertEquals(0L, metrics.get("validationFailures"));
        assertEquals(0L, metrics.get("businessProcessingFailures"));
        assertEquals(0L, metrics.get("systemErrors"));
    }
    
    @Test
    void testProcessingStatistics() {
        // Given
        String transactionId = "TXN-12345";
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        // Mock successful processing
        IdempotencyResult idempotencyResult = IdempotencyResult.newMessage("MUID-12345");
        when(idempotencyHandler.checkAndRegister(context)).thenReturn(idempotencyResult);
        
        Map<String, Object> messagePayload = createTestMessagePayload();
        TransactionMessage transactionMessage = createTestTransactionMessage(transactionId);
        ParsingResult parsingResult = ParsingResult.success(messagePayload, transactionMessage);
        when(parsingHandler.parseAndEnrich(context)).thenReturn(parsingResult);
        
        ValidationResult validationResult = createSuccessfulValidationResult(transactionId);
        when(validationHandler.validate(messagePayload, transactionId)).thenReturn(validationResult);
        when(validationHandler.isValidationSuccessful(validationResult)).thenReturn(true);
        
        ProcessedTransactionMessage processedMessage = createTestProcessedMessage(transactionId);
        BusinessProcessingResult businessResult = BusinessProcessingResult.success(processedMessage);
        when(businessHandler.process(messagePayload, transactionId)).thenReturn(businessResult);
        
        // When - Process messages
        processingHandler.handleTransaction(context);
        processingHandler.handleTransaction(context);
        
        // Then - Check statistics
        Map<String, Object> statistics = processingHandler.getProcessingStatistics();
        assertEquals(2L, statistics.get("totalMessagesProcessed"));
        assertEquals(2L, statistics.get("successfulMessages"));
        assertEquals(0L, statistics.get("failedMessages"));
        assertEquals("100.00%", statistics.get("successRate"));
        assertEquals("0.00%", statistics.get("failureRate"));
    }
    
    @Test
    void testMetricsReset() {
        // Given
        String transactionId = "TXN-12345";
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        // Mock successful processing
        IdempotencyResult idempotencyResult = IdempotencyResult.newMessage("MUID-12345");
        when(idempotencyHandler.checkAndRegister(context)).thenReturn(idempotencyResult);
        
        Map<String, Object> messagePayload = createTestMessagePayload();
        TransactionMessage transactionMessage = createTestTransactionMessage(transactionId);
        ParsingResult parsingResult = ParsingResult.success(messagePayload, transactionMessage);
        when(parsingHandler.parseAndEnrich(context)).thenReturn(parsingResult);
        
        ValidationResult validationResult = createSuccessfulValidationResult(transactionId);
        when(validationHandler.validate(messagePayload, transactionId)).thenReturn(validationResult);
        when(validationHandler.isValidationSuccessful(validationResult)).thenReturn(true);
        
        ProcessedTransactionMessage processedMessage = createTestProcessedMessage(transactionId);
        BusinessProcessingResult businessResult = BusinessProcessingResult.success(processedMessage);
        when(businessHandler.process(messagePayload, transactionId)).thenReturn(businessResult);
        
        // Process a message first
        processingHandler.handleTransaction(context);
        
        // Verify metrics are set
        Map<String, Long> metrics = processingHandler.getProcessingMetrics();
        assertEquals(1L, metrics.get("totalMessagesProcessed"));
        
        // When - Reset metrics
        processingHandler.resetProcessingMetrics();
        
        // Then - Verify metrics are reset
        Map<String, Long> resetMetrics = processingHandler.getProcessingMetrics();
        assertEquals(0L, resetMetrics.get("totalMessagesProcessed"));
        assertEquals(0L, resetMetrics.get("successfulMessages"));
        assertEquals(0L, resetMetrics.get("duplicateMessages"));
        assertEquals(0L, resetMetrics.get("parsingFailures"));
        assertEquals(0L, resetMetrics.get("validationFailures"));
        assertEquals(0L, resetMetrics.get("businessProcessingFailures"));
        assertEquals(0L, resetMetrics.get("systemErrors"));
    }
    
    // Helper methods
    
    private ConsumerRecord<String, GenericRecord> createTestConsumerRecord(String transactionId) {
        GenericRecord avroMessage = mock(GenericRecord.class);
        return new ConsumerRecord<>("test-topic", 0, 0L, transactionId, avroMessage);
    }
    
    private Map<String, Object> createTestMessagePayload() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("transactionId", "TXN-12345");
        payload.put("amount", 1000.0);
        payload.put("currency", "USD");
        payload.put("senderAccount", "ACC-001");
        payload.put("receiverAccount", "ACC-002");
        payload.put("transactionType", "TRANSFER");
        return payload;
    }
    
    private TransactionMessage createTestTransactionMessage(String transactionId) {
        TransactionMessage message = new TransactionMessage();
        message.setTransactionId(transactionId);
        message.setAmount(java.math.BigDecimal.valueOf(1000.0));
        message.setCurrency("USD");
        message.setSenderAccount("ACC-001");
        message.setReceiverAccount("ACC-002");
        message.setTransactionType("TRANSFER");
        return message;
    }
    
    private ProcessedTransactionMessage createTestProcessedMessage(String transactionId) {
        ProcessedTransactionMessage message = new ProcessedTransactionMessage();
        message.setTransactionId(transactionId);
        message.setStatus("COMPLETED");
        message.setProcessingNodeId("unified-transaction-consumer");
        message.setErrorMessage(null);
        return message;
    }
    
    private ValidationResult createSuccessfulValidationResult(String transactionId) {
        return new ValidationResult(transactionId, java.util.List.of());
    }
    
    private ValidationResult createFailedValidationResult(String transactionId) {
        TagValidationResult failure1 = new TagValidationResult(
            "CURRENCY", "USD", ValidationStatus.FAILURE, "Invalid currency code", "INVALID_CURRENCY");
        TagValidationResult failure2 = new TagValidationResult(
            "BENEFICIARY", "John Doe", ValidationStatus.FAILURE, "Missing beneficiary name", "MISSING_BENEFICIARY");
        
        return new ValidationResult(transactionId, java.util.List.of(failure1, failure2));
    }
}
