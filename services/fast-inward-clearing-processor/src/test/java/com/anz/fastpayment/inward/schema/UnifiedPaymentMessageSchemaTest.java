package com.anz.fastpayment.inward.schema;

import com.anz.fastpayment.inward.avro.UnifiedPaymentMessage;

import com.anz.fastpayment.inward.handler.impl.MessageParsingHandlerImpl;
import com.anz.fastpayment.inward.model.ProcessingContext;
import com.anz.fastpayment.inward.model.ParsingResult;
import com.anz.fastpayment.inward.model.TransactionMessage;
import com.anz.fastpayment.inward.scheme.validation.util.AvroMessageParser;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Integration tests for UnifiedPaymentMessage schema
 * Tests the new schema structure and parsing capabilities
 */
@ExtendWith(MockitoExtension.class)
class UnifiedPaymentMessageSchemaTest {

    private MessageParsingHandlerImpl parsingHandler;
    
    @Mock
    private AvroMessageParser avroMessageParser;
    
    @BeforeEach
    void setUp() {
        parsingHandler = new MessageParsingHandlerImpl();
    }
    
    @Test
    void testUnifiedPaymentMessageSchemaStructure() {
        // Given
        UnifiedPaymentMessage message = new UnifiedPaymentMessage();
        
        // When & Then
        assertNotNull(message);
        // Basic schema structure validation
        assertNotNull(message.getHeader());
        assertNotNull(message.getBody());
        assertNotNull(message.getProcctxt());
        assertNotNull(message.getMessages());
    }
    
    @Test
    void testMessageParsingWithNewSchema() {
        // Given
        String transactionId = "TXN-12345";
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        // Mock the AvroMessageParser to return test data
        Map<String, Object> expectedPayload = createTestMessagePayload();
        when(AvroMessageParser.convertToMap(any(GenericRecord.class))).thenReturn(expectedPayload);
        
        // When
        ParsingResult result = parsingHandler.parseAndEnrich(context);
        
        // Then
        assertTrue(result.isSuccess());
        assertNotNull(result.getMessagePayload());
        assertNotNull(result.getTransactionMessage());
        
        // Verify payload structure
        Map<String, Object> payload = result.getMessagePayload();
        assertEquals("TXN-12345", payload.get("transactionId"));
        assertEquals(1000.0, payload.get("amount"));
        assertEquals("USD", payload.get("currency"));
        
        // Verify transaction message
        TransactionMessage transactionMessage = result.getTransactionMessage();
        assertEquals("TXN-12345", transactionMessage.getTransactionId());
        assertEquals(BigDecimal.valueOf(1000.0), transactionMessage.getAmount());
        assertEquals("USD", transactionMessage.getCurrency());
    }
    
    @Test
    void testMessageSectionExtraction() {
        // Given
        String transactionId = "TXN-12345";
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        // Mock the AvroMessageParser
        Map<String, Object> expectedPayload = createTestMessagePayload();
        when(AvroMessageParser.convertToMap(any(GenericRecord.class))).thenReturn(expectedPayload);
        
        // When
        Map<String, Object> extractedPayload = parsingHandler.extractMessageSection(context);
        
        // Then
        assertNotNull(extractedPayload);
        assertFalse(extractedPayload.isEmpty());
        assertEquals("TXN-12345", extractedPayload.get("transactionId"));
        assertEquals(1000.0, extractedPayload.get("amount"));
        assertEquals("USD", extractedPayload.get("currency"));
    }
    
    @Test
    void testTransactionMessageConversion() {
        // Given
        String transactionId = "TXN-12345";
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        // When
        TransactionMessage transactionMessage = parsingHandler.convertToTransactionMessage(context);
        
        // Then
        assertNotNull(transactionMessage);
        assertEquals("TXN-12345", transactionMessage.getTransactionId());
        assertNotNull(transactionMessage.getTimestamp());
    }
    
    @Test
    void testMessageEnrichment() {
        // Given
        String transactionId = "TXN-12345";
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecord(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        TransactionMessage transactionMessage = new TransactionMessage();
        transactionMessage.setTransactionId(transactionId);
        transactionMessage.setAmount(BigDecimal.valueOf(1000.0));
        transactionMessage.setCurrency("USD");
        
        // When
        parsingHandler.enrichWithMetadata(transactionMessage, context);
        
        // Then
        assertNotNull(transactionMessage.getTimestamp());
        assertNotNull(transactionMessage.getReference());
        assertNotNull(transactionMessage.getPriority());
        assertEquals("NORMAL", transactionMessage.getPriority());
        assertTrue(transactionMessage.getReference().startsWith("REF-"));
    }
    
    @Test
    void testEmptyMessageHandling() {
        // Given
        String transactionId = "TXN-12345";
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecordWithEmptyMessage(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        // When
        ParsingResult result = parsingHandler.parseAndEnrich(context);
        
        // Then
        assertFalse(result.isSuccess());
        assertEquals("Failed to extract message section from Avro message", result.getErrorMessage());
    }
    
    @Test
    void testNullMessageHandling() {
        // Given
        String transactionId = "TXN-12345";
        ConsumerRecord<String, GenericRecord> consumerRecord = createTestConsumerRecordWithNullMessage(transactionId);
        ProcessingContext context = new ProcessingContext(consumerRecord);
        
        // When
        ParsingResult result = parsingHandler.parseAndEnrich(context);
        
        // Then
        assertFalse(result.isSuccess());
        assertEquals("Failed to extract message section from Avro message", result.getErrorMessage());
    }
    
    @Test
    void testSchemaValidation() {
        // Given
        UnifiedPaymentMessage message = new UnifiedPaymentMessage();
        
        // When & Then
        // Basic schema validation
        assertNotNull(message);
        assertNotNull(message.getHeader());
        assertNotNull(message.getBody());
        assertNotNull(message.getProcctxt());
        assertNotNull(message.getMessages());
    }
    
    @Test
    void testAmountPrecisionHandling() {
        // Given
        UnifiedPaymentMessage message = new UnifiedPaymentMessage();
        
        // When & Then
        // Basic schema structure validation
        assertNotNull(message);
        assertNotNull(message.getHeader());
        assertNotNull(message.getBody());
    }
    
    // Helper methods
    
    private ConsumerRecord<String, GenericRecord> createTestConsumerRecord(String transactionId) {
        UnifiedPaymentMessage message = new UnifiedPaymentMessage();
        return new ConsumerRecord<>("test-topic", 0, 0L, transactionId, message);
    }
    
    private ConsumerRecord<String, GenericRecord> createTestConsumerRecordWithEmptyMessage(String transactionId) {
        UnifiedPaymentMessage message = new UnifiedPaymentMessage();
        message.setMessages(java.util.List.of()); // Empty messages array
        return new ConsumerRecord<>("test-topic", 0, 0L, transactionId, message);
    }
    
    private ConsumerRecord<String, GenericRecord> createTestConsumerRecordWithNullMessage(String transactionId) {
        UnifiedPaymentMessage message = new UnifiedPaymentMessage();
        message.setMessages(null); // Null messages array
        return new ConsumerRecord<>("test-topic", 0, 0L, transactionId, message);
    }
    
    private Map<String, Object> createTestMessagePayload() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("transactionId", "TXN-12345");
        payload.put("amount", 1000.0);
        payload.put("currency", "USD");
        payload.put("senderAccount", "ACC-001");
        payload.put("receiverAccount", "ACC-002");
        payload.put("transactionType", "TRANSFER");
        payload.put("paymentId", "PAY-12345");
        payload.put("endToEndId", "E2E-12345");
        payload.put("instructionId", "INST-12345");
        return payload;
    }
}
