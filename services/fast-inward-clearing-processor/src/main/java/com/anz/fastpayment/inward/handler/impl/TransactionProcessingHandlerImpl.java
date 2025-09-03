package com.anz.fastpayment.inward.handler.impl;

import com.anz.fastpayment.inward.handler.*;
import com.anz.fastpayment.inward.model.*;
import com.anz.fastpayment.inward.metrics.ProcessingTimeMetrics;
import com.anz.fastpayment.inward.metrics.CircuitBreakerMetrics;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of TransactionProcessingHandler
 * Main orchestrator for the transaction processing pipeline
 */
@Service
public class TransactionProcessingHandlerImpl implements TransactionProcessingHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(TransactionProcessingHandlerImpl.class);
    
    private final IdempotencyHandler idempotencyHandler;
    private final MessageParsingHandler parsingHandler;
    private final ValidationHandler validationHandler;
    private final BusinessProcessingHandler businessHandler;
    private final ProcessingTimeMetrics processingTimeMetrics;
    private final CircuitBreakerMetrics circuitBreakerMetrics;
    
    // Metrics counters
    private final AtomicLong totalMessagesProcessed = new AtomicLong(0);
    private final AtomicLong successfulMessages = new AtomicLong(0);
    private final AtomicLong duplicateMessages = new AtomicLong(0);
    private final AtomicLong parsingFailures = new AtomicLong(0);
    private final AtomicLong validationFailures = new AtomicLong(0);
    private final AtomicLong businessProcessingFailures = new AtomicLong(0);
    private final AtomicLong systemErrors = new AtomicLong(0);
    
    public TransactionProcessingHandlerImpl(IdempotencyHandler idempotencyHandler,
                                          MessageParsingHandler parsingHandler,
                                          ValidationHandler validationHandler,
                                          BusinessProcessingHandler businessHandler,
                                          ProcessingTimeMetrics processingTimeMetrics,
                                          CircuitBreakerMetrics circuitBreakerMetrics) {
        this.idempotencyHandler = idempotencyHandler;
        this.parsingHandler = parsingHandler;
        this.validationHandler = validationHandler;
        this.businessHandler = businessHandler;
        this.processingTimeMetrics = processingTimeMetrics;
        this.circuitBreakerMetrics = circuitBreakerMetrics;
    }
    
    @Override
    public ProcessingResult handleTransaction(ConsumerRecord<String, GenericRecord> consumerRecord) {
        ProcessingContext context = new ProcessingContext(consumerRecord);
        return handleTransaction(context);
    }
    
    @Override
    public ProcessingResult handleTransaction(ProcessingContext context) {
        String transactionId = context.getTransactionId();
        long startTime = System.currentTimeMillis();
        
        logger.info("Starting transaction processing for: {}", transactionId);
        
        try {
            totalMessagesProcessed.incrementAndGet();
            
            // Check circuit breaker before processing
            if (!circuitBreakerMetrics.isOperationAllowed()) {
                circuitBreakerMetrics.recordRejected();
                logger.warn("Transaction processing rejected by circuit breaker for: {}", transactionId);
                return ProcessingResult.systemError(new RuntimeException("Circuit breaker is OPEN"), 
                                                   System.currentTimeMillis() - startTime);
            }
            
            // Step 1: Idempotency Check
            long idempotencyStartTime = System.currentTimeMillis();
            IdempotencyResult idempotencyResult = idempotencyHandler.checkAndRegister(context);
            long idempotencyDuration = System.currentTimeMillis() - idempotencyStartTime;
            processingTimeMetrics.recordIdempotencyTime(idempotencyDuration);
            
            if (!idempotencyResult.isNewMessage()) {
                duplicateMessages.incrementAndGet();
                circuitBreakerMetrics.recordSuccess(); // Duplicate detection is considered success
                logger.info("Duplicate message detected for transaction: {} - MUID: {}", 
                           transactionId, idempotencyResult.getMuid());
                return ProcessingResult.duplicate(idempotencyResult.getMuid(), 
                                                System.currentTimeMillis() - startTime);
            }
            
            // Step 2: Message Parsing & Enrichment
            long parsingStartTime = System.currentTimeMillis();
            ParsingResult parsingResult = parsingHandler.parseAndEnrich(context);
            long parsingDuration = System.currentTimeMillis() - parsingStartTime;
            processingTimeMetrics.recordParsingTime(parsingDuration);
            
            if (!parsingResult.isSuccess()) {
                parsingFailures.incrementAndGet();
                circuitBreakerMetrics.recordFailure();
                logger.warn("Message parsing failed for transaction: {} - Error: {}", 
                           transactionId, parsingResult.getErrorMessage());
                return ProcessingResult.parsingFailed(parsingResult.getErrorMessage(), 
                                                    System.currentTimeMillis() - startTime);
            }
            
            // Step 3: Validation
            long validationStartTime = System.currentTimeMillis();
            com.anz.fastpayment.inward.scheme.validation.model.ValidationResult validationResult = 
                validationHandler.validate(parsingResult.getMessagePayload(), transactionId);
            long validationDuration = System.currentTimeMillis() - validationStartTime;
            processingTimeMetrics.recordValidationTime(validationDuration);
            
            if (!validationHandler.isValidationSuccessful(validationResult)) {
                validationFailures.incrementAndGet();
                circuitBreakerMetrics.recordFailure();
                List<String> validationErrors = validationHandler.getValidationErrors(validationResult);
                logger.warn("Validation failed for transaction: {} - Errors: {}", 
                           transactionId, validationErrors);
                
                // Create failure response with validation errors in Trailer
                BusinessProcessingResult failureResult = businessHandler.createFailureResponse(
                    context.getAvroMessage(), transactionId, validationErrors);
                return ProcessingResult.success(failureResult.getResponseMessage(), 
                                               System.currentTimeMillis() - startTime);
            }
            
            // Step 4: Create Response (No business processing needed)
            long responseStartTime = System.currentTimeMillis();
            BusinessProcessingResult successResult = businessHandler.createSuccessResponse(
                context.getAvroMessage(), transactionId);
            long responseDuration = System.currentTimeMillis() - responseStartTime;
            processingTimeMetrics.recordResponseCreationTime(responseDuration);
            
            // Success - Update idempotency status
            idempotencyHandler.updateProcessingStatus(idempotencyResult.getMuid(), "COMPLETED");
            
            successfulMessages.incrementAndGet();
            circuitBreakerMetrics.recordSuccess();
            long processingDuration = System.currentTimeMillis() - startTime;
            processingTimeMetrics.recordOverallProcessingTime(processingDuration);
            
            logger.info("Successfully processed transaction: {} in {}ms", transactionId, processingDuration);
            return ProcessingResult.success(successResult.getResponseMessage(), processingDuration);
            
        } catch (Exception e) {
            systemErrors.incrementAndGet();
            circuitBreakerMetrics.recordFailure();
            long processingDuration = System.currentTimeMillis() - startTime;
            processingTimeMetrics.recordOverallProcessingTime(processingDuration);
            
            logger.error("System error during transaction processing for: {} - Error: {}", 
                        transactionId, e.getMessage(), e);
            
            // Try to update idempotency status if possible
            try {
                String muid = idempotencyHandler.extractMuid(context);
                idempotencyHandler.updateProcessingStatus(muid, "FAILED");
            } catch (Exception statusUpdateError) {
                logger.warn("Failed to update processing status after system error: {}", 
                           statusUpdateError.getMessage());
            }
            
            return ProcessingResult.systemError(e, processingDuration);
        }
    }
    
    @Override
    public Map<String, Long> getProcessingMetrics() {
        return Map.of(
            "totalMessagesProcessed", totalMessagesProcessed.get(),
            "successfulMessages", successfulMessages.get(),
            "duplicateMessages", duplicateMessages.get(),
            "parsingFailures", parsingFailures.get(),
            "validationFailures", validationFailures.get(),
            "businessProcessingFailures", businessProcessingFailures.get(),
            "systemErrors", systemErrors.get()
        );
    }
    
    @Override
    public void resetProcessingMetrics() {
        totalMessagesProcessed.set(0);
        successfulMessages.set(0);
        duplicateMessages.set(0);
        parsingFailures.set(0);
        validationFailures.set(0);
        businessProcessingFailures.set(0);
        systemErrors.set(0);
        
        logger.info("Processing metrics reset");
    }
    
    @Override
    public Map<String, Object> getProcessingStatistics() {
        long total = totalMessagesProcessed.get();
        long successful = successfulMessages.get();
        long failed = total - successful;
        
        double successRate = total > 0 ? (double) successful / total * 100 : 0.0;
        double failureRate = total > 0 ? (double) failed / total * 100 : 0.0;
        
        Map<String, Object> statistics = new HashMap<>();
        statistics.put("totalMessagesProcessed", total);
        statistics.put("successfulMessages", successful);
        statistics.put("failedMessages", failed);
        statistics.put("successRate", String.format("%.2f%%", successRate));
        statistics.put("failureRate", String.format("%.2f%%", failureRate));
        statistics.put("duplicateMessages", duplicateMessages.get());
        statistics.put("parsingFailures", parsingFailures.get());
        statistics.put("validationFailures", validationFailures.get());
        statistics.put("businessProcessingFailures", businessProcessingFailures.get());
        statistics.put("systemErrors", systemErrors.get());
        
        return statistics;
    }
}
