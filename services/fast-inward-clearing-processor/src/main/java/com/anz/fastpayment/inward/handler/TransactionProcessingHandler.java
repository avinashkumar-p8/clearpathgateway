package com.anz.fastpayment.inward.handler;

import com.anz.fastpayment.inward.model.ProcessingContext;
import com.anz.fastpayment.inward.model.ProcessingResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.avro.generic.GenericRecord;

/**
 * Main handler for transaction processing pipeline
 * Orchestrates the complete processing flow
 */
public interface TransactionProcessingHandler {
    
    /**
     * Handle transaction processing through the complete pipeline
     * 
     * @param consumerRecord Kafka consumer record containing the message
     * @return ProcessingResult containing the outcome of processing
     */
    ProcessingResult handleTransaction(ConsumerRecord<String, GenericRecord> consumerRecord);
    
    /**
     * Handle transaction processing with processing context
     * 
     * @param context Processing context containing message information
     * @return ProcessingResult containing the outcome of processing
     */
    ProcessingResult handleTransaction(ProcessingContext context);
    
    /**
     * Get processing metrics
     * 
     * @return Map containing processing metrics
     */
    java.util.Map<String, Long> getProcessingMetrics();
    
    /**
     * Reset processing metrics
     */
    void resetProcessingMetrics();
    
    /**
     * Get processing statistics
     * 
     * @return Map containing processing statistics
     */
    java.util.Map<String, Object> getProcessingStatistics();
}
