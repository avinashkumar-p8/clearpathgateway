package com.anz.fastpayment.inward.handler;

import com.anz.fastpayment.inward.model.ParsingResult;
import com.anz.fastpayment.inward.model.ProcessingContext;

/**
 * Handler for message parsing and enrichment operations
 * Converts Avro messages to internal data structures
 */
public interface MessageParsingHandler {
    
    /**
     * Parse and enrich message from processing context
     * 
     * @param context Processing context containing Avro message
     * @return ParsingResult with parsed data and transaction message
     */
    ParsingResult parseAndEnrich(ProcessingContext context);
    
    /**
     * Extract message section from Avro message
     * 
     * @param context Processing context
     * @return Map containing message payload for validation
     */
    java.util.Map<String, Object> extractMessageSection(ProcessingContext context);
    
    /**
     * Convert Avro message to TransactionMessage
     * 
     * @param context Processing context
     * @return TransactionMessage object
     */
    com.anz.fastpayment.inward.model.TransactionMessage convertToTransactionMessage(ProcessingContext context);
    
    /**
     * Enrich transaction message with metadata
     * 
     * @param transactionMessage Transaction message to enrich
     * @param context Processing context
     */
    void enrichWithMetadata(com.anz.fastpayment.inward.model.TransactionMessage transactionMessage, ProcessingContext context);
}
