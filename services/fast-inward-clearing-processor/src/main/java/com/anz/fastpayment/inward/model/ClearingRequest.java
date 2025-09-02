package com.anz.fastpayment.inward.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Clearing Request model for currency validation
 * Uses Java 21 record for immutability and concise syntax
 */
public record ClearingRequest(
    
    @JsonProperty("transactionId")
    String transactionId,
    
    @JsonProperty("amount")
    BigDecimal amount,
    
    @JsonProperty("currency")
    String currency,
    
    @JsonProperty("senderAccount")
    String senderAccount,
    
    @JsonProperty("receiverAccount")
    String receiverAccount,
    
    @JsonProperty("transactionType")
    String transactionType,
    
    @JsonProperty("country")
    String country,
    
    @JsonProperty("description")
    String description,
    
    @JsonProperty("timestamp")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    LocalDateTime timestamp,
    
    @JsonProperty("priority")
    String priority,
    
    @JsonProperty("reference")
    String reference
) {
    
    /**
     * Canonical constructor with basic defaults
     */
    public ClearingRequest {
        if (timestamp == null) {
            timestamp = LocalDateTime.now();
        }
        
        if (priority == null) {
            priority = "NORMAL";
        }
    }
    
    /**
     * Factory method for creating from TransactionMessage
     */
    public static ClearingRequest fromTransactionMessage(TransactionMessage message, String country) {
        return new ClearingRequest(
            message.getTransactionId(),
            message.getAmount(),
            message.getCurrency(),
            message.getSenderAccount(),
            message.getReceiverAccount(),
            message.getTransactionType(),
            country,
            message.getDescription(),
            message.getTimestamp(),
            message.getPriority(),
            message.getReference()
        );
    }
}

