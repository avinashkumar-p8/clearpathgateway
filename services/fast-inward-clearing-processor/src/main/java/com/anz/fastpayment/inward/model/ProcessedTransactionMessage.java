package com.anz.fastpayment.inward.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Processed Transaction Message
 * Represents a transaction message after processing, validation, enrichment, and business rules
 */
public class ProcessedTransactionMessage {

    @JsonProperty("transactionId")
    private String transactionId;

    @JsonProperty("amount")
    private BigDecimal amount;

    @JsonProperty("currency")
    private String currency;

    @JsonProperty("senderAccount")
    private String senderAccount;

    @JsonProperty("receiverAccount")
    private String receiverAccount;

    @JsonProperty("transactionType")
    private String transactionType;

    @JsonProperty("description")
    private String description;

    @JsonProperty("originalTimestamp")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime originalTimestamp;

    @JsonProperty("processingTimestamp")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime processingTimestamp;

    @JsonProperty("processingNodeId")
    private String processingNodeId;

    @JsonProperty("status")
    private String status;

    @JsonProperty("priority")
    private String priority;

    @JsonProperty("reference")
    private String reference;

    @JsonProperty("businessRuleResults")
    private BusinessRuleResults businessRuleResults;



    // Default constructor
    public ProcessedTransactionMessage() {
        this.processingTimestamp = LocalDateTime.now();
    }

    // Constructor from original transaction message
    public ProcessedTransactionMessage(TransactionMessage original) {
        this.transactionId = original.getTransactionId();
        this.amount = original.getAmount();
        this.currency = original.getCurrency();
        this.senderAccount = original.getSenderAccount();
        this.receiverAccount = original.getReceiverAccount();
        this.transactionType = original.getTransactionType();
        this.description = original.getDescription();
        this.originalTimestamp = original.getTimestamp();
        this.processingTimestamp = LocalDateTime.now();
        this.priority = original.getPriority();
        this.reference = original.getReference();
        this.status = "PROCESSED";

        this.processingNodeId = "default-processor";
    }

    // Getters and Setters
    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getSenderAccount() {
        return senderAccount;
    }

    public void setSenderAccount(String senderAccount) {
        this.senderAccount = senderAccount;
    }

    public String getReceiverAccount() {
        return receiverAccount;
    }

    public void setReceiverAccount(String receiverAccount) {
        this.receiverAccount = receiverAccount;
    }

    public String getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(String transactionType) {
        this.transactionType = transactionType;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public LocalDateTime getOriginalTimestamp() {
        return originalTimestamp;
    }

    public void setOriginalTimestamp(LocalDateTime originalTimestamp) {
        this.originalTimestamp = originalTimestamp;
    }

    public LocalDateTime getProcessingTimestamp() {
        return processingTimestamp;
    }

    public void setProcessingTimestamp(LocalDateTime processingTimestamp) {
        this.processingTimestamp = processingTimestamp;
    }

    public String getProcessingNodeId() {
        return processingNodeId;
    }

    public void setProcessingNodeId(String processingNodeId) {
        this.processingNodeId = processingNodeId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
    }

    public String getReference() {
        return reference;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public BusinessRuleResults getBusinessRuleResults() {
        return businessRuleResults;
    }

    public void setBusinessRuleResults(BusinessRuleResults businessRuleResults) {
        this.businessRuleResults = businessRuleResults;
    }



    @Override
    public String toString() {
        return "ProcessedTransactionMessage{" +
                "transactionId='" + transactionId + '\'' +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                ", senderAccount='" + senderAccount + '\'' +
                ", receiverAccount='" + receiverAccount + '\'' +
                ", transactionType='" + transactionType + '\'' +
                ", description='" + description + '\'' +
                ", originalTimestamp=" + originalTimestamp +
                ", processingTimestamp=" + processingTimestamp +
                ", processingNodeId='" + processingNodeId + '\'' +
                ", status='" + status + '\'' +
                ", priority='" + priority + '\'' +
                ", reference='" + reference + '\'' +
                ", businessRuleResults=" + businessRuleResults +

                '}';
    }
}
