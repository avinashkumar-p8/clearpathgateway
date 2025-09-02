package com.anz.fastpayment.inward.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Transaction Message POJO for Avro serialization/deserialization
 * Represents an incoming transaction that needs to be processed
 */
public class TransactionMessage {

    @JsonProperty("transactionId")
    @NotBlank(message = "Transaction ID is required")
    @Size(max = 50, message = "Transaction ID must not exceed 50 characters")
    private String transactionId;

    @JsonProperty("amount")
    @NotNull(message = "Amount is required")
    @Positive(message = "Amount must be positive")
    private BigDecimal amount;

    @JsonProperty("currency")
    @NotBlank(message = "Currency is required")
    @Size(min = 3, max = 3, message = "Currency must be exactly 3 characters")
    private String currency;

    @JsonProperty("senderAccount")
    @NotBlank(message = "Sender account is required")
    @Size(max = 50, message = "Sender account must not exceed 50 characters")
    private String senderAccount;

    @JsonProperty("receiverAccount")
    @NotBlank(message = "Receiver account is required")
    @Size(max = 50, message = "Receiver account must not exceed 50 characters")
    private String receiverAccount;

    @JsonProperty("transactionType")
    @NotBlank(message = "Transaction type is required")
    private String transactionType;

    @JsonProperty("description")
    @Size(max = 200, message = "Description must not exceed 200 characters")
    private String description;

    @JsonProperty("timestamp")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime timestamp;

    @JsonProperty("priority")
    private String priority;

    @JsonProperty("reference")
    @Size(max = 100, message = "Reference must not exceed 100 characters")
    private String reference;

    // Default constructor for Avro
    public TransactionMessage() {
        this.transactionId = UUID.randomUUID().toString();
        this.timestamp = LocalDateTime.now();
    }

    // Constructor with required fields
    public TransactionMessage(String transactionId, BigDecimal amount, String currency, 
                           String senderAccount, String receiverAccount, String transactionType) {
        this.transactionId = transactionId;
        this.amount = amount;
        this.currency = currency;
        this.senderAccount = senderAccount;
        this.receiverAccount = receiverAccount;
        this.transactionType = transactionType;
        this.timestamp = LocalDateTime.now();
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

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
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

    @Override
    public String toString() {
        return "TransactionMessage{" +
                "transactionId='" + transactionId + '\'' +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                ", senderAccount='" + senderAccount + '\'' +
                ", receiverAccount='" + receiverAccount + '\'' +
                ", transactionType='" + transactionType + '\'' +
                ", description='" + description + '\'' +
                ", timestamp=" + timestamp +
                ", priority='" + priority + '\'' +
                ", reference='" + reference + '\'' +
                '}';
    }
}
