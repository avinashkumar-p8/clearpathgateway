package com.anz.fastpayment.inward.scheme.validation.entity;

import com.google.cloud.spring.data.spanner.core.mapping.Column;
import com.google.cloud.spring.data.spanner.core.mapping.PrimaryKey;
import com.google.cloud.spring.data.spanner.core.mapping.Table;
import java.time.LocalDateTime;

/**
 * MessageUniqueId entity for tracking message idempotency
 * Maps to the message_unique_ids table in Spanner
 */
@Table(name = "message_unique_ids")
public class MessageUniqueId {
    
    @PrimaryKey
    @Column(name = "muid")
    private String muid;
    
    @Column(name = "message_topic")
    private String topic;
    
    @Column(name = "message_partition")
    private Integer partition;
    
    @Column(name = "message_offset")
    private Long offset;
    
    @Column(name = "event_payload")
    private String eventPayload;
    
    @Column(name = "processing_status")
    private String processingStatus;
    
    @Column(name = "created_at")
    private LocalDateTime createdAt;
    
    @Column(name = "processed_at")
    private LocalDateTime processedAt;
    
    @Column(name = "is_active")
    private Boolean isActive;
    
    // Default constructor for Spanner
    protected MessageUniqueId() {}
    
    // Constructor for creating new MUID records
    public MessageUniqueId(String muid, String topic, Integer partition, Long offset, String eventPayload) {
        this.muid = muid;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.eventPayload = eventPayload;
        this.processingStatus = "REGISTERED";
        this.createdAt = LocalDateTime.now();
        this.isActive = true;
    }
    
    // Getters and Setters
    public String getMuid() {
        return muid;
    }
    
    public void setMuid(String muid) {
        this.muid = muid;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
    }
    
    public Integer getPartition() {
        return partition;
    }
    
    public void setPartition(Integer partition) {
        this.partition = partition;
    }
    
    public Long getOffset() {
        return offset;
    }
    
    public void setOffset(Long offset) {
        this.offset = offset;
    }
    
    public String getEventPayload() {
        return eventPayload;
    }
    
    public void setEventPayload(String eventPayload) {
        this.eventPayload = eventPayload;
    }
    
    public String getProcessingStatus() {
        return processingStatus;
    }
    
    public void setProcessingStatus(String processingStatus) {
        this.processingStatus = processingStatus;
    }
    
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    public LocalDateTime getProcessedAt() {
        return processedAt;
    }
    
    public void setProcessedAt(LocalDateTime processedAt) {
        this.processedAt = processedAt;
    }
    
    public Boolean getIsActive() {
        return isActive;
    }
    
    public void setIsActive(Boolean isActive) {
        this.isActive = isActive;
    }
    
    // Spanner doesn't have @PreUpdate, so we'll handle this in the service layer
    
    @Override
    public String toString() {
        return "MessageUniqueId{" +
                "muid='" + muid + '\'' +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", processingStatus='" + processingStatus + '\'' +
                ", isActive=" + isActive +
                '}';
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        MessageUniqueId that = (MessageUniqueId) o;
        return muid != null ? muid.equals(that.muid) : that.muid == null;
    }
    
    @Override
    public int hashCode() {
        return muid != null ? muid.hashCode() : 0;
    }
}
