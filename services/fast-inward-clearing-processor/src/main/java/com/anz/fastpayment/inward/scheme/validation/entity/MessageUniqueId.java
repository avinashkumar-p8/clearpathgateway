package com.anz.fastpayment.inward.scheme.validation.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

/**
 * MessageUniqueId entity for tracking message idempotency
 * Maps to the message_unique_ids table in Spanner
 */
@Entity
@Table(name = "message_unique_ids")
public class MessageUniqueId {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "muid", nullable = false, unique = true, length = 255)
    private String muid;
    
    @Column(name = "message_topic", nullable = false, length = 100)
    private String topic;
    
    @Column(name = "message_partition", nullable = false)
    private Integer partition;
    
    @Column(name = "message_offset", nullable = false)
    private Long offset;
    
    @Column(name = "event_payload", columnDefinition = "TEXT")
    private String eventPayload;
    
    @Column(name = "processing_status", nullable = false, length = 50)
    private String processingStatus;
    
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
    
    @Column(name = "processed_at")
    private LocalDateTime processedAt;
    
    @Column(name = "is_active", nullable = false)
    private Boolean isActive;
    
    // Default constructor for JPA
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
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
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
    
    @PreUpdate
    protected void onUpdate() {
        if ("COMPLETED".equals(this.processingStatus) || "FAILED".equals(this.processingStatus)) {
            this.processedAt = LocalDateTime.now();
        }
    }
    
    @Override
    public String toString() {
        return "MessageUniqueId{" +
                "id=" + id +
                ", muid='" + muid + '\'' +
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
