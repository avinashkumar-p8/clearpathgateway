package com.anz.fastpayment.router.model;

import com.google.cloud.spring.data.spanner.core.mapping.Column;
import com.google.cloud.spring.data.spanner.core.mapping.PrimaryKey;
import com.google.cloud.spring.data.spanner.core.mapping.Table;

import java.time.Instant;

@Table(name = "UnifiedMessages")
public class UnifiedMessage {

    @PrimaryKey
    @Column(name = "puid")
    private String puid;

    @Column(name = "message_type")
    private String messageType;

    @Column(name = "created_at")
    private Instant createdAt;

    @Column(name = "json")
    private String json;

    public String getPuid() { return puid; }
    public void setPuid(String puid) { this.puid = puid; }
    public String getMessageType() { return messageType; }
    public void setMessageType(String messageType) { this.messageType = messageType; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public String getJson() { return json; }
    public void setJson(String json) { this.json = json; }
}
