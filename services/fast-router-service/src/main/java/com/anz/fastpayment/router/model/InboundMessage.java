package com.anz.fastpayment.router.model;

import com.google.cloud.spring.data.spanner.core.mapping.Column;
import com.google.cloud.spring.data.spanner.core.mapping.PrimaryKey;
import com.google.cloud.spring.data.spanner.core.mapping.Table;

import java.time.Instant;

@Table(name = "InboundMessages")
public class InboundMessage {

    @PrimaryKey
    @Column(name = "puid")
    private String puid;

    @Column(name = "channel_id")
    private String channelId;

    @Column(name = "message_type")
    private String messageType;

    @Column(name = "received_at")
    private Instant receivedAt;

    @Column(name = "raw_xml")
    private String rawXml;

    @Column(name = "status")
    private String status;

    @Column(name = "error")
    private String error;

    public String getPuid() { return puid; }
    public void setPuid(String puid) { this.puid = puid; }
    public String getChannelId() { return channelId; }
    public void setChannelId(String channelId) { this.channelId = channelId; }
    public String getMessageType() { return messageType; }
    public void setMessageType(String messageType) { this.messageType = messageType; }
    public Instant getReceivedAt() { return receivedAt; }
    public void setReceivedAt(Instant receivedAt) { this.receivedAt = receivedAt; }
    public String getRawXml() { return rawXml; }
    public void setRawXml(String rawXml) { this.rawXml = rawXml; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getError() { return error; }
    public void setError(String error) { this.error = error; }
}


