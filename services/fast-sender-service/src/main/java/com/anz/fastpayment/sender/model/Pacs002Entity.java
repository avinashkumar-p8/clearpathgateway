package com.anz.fastpayment.sender.model;

import com.google.cloud.spring.data.spanner.core.mapping.Column;
import com.google.cloud.spring.data.spanner.core.mapping.PrimaryKey;
import com.google.cloud.spring.data.spanner.core.mapping.Table;

import java.time.Instant;

@Table(name = "Pacs002Messages")
public class Pacs002Entity {

    @PrimaryKey
    @Column(name = "puid")
    private String puid;

    @Column(name = "unique_id")
    private String uniqueId;

    @Column(name = "created_at")
    private Instant createdAt;

    @Column(name = "xml")
    private String xml;

    @Column(name = "event_json")
    private String eventJson;

    public String getPuid() { return puid; }
    public void setPuid(String puid) { this.puid = puid; }
    public String getUniqueId() { return uniqueId; }
    public void setUniqueId(String uniqueId) { this.uniqueId = uniqueId; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public String getXml() { return xml; }
    public void setXml(String xml) { this.xml = xml; }
    public String getEventJson() { return eventJson; }
    public void setEventJson(String eventJson) { this.eventJson = eventJson; }
}
