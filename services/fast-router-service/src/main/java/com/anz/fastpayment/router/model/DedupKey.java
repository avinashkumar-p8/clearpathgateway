package com.anz.fastpayment.router.model;

import com.google.cloud.spring.data.spanner.core.mapping.Column;
import com.google.cloud.spring.data.spanner.core.mapping.PrimaryKey;
import com.google.cloud.spring.data.spanner.core.mapping.Table;

@Table(name = "DedupKeys")
public class DedupKey {

    @PrimaryKey(keyOrder = 1)
    @Column(name = "message_type")
    private String messageType;

    @PrimaryKey(keyOrder = 2)
    @Column(name = "unique_id")
    private String uniqueId;

    public String getMessageType() { return messageType; }
    public void setMessageType(String messageType) { this.messageType = messageType; }

    public String getUniqueId() { return uniqueId; }
    public void setUniqueId(String uniqueId) { this.uniqueId = uniqueId; }
}




