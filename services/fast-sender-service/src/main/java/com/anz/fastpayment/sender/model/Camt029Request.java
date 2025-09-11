package com.anz.fastpayment.sender.model;

import jakarta.validation.constraints.NotBlank;

public class Camt029Request {

    @NotBlank
    private String puid;

    // messageType of original inbound (camt.056.001.11)
    @NotBlank
    private String messageType;

    // original camt.056 XML
    @NotBlank
    private String originalXml;

    private String error;

    private String uniqueId;

    public String getPuid() { return puid; }
    public void setPuid(String puid) { this.puid = puid; }
    public String getMessageType() { return messageType; }
    public void setMessageType(String messageType) { this.messageType = messageType; }
    public String getOriginalXml() { return originalXml; }
    public void setOriginalXml(String originalXml) { this.originalXml = originalXml; }
    public String getError() { return error; }
    public void setError(String error) { this.error = error; }
    public String getUniqueId() { return uniqueId; }
    public void setUniqueId(String uniqueId) { this.uniqueId = uniqueId; }
}


