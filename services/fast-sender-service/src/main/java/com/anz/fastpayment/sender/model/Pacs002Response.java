package com.anz.fastpayment.sender.model;

public class Pacs002Response {

    private String puid;
    private String status; // ACCEPTED/REJECTED (request handling acknowledgement)

    public Pacs002Response() {}

    public Pacs002Response(String puid, String status) {
        this.puid = puid;
        this.status = status;
    }

    public String getPuid() {
        return puid;
    }

    public void setPuid(String puid) {
        this.puid = puid;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
