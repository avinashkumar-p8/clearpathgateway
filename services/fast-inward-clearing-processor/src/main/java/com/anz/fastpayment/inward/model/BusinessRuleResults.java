package com.anz.fastpayment.inward.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Business Rule Results
 * Stores the results of business rule validations and checks
 */
public class BusinessRuleResults {

    @JsonProperty("isBlocked")
    private boolean isBlocked;

    @JsonProperty("blockReason")
    private String blockReason;

    @JsonProperty("riskScore")
    private int riskScore;

    @JsonProperty("overallStatus")
    private String overallStatus;

    public BusinessRuleResults() {
        this.overallStatus = "PASSED";
        this.riskScore = 0;
    }

    // Getters and Setters
    public boolean isBlocked() {
        return isBlocked;
    }

    public void setBlocked(boolean blocked) {
        isBlocked = blocked;
    }

    public String getBlockReason() {
        return blockReason;
    }

    public void setBlockReason(String blockReason) {
        this.blockReason = blockReason;
    }

    public int getRiskScore() {
        return riskScore;
    }

    public void setRiskScore(int riskScore) {
        this.riskScore = riskScore;
    }

    public String getOverallStatus() {
        return overallStatus;
    }

    public void setOverallStatus(String overallStatus) {
        this.overallStatus = overallStatus;
    }

    @Override
    public String toString() {
        return "BusinessRuleResults{" +
                "isBlocked=" + isBlocked +
                ", blockReason='" + blockReason + '\'' +
                ", riskScore=" + riskScore +
                ", overallStatus='" + overallStatus + '\'' +
                '}';
    }
}
