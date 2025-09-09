package com.anz.fastpayment.router.model;

import com.google.cloud.spring.data.spanner.core.mapping.Column;
import com.google.cloud.spring.data.spanner.core.mapping.PrimaryKey;
import com.google.cloud.spring.data.spanner.core.mapping.Table;

@Table(name = "DedupeKeys")
public class DedupeKey {

    @PrimaryKey
    @Column(name = "basis")
    private String basis;

    @Column(name = "created_at")
    private java.time.Instant createdAt;

    public DedupeKey() {}

    public DedupeKey(String basis, java.time.Instant createdAt) {
        this.basis = basis;
        this.createdAt = createdAt;
    }

    public String getBasis() {
        return basis;
    }

    public void setBasis(String basis) {
        this.basis = basis;
    }

    public java.time.Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(java.time.Instant createdAt) {
        this.createdAt = createdAt;
    }
}






