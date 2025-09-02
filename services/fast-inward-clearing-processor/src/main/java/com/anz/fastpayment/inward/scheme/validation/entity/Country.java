package com.anz.fastpayment.inward.scheme.validation.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Country entity for banking operations
 * Represents supported countries with validation rules
 */
@Entity
@Table(name = "countries")
public class Country {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "code", unique = true, nullable = false, length = 2)
    private String code;
    
    @Column(name = "name", nullable = false, length = 100)
    private String name;
    
    @Column(name = "iso_code", unique = true, length = 3)
    private String isoCode;
    
    @Column(name = "is_active", nullable = false)
    private boolean active = true;
    
    @Column(name = "region", length = 50)
    private String region;
    
    @Column(name = "sub_region", length = 50)
    private String subRegion;
    
    @ElementCollection
    @CollectionTable(name = "country_currency_mappings", 
                    joinColumns = @JoinColumn(name = "country_id"))
    @Column(name = "currency_code", length = 3)
    private List<String> validCurrencies;
    
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
    
    @Column(name = "created_by", length = 50)
    private String createdBy;
    
    @Column(name = "updated_by", length = 50)
    private String updatedBy;
    
    // Default constructor
    public Country() {
        this.createdAt = LocalDateTime.now();
    }
    
    // Constructor with required fields
    public Country(String code, String name) {
        this();
        this.code = code;
        this.name = name;
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public String getCode() {
        return code;
    }
    
    public void setCode(String code) {
        this.code = code;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getIsoCode() {
        return isoCode;
    }
    
    public void setIsoCode(String isoCode) {
        this.isoCode = isoCode;
    }
    
    public boolean isActive() {
        return active;
    }
    
    public void setActive(boolean active) {
        this.active = active;
    }
    
    public String getRegion() {
        return region;
    }
    
    public void setRegion(String region) {
        this.region = region;
    }
    
    public String getSubRegion() {
        return subRegion;
    }
    
    public void setSubRegion(String subRegion) {
        this.subRegion = subRegion;
    }
    
    public List<String> getValidCurrencies() {
        return validCurrencies;
    }
    
    public void setValidCurrencies(List<String> validCurrencies) {
        this.validCurrencies = validCurrencies;
    }
    
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
    
    public String getCreatedBy() {
        return createdBy;
    }
    
    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }
    
    public String getUpdatedBy() {
        return updatedBy;
    }
    
    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }
    
    @PreUpdate
    public void preUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
    
    @Override
    public String toString() {
        return "Country{" +
                "id=" + id +
                ", code='" + code + '\'' +
                ", name='" + name + '\'' +
                ", isoCode='" + isoCode + '\'' +
                ", active=" + active +
                ", region='" + region + '\'' +
                ", subRegion='" + subRegion + '\'' +
                ", validCurrencies=" + validCurrencies +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                ", createdBy='" + createdBy + '\'' +
                ", updatedBy='" + updatedBy + '\'' +
                '}';
    }
}
