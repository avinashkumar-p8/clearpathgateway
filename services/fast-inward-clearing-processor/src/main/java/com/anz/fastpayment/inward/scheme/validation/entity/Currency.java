package com.anz.fastpayment.inward.scheme.validation.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Currency entity for banking operations
 * Represents supported currencies with validation rules
 */
@Entity
@Table(name = "currencies")
public class Currency {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "code", unique = true, nullable = false, length = 3)
    private String code;
    
    @Column(name = "name", nullable = false, length = 100)
    private String name;
    
    @Column(name = "symbol", length = 5)
    private String symbol;
    
    @Column(name = "is_active", nullable = false)
    private boolean active = true;
    
    @Column(name = "decimal_places", nullable = false)
    private int decimalPlaces = 2;
    
    @ElementCollection
    @CollectionTable(name = "currency_country_mappings", 
                    joinColumns = @JoinColumn(name = "currency_id"))
    @Column(name = "country_code", length = 2)
    private List<String> validCountries;
    
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
    
    @Column(name = "created_by", length = 50)
    private String createdBy;
    
    @Column(name = "updated_by", length = 50)
    private String updatedBy;
    
    // Default constructor
    public Currency() {
        this.createdAt = LocalDateTime.now();
    }
    
    // Constructor with required fields
    public Currency(String code, String name) {
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
    
    public String getSymbol() {
        return symbol;
    }
    
    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }
    
    public boolean isActive() {
        return active;
    }
    
    public void setActive(boolean active) {
        this.active = active;
    }
    
    public int getDecimalPlaces() {
        return decimalPlaces;
    }
    
    public void setDecimalPlaces(int decimalPlaces) {
        this.decimalPlaces = decimalPlaces;
    }
    
    public List<String> getValidCountries() {
        return validCountries;
    }
    
    public void setValidCountries(List<String> validCountries) {
        this.validCountries = validCountries;
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
        return "Currency{" +
                "id=" + id +
                ", code='" + code + '\'' +
                ", name='" + name + '\'' +
                ", symbol='" + symbol + '\'' +
                ", active=" + active +
                ", decimalPlaces=" + decimalPlaces +
                ", validCountries=" + validCountries +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                ", createdBy='" + createdBy + '\'' +
                ", updatedBy='" + updatedBy + '\'' +
                '}';
    }
}
