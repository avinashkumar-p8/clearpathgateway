-- Spanner Database Initialization Script for Fast Inward Clearing Processor
-- This script creates the necessary tables for the inward clearing processor

-- Create MessageUniqueId table for idempotency
CREATE TABLE MessageUniqueId (
    MUID STRING(255) NOT NULL,
    ProcessedAt TIMESTAMP NOT NULL,
    Status STRING(50) NOT NULL,
    ErrorMessage STRING(MAX),
    CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (MUID);

-- Create Currency table for validation
CREATE TABLE Currency (
    Code STRING(3) NOT NULL,
    Name STRING(100) NOT NULL,
    IsActive BOOL NOT NULL DEFAULT (true),
    ValidCountries ARRAY<STRING(2)>,
    CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (Code);

-- Create Country table for validation
CREATE TABLE Country (
    Code STRING(2) NOT NULL,
    Name STRING(100) NOT NULL,
    IsActive BOOL NOT NULL DEFAULT (true),
    Region STRING(50),
    CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (Code);

-- Insert sample currency data
INSERT INTO Currency (Code, Name, IsActive, ValidCountries, CreatedAt, UpdatedAt) VALUES
('SGD', 'Singapore Dollar', true, ['SG'], PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('USD', 'US Dollar', true, ['US'], PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('EUR', 'Euro', true, ['DE', 'FR', 'IT', 'ES'], PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('GBP', 'British Pound', true, ['GB'], PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('JPY', 'Japanese Yen', true, ['JP'], PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('AUD', 'Australian Dollar', true, ['AU'], PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('CAD', 'Canadian Dollar', true, ['CA'], PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('CHF', 'Swiss Franc', true, ['CH'], PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('HKD', 'Hong Kong Dollar', true, ['HK'], PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('CNY', 'Chinese Yuan', true, ['CN'], PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP());

-- Insert sample country data
INSERT INTO Country (Code, Name, IsActive, Region, CreatedAt, UpdatedAt) VALUES
('SG', 'Singapore', true, 'Asia', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('US', 'United States', true, 'North America', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('GB', 'United Kingdom', true, 'Europe', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('DE', 'Germany', true, 'Europe', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('FR', 'France', true, 'Europe', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('IT', 'Italy', true, 'Europe', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('ES', 'Spain', true, 'Europe', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('JP', 'Japan', true, 'Asia', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('AU', 'Australia', true, 'Oceania', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('CA', 'Canada', true, 'North America', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('CH', 'Switzerland', true, 'Europe', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('HK', 'Hong Kong', true, 'Asia', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('CN', 'China', true, 'Asia', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('MY', 'Malaysia', true, 'Asia', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('TH', 'Thailand', true, 'Asia', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('ID', 'Indonesia', true, 'Asia', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('PH', 'Philippines', true, 'Asia', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('VN', 'Vietnam', true, 'Asia', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('IN', 'India', true, 'Asia', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP()),
('KR', 'South Korea', true, 'Asia', PENDING_COMMIT_TIMESTAMP(), PENDING_COMMIT_TIMESTAMP());
