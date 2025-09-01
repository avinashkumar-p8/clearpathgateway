# Clearing Request Currency Validation System

## Overview

The InwardClearingProcessor now includes a focused currency validation system that implements currency-country validation requirements. This system ensures that all incoming clearing requests have valid currency-country combinations before processing.

## Architecture Components

### 1. Core Validation Models

#### `ClearingRequest` (Java 21 Record)
- **Purpose**: Immutable data model for clearing request currency validation
- **Features**: 
  - Uses Java 21 record for immutability and concise syntax
  - Basic structure without validation annotations
  - Factory method for creating from `TransactionMessage`

#### `ValidationResult` (Java 21 Record)
- **Purpose**: Represents the result of currency validation operations
- **Features**:
  - Immutable validation status
  - Collection of validation errors
  - Factory methods for success/failure scenarios
  - Helper methods for error checking

#### `ValidationError` (Java 21 Record)
- **Purpose**: Detailed error information for currency validation failures
- **Features**:
  - Field-specific error details
  - Error severity levels (ERROR, WARNING, INFO)
  - Error codes for categorization
  - Timestamp for audit trails

### 2. Validation Service

#### `ClearingRequestValidator`
- **Purpose**: Currency validation service implementing currency-country validation logic
- **Features**:
  - **Currency-Country Validation**: Ensures valid currency-country combinations
  - **Currency Lookup**: Get valid currencies for a country
  - **Country Lookup**: Get valid countries for a currency
  - **Validation Check**: Check if currency-country combination is valid

### 3. Exception Handling

#### `InvalidCurrencyCountryException` (Sealed Class)
- **Purpose**: Custom exception for invalid currency-country combinations
- **Features**:
  - Uses Java 21 sealed class pattern
  - Specific exception types for currency vs. country issues
  - Includes both currency and country information

#### `ClearingExceptionHandler` (Controller Advice)
- **Purpose**: Centralized exception handling for currency validation errors
- **Features**:
  - Global exception handler with proper HTTP status codes
  - Structured error responses
  - Comprehensive logging for debugging

### 4. REST API

#### `ClearingValidationController`
- **Purpose**: REST endpoints for currency validation operations
- **Endpoints**:
  - `POST /api/v1/clearing/validation/validate` - Validate clearing request currency
  - `GET /api/v1/clearing/validation/currencies/{country}` - Get valid currencies for country
  - `GET /api/v1/clearing/validation/countries/{currency}` - Get valid countries for currency
  - `GET /api/v1/clearing/validation/check` - Check currency-country validity
  - `GET /api/v1/clearing/validation/health` - Health check

## Currency Validation Rules

### Currency-Country Validation
- **SGD**: Singapore (SG)
- **USD**: United States (US), Singapore (SG), Hong Kong (HK), Japan (JP)
- **EUR**: Germany (DE), France (FR), Italy (IT), Spain (ES), Netherlands (NL)
- **GBP**: United Kingdom (GB)
- **JPY**: Japan (JP)
- **HKD**: Hong Kong (HK)
- **CNY**: China (CN), Singapore (SG), Hong Kong (HK)
- **AUD**: Australia (AU)
- **CAD**: Canada (CA)

## Integration

### 1. Existing Service Integration
The currency validation system is integrated into the existing `ClearingProcessorServiceImpl`:
- Replaces comprehensive validation with focused currency validation
- Maintains backward compatibility
- Enhances error reporting and logging

### 2. Kafka Integration
- Currency validation occurs before Kafka message processing
- Prevents invalid currency-country combinations from entering the processing pipeline
- Maintains 4.5-second SLA compliance

## Testing

### 1. Unit Tests
- **Coverage**: Currency validation scenarios including edge cases
- **Framework**: JUnit 5 with Mockito
- **Test Cases**: 
  - Successful currency validation
  - Invalid currency-country combinations
  - Currency and country lookup methods
  - Null and edge case handling

### 2. Test Execution
```bash
mvn test
```

## Usage Examples

### 1. Basic Currency Validation
```java
@Autowired
private ClearingRequestValidator validator;

ClearingRequest request = new ClearingRequest(...);
ValidationResult result = validator.validateClearingRequest(request);

if (result.isValid()) {
    // Process the request
} else {
    // Handle validation errors
    result.getErrors().forEach(error -> 
        log.error("Validation error: {} - {}", error.field(), error.message()));
}
```

### 2. Currency-Country Check
```java
boolean isValid = validator.isValidCurrencyCountry("USD", "SG");
List<String> validCurrencies = validator.getValidCurrenciesForCountry("SG");
List<String> validCountries = validator.getValidCountriesForCurrency("USD");
```

### 3. REST API Usage
```bash
# Validate a clearing request currency
curl -X POST http://localhost:8080/api/v1/clearing/validation/validate \
  -H "Content-Type: application/json" \
  -d @clearing-request.json

# Check currency-country combination
curl "http://localhost:8080/api/v1/clearing/validation/check?currency=USD&country=SG"
```

## Benefits

### 1. Focused Validation
- Currency validation only, keeping the system simple
- Fast validation using Java 21 features
- Clear error messages for debugging

### 2. Data Integrity
- Ensures valid currency-country combinations
- Prevents invalid data from entering the system

### 3. Performance
- Fast validation using Java 21 features
- Efficient data structures for currency-country mappings
- Minimal impact on processing SLA

### 4. Maintainability
- Clean, modular design
- Comprehensive test coverage
- Clear separation of concerns
- Easy to extend with additional currency rules

## Future Enhancements

### 1. Dynamic Configuration
- Externalize currency-country rules to configuration files
- Runtime updates without redeployment
- Environment-specific currency rules

### 2. Advanced Currency Validation
- Real-time currency exchange rate validation
- Dynamic currency availability based on market conditions
- Multi-currency support expansion

### 3. Monitoring and Alerting
- Currency validation metrics and dashboards
- Real-time validation failure alerts
- Performance monitoring and optimization

## Dependencies

- **Java 21**: Records, sealed classes, pattern matching
- **Spring Boot 3.2.1**: REST API, dependency injection
- **SLF4J**: Comprehensive logging
- **JUnit 5**: Unit testing framework
- **Mockito**: Mocking framework for testing

