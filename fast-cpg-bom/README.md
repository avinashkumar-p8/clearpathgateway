# Fast CPG BOM (Bill of Materials)

## Overview

The Fast CPG BOM provides centralized dependency management for all APEAFAST-SG ClearPath Gateway services. This ensures consistent versions across all microservices and simplifies maintenance.

## Purpose

- **Centralized Version Management**: All dependency versions are defined in one place
- **Consistency**: Ensures all services use the same versions of dependencies
- **Simplified Maintenance**: Easy to upgrade dependencies across all services
- **Reduced Duplication**: Eliminates redundant dependency management in service POMs

## Structure

The BOM inherits from `spring-boot-starter-parent` and manages versions for:

### Core Technologies
- **Java 21**: Latest LTS with Virtual Threads support
- **Spring Boot 3.2.1**: Latest stable release
- **Spring Cloud 2023.0.0**: Latest stable release
- **Spring Cloud GCP 4.9.0**: Google Cloud Platform integration

### Key Dependencies
- **Kafka 3.6.1**: Message streaming platform
- **Micrometer 1.12.1**: Metrics and observability
- **Resilience4j 2.1.0**: Circuit breakers and resilience patterns
- **Jackson 2.16.1**: JSON processing
- **Testcontainers 1.19.3**: Integration testing

### Build Tools
- **Maven Compiler Plugin 3.12.1**: Java 21 compilation
- **Jib Maven Plugin 3.4.0**: Container building
- **OWASP Dependency Check 8.4.3**: Security scanning

## Usage

### For Service POMs

Services should use this BOM as their parent:

```xml
<parent>
    <groupId>com.anz.fastpayment</groupId>
    <artifactId>fast-cpg-bom</artifactId>
    <version>21.0.0-apeafast-SNAPSHOT</version>
    <relativePath>../../fast-cpg-bom</relativePath>
</parent>
```

### Dependencies Without Versions

When the BOM is the parent, dependencies don't need version specifications:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-web</artifactId>
    <!-- No version needed - managed by BOM -->
</dependency>
```

### Build Plugins

Plugin configurations are also managed by the BOM:

```xml
<plugin>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-maven-plugin</artifactId>
    <!-- Configuration managed by BOM -->
</plugin>
```

## Profiles

### Security Scanning
```bash
mvn clean install -Psecurity-scan
```

### Docker Building
```bash
mvn clean install -Pdocker-build
```

## Version Updates

To update dependency versions:

1. Update the version property in the BOM
2. Build and test all services
3. Commit changes to all services simultaneously

## Benefits

✅ **Simplified Service POMs**: Services focus on their specific dependencies
✅ **Version Consistency**: No version conflicts between services
✅ **Easy Upgrades**: Update one place to upgrade all services
✅ **Security**: Centralized security scanning configuration
✅ **Documentation**: Clear view of all technology versions used

## Services Using This BOM

- fast-router-service
- fast-inward-clearing-processor
- fast-sender-service
- fast-outward-clearing-processor
- fast-availability-service

**Note**: fast-liquidity-service is implemented in Go/Rust and doesn't use this BOM.