# Environment Variables Configuration

This document describes the environment variables used by the Clear Path Gateway services.

## Kafka Configuration

### KAFKA_BOOTSTRAP_SERVERS
- **Description**: Kafka bootstrap servers connection string
- **Default**: `kafka:29092` (for Docker Compose)
- **Example**: `kafka:29092` or `localhost:9092` (for local development)
- **Used by**: All services that produce/consume Kafka messages

### SCHEMA_REGISTRY_URL
- **Description**: URL for the Avro Schema Registry
- **Default**: `http://schema-registry:8081` (for Docker Compose)
- **Example**: `http://localhost:8081` (for local development)
- **Used by**: All services that produce/consume Avro messages

### SCHEMA_REGISTRY_USERNAME
- **Description**: Username for Schema Registry authentication (required for production Confluent deployments)
- **Default**: None (no authentication for local development)
- **Example**: `prod_user`
- **Used by**: All services that produce/consume Avro messages
- **Note**: Set in deployment secrets/configuration store, do not hardcode in code

### SCHEMA_REGISTRY_PASSWORD
- **Description**: Password for Schema Registry authentication (required for production Confluent deployments)
- **Default**: None (no authentication for local development)
- **Example**: `********` (stored securely in secrets)
- **Used by**: All services that produce/consume Avro messages
- **Note**: Set in deployment secrets/configuration store, do not hardcode in code

### SCHEMA_REGISTRY_BASIC_AUTH
- **Description**: Combined username:password for Schema Registry authentication (alternative to separate variables)
- **Default**: None (no authentication for local development)
- **Example**: `prod_user:secure_password_123`
- **Used by**: All services that produce/consume Avro messages
- **Note**: When both `SCHEMA_REGISTRY_BASIC_AUTH` and separate username/password variables are present, the combined variable takes precedence

### SCHEMA_REGISTRY_AUTH_CREDENTIALS_SOURCE
- **Description**: Source type for Schema Registry authentication credentials (required for Confluent deployments)
- **Default**: `USER_INFO` (when authentication is enabled)
- **Example**: `USER_INFO`
- **Used by**: All services that produce/consume Avro messages
- **Note**: This property must be set to `USER_INFO` for basic authentication to work with Confluent Schema Registry

> **⚠️ Schema Registry Authentication Important**:
> - **Local Development**: No authentication required, uses default Docker Compose setup
> - **Production Confluent**: **MUST** set `SCHEMA_REGISTRY_USERNAME` and `SCHEMA_REGISTRY_PASSWORD`
> - **Security**: Store credentials in Kubernetes secrets, HashiCorp Vault, or equivalent secret management system
> - **Never hardcode** authentication credentials in code, configuration files, or commit them to version control
>
> **Configuration Examples**:
> 
> **Option 1: Separate Username/Password Variables (Recommended)**
> ```yaml
> spring:
>   kafka:
>     properties:
>       schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8081}
>       schema.registry.basic.auth.credentials.source: USER_INFO
>       schema.registry.basic.auth.user.info: ${SCHEMA_REGISTRY_USERNAME:}:${SCHEMA_REGISTRY_PASSWORD:}
> ```
> 
> **Option 2: Single Combined Authentication Variable**
> ```yaml
> spring:
>   kafka:
>     properties:
>       schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8081}
>       schema.registry.basic.auth.credentials.source: USER_INFO
>       schema.registry.basic.auth.user.info: ${SCHEMA_REGISTRY_BASIC_AUTH:}
> ```
> 
> **Environment Variable Precedence**:
> - **Combined Variable**: `SCHEMA_REGISTRY_BASIC_AUTH` (e.g., `username:password`)
> - **Separate Variables**: `SCHEMA_REGISTRY_USERNAME` and `SCHEMA_REGISTRY_PASSWORD`
> - **Precedence**: When both are present, the **combined variable takes precedence** for simplicity

## Service Configuration

### SPRING_PROFILES_ACTIVE
- **Description**: Spring Boot profile to activate
- **Default**: `local`
- **Options**: `local`, `docker`, `production`
- **Used by**: All Spring Boot services

### DATABASE_URL
- **Description**: Database connection URL
- **Default**: `jdbc:postgresql://postgres:5432/payment_gateway`
- **Used by**: Services that require database access

### DATABASE_USERNAME
- **Description**: Database username
- **Default**: `dev_user`
- **Used by**: Services that require database access

### DATABASE_PASSWORD
- **Description**: Database password
- **Default**: `dev_password`
- **Used by**: Services that require database access

## Redis Configuration

### REDIS_HOST
- **Description**: Redis server hostname
- **Default**: `redis` (for Docker Compose), `localhost` (for local development)
- **Used by**: Services that use Redis for caching

### REDIS_PORT
- **Description**: Redis server port
- **Default**: `6379`
- **Used by**: Services that use Redis for caching

## External Service URLs

### FAST_ROUTER_SERVICE_URL
- **Description**: URL for the Fast Router Service
- **Default**: `http://fast-router-service:8080`
- **Used by**: Services that communicate with the router

### FAST_LIQUIDITY_SERVICE_URL
- **Description**: URL for the Fast Liquidity Service
- **Default**: `http://fast-liquidity-service:8084`
- **Used by**: Services that require liquidity checks

### FAST_INWARD_PROCESSOR_URL
- **Description**: URL for the Fast Inward Clearing Processor
- **Default**: `http://fast-inward-clearing-processor:8081`
- **Used by**: Services that communicate with the inward processor

### FAST_OUTWARD_PROCESSOR_URL
- **Description**: URL for the Fast Outward Clearing Processor
- **Default**: `http://fast-outward-clearing-processor:8082`
- **Used by**: Services that communicate with the outward processor

### FAST_SENDER_SERVICE_URL
- **Description**: URL for the Fast Sender Service
- **Default**: `http://fast-sender-service:8083`
- **Used by**: Services that communicate with the sender service

### FAST_AVAILABILITY_SERVICE_URL
- **Description**: URL for the Fast Availability Service
- **Default**: `http://fast-availability-service:8085`
- **Used by**: Services that communicate with the availability service

### G3_HOST_ENDPOINT
- **Description**: G3 Host system endpoint
- **Default**: `http://localhost:8443/g3host` (local), `https://g3host.anz.com/api` (production)
- **Used by**: Services that communicate with G3 Host

### CPG_ENDPOINT
- **Description**: CPG Gateway endpoint
- **Default**: `http://localhost:8444/cpg` (local), `https://cpg.anz.com/gateway` (production)
- **Used by**: Services that communicate with CPG

## Security Configuration

### JWT_SECRET
- **Description**: Secret key for JWT token signing
- **Default**: `dev-secret-key-change-in-production`
- **Used by**: Services that handle authentication

### MTLS_ENABLED
- **Description**: Enable mutual TLS authentication
- **Default**: `false`
- **Used by**: Services that support mTLS

## SLA Configuration

### INWARD_PAYMENT_SLA_SECONDS
- **Description**: SLA timeout for inward payment processing
- **Default**: `4.5`
- **Used by**: Inward clearing processor

### OUTWARD_PAYMENT_SLA_SECONDS
- **Description**: SLA timeout for outward payment processing
- **Default**: `2.0`
- **Used by**: Outward clearing processor

## Liquidity Configuration

### NET_DEBIT_CAP_SGD
- **Description**: Net debit cap for SGD transactions
- **Default**: `500000000.00`
- **Used by**: Liquidity service

### REGULATORY_BUFFER_SGD
- **Description**: Regulatory buffer for SGD transactions
- **Default**: `50000000.00`
- **Used by**: Liquidity service

## Environment-Specific Configuration

### Local Development
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export SCHEMA_REGISTRY_URL=http://localhost:8081
export REDIS_HOST=localhost
export DATABASE_URL=jdbc:postgresql://localhost:5432/payment_gateway

# Service URLs for local development
export FAST_ROUTER_SERVICE_URL=http://localhost:8080
export FAST_LIQUIDITY_SERVICE_URL=http://localhost:8084
export FAST_INWARD_PROCESSOR_URL=http://localhost:8081
export FAST_OUTWARD_PROCESSOR_URL=http://localhost:8082
export FAST_SENDER_SERVICE_URL=http://localhost:8083
export FAST_AVAILABILITY_SERVICE_URL=http://localhost:8085
```

### Docker Compose
```bash
export KAFKA_BOOTSTRAP_SERVERS=kafka:29092
export SCHEMA_REGISTRY_URL=http://schema-registry:8081
export REDIS_HOST=redis
export DATABASE_URL=jdbc:postgresql://postgres:5432/payment_gateway

# Service URLs for Docker Compose
export FAST_ROUTER_SERVICE_URL=http://fast-router-service:8080
export FAST_LIQUIDITY_SERVICE_URL=http://fast-liquidity-service:8084
export FAST_INWARD_PROCESSOR_URL=http://fast-inward-clearing-processor:8081
export FAST_OUTWARD_PROCESSOR_URL=http://fast-outward-clearing-processor:8082
export FAST_SENDER_SERVICE_URL=http://fast-sender-service:8083
export FAST_AVAILABILITY_SERVICE_URL=http://fast-availability-service:8085
```

### Production
```bash
export KAFKA_BOOTSTRAP_SERVERS=kafka-cluster:9092
export SCHEMA_REGISTRY_URL=https://schema-registry.company.com

# Option 1: Separate username/password variables
export SCHEMA_REGISTRY_USERNAME=prod_user
export SCHEMA_REGISTRY_PASSWORD=********

# Option 2: Combined authentication variable (takes precedence if both are set)
export SCHEMA_REGISTRY_BASIC_AUTH=prod_user:secure_password_123

# Required for Confluent Schema Registry authentication
export SCHEMA_REGISTRY_AUTH_CREDENTIALS_SOURCE=USER_INFO

export REDIS_HOST=redis-cluster.company.com
export DATABASE_URL=jdbc:postgresql://db-cluster.company.com:5432/payment_gateway
```

## Configuration Files

The environment variables are configured in:
- `docker-compose.yml` - For Docker Compose environment
- `scripts/dev-setup.sh` - Generates `.env.template`
- `services/*/src/main/resources/application.yml` - Service-specific configuration
- `services/*/k8s/deployment.yaml` - Kubernetes deployment configuration

## Notes

- All services now use environment variables for Kafka and Schema Registry configuration
- The default values are set for Docker Compose environment
- Local development requires setting appropriate localhost values
- Production deployments should override all sensitive configuration values
- Schema Registry URL is required for all services that handle Avro messages
- **Schema Registry Authentication**: Production Confluent deployments require authentication credentials and the `SCHEMA_REGISTRY_AUTH_CREDENTIALS_SOURCE=USER_INFO` property. Use either separate `SCHEMA_REGISTRY_USERNAME`/`SCHEMA_REGISTRY_PASSWORD` variables or a combined `SCHEMA_REGISTRY_BASIC_AUTH` variable. When both are present, the combined variable takes precedence. Never hardcode these values in code or configuration files.
- **Service Port Mapping**: Docker Compose maps container port 8080 to different host ports for each service to avoid conflicts. Use the documented host ports for external access.
