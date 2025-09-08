# Fast Inward Clearing Processor - Local Development

This document explains how to run the Fast Inward Clearing Processor in **local development mode**:
- **Application**: Runs locally using Maven/Java
- **Infrastructure**: Runs in Docker (Kafka, Spanner, Schema Registry, Zookeeper)

## 🚀 Quick Start

### Start Everything
```bash
./start-local-dev.sh
```

### Stop Everything
```bash
./stop-local-dev.sh
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Local Development                        │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
│  │   Application   │    │        Docker Services         │ │
│  │   (Local Java)  │◄──►│                                 │ │
│  │                 │    │  ┌─────────┐  ┌──────────────┐  │ │
│  │  Maven/Spring   │    │  │ Kafka   │  │ Spanner      │  │ │
│  │  Boot:8080      │    │  │ :9092   │  │ Emulator     │  │ │
│  └─────────────────┘    │  │         │  │ :9010        │  │ │
│                         │  └─────────┘  └──────────────┘  │ │
│                         │  ┌─────────┐  ┌──────────────┐  │ │
│                         │  │Schema   │  │ Zookeeper    │  │ │
│                         │  │Registry │  │ :2181        │  │ │
│                         │  │:8081    │  │              │  │ │
│                         │  └─────────┘  └──────────────┘  │ │
│                         └─────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## 📋 Prerequisites

- **Java 21** (Zulu JDK recommended)
- **Maven 3.9+**
- **Docker & Docker Compose**

## 🛠️ Manual Setup

### 1. Start Infrastructure Services
```bash
# Start all infrastructure services in Docker
docker-compose up -d

# Check service status
docker-compose ps
```

### 2. Initialize Spanner Database
```bash
# Initialize Spanner database
bash scripts/init-spanner-docker.sh
```

### 3. Start Application Locally
```bash
# Set environment variables
export SPRING_PROFILES_ACTIVE=local
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export SCHEMA_REGISTRY_URL=http://localhost:8081
export GCP_PROJECT_ID=anz-fastpayment-sg
export SPANNER_INSTANCE=payment-gateway
export SPANNER_DATABASE=inward-processor-db
export SPANNER_EMULATOR_HOST=localhost:9010

# Start the application
mvn spring-boot:run
```

## 🌐 Service URLs

- **Application**: http://localhost:8080
- **Health Check**: http://localhost:8080/actuator/health
- **Kafka**: localhost:9092
- **Schema Registry**: http://localhost:8081
- **Spanner Emulator**: http://localhost:9010

## 🔧 Configuration

### Application Configuration
The application uses the `local` profile by default, which is configured in `application.yml`:

```yaml
spring:
  profiles:
    active: local
  kafka:
    bootstrap-servers: localhost:9092
  cloud:
    gcp:
      spanner:
        emulator-host: localhost:9010
```

### Environment Variables
You can override configuration using environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export SCHEMA_REGISTRY_URL=http://localhost:8081
export SPANNER_EMULATOR_HOST=localhost:9010
```

## 🐛 Troubleshooting

### Spanner Connection Issues
```bash
# Check if Spanner emulator is running
docker-compose ps spanner-emulator

# Check Spanner logs
docker-compose logs spanner-emulator

# Test Spanner connectivity
curl http://localhost:9010
```

### Kafka Connection Issues
```bash
# Check if Kafka is running
docker-compose ps kafka

# Check Kafka logs
docker-compose logs kafka

# Test Kafka connectivity
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Application Issues
```bash
# Check application logs
tail -f service.log

# Check if port 8080 is available
lsof -i :8080
```

## 🧪 Running Tests

### Playwright Tests
```bash
cd playwright
./run-tests.sh
```

### Unit Tests
```bash
mvn test
```

## 📊 Monitoring

### Health Checks
- Application: http://localhost:8080/actuator/health
- Metrics: http://localhost:8080/actuator/metrics
- Prometheus: http://localhost:8080/actuator/prometheus

### Logs
- Application logs: `service.log`
- Docker logs: `docker-compose logs [service-name]`

## 🔄 Development Workflow

1. **Start infrastructure**: `./start-local-dev.sh`
2. **Make code changes**
3. **Restart application**: `Ctrl+C` then `mvn spring-boot:run`
4. **Test changes**
5. **Stop everything**: `./stop-local-dev.sh`

## 📝 Notes

- The application runs on port 8080
- All infrastructure services run in Docker containers
- Spanner database is automatically initialized
- Configuration is optimized for local development
- No external dependencies required (everything runs locally)
