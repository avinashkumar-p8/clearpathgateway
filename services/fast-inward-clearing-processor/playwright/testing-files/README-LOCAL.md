# Fast Inward Clearing Processor - Docker-Only Development

This document explains how to set up and run the Fast Inward Clearing Processor using **Docker for everything** - no local installations required!

## 🐳 Docker-Only Approach

**Everything runs in Docker containers:**
- ✅ **Kafka** - Message streaming
- ✅ **Zookeeper** - Kafka coordination  
- ✅ **Schema Registry** - Avro schema management
- ✅ **Spanner Emulator** - Database (no GCP required)
- ✅ **Application** - Fast Inward Clearing Processor

## Prerequisites

- **Docker and Docker Compose** (that's it!)
- No Java, Maven, or Google Cloud SDK needed locally

## 🚀 Quick Start

### Option 1: All-in-One Command
```bash
./docker-dev.sh start
```

### Option 2: Step by Step
```bash
# Start infrastructure services
docker-compose up -d zookeeper kafka schema-registry spanner-emulator

# Initialize Spanner database
./docker-dev.sh init-db

# Start application
docker-compose up -d fast-inward-clearing-processor
```

### Stop All Services
```bash
./docker-dev.sh stop
```

## 🛠️ Docker Development Commands

```bash
# Start everything
./docker-dev.sh start

# View logs
./docker-dev.sh logs

# View application logs only
./docker-dev.sh logs-app

# Check service status
./docker-dev.sh status

# Restart services
./docker-dev.sh restart

# Clean up everything
./docker-dev.sh clean

# Run tests
./docker-dev.sh test

# Open shell in application container
./docker-dev.sh shell
```

## Manual Setup

### 1. Start Infrastructure Services
```bash
docker-compose up -d zookeeper kafka schema-registry spanner-emulator
```

### 2. Initialize Spanner Database
```bash
bash scripts/init-spanner-docker.sh
```

### 3. Build and Start Application
```bash
mvn clean compile
docker-compose up -d fast-inward-clearing-processor
```

## Service URLs

- **Application**: http://localhost:8080
- **Health Check**: http://localhost:8080/actuator/health
- **Metrics**: http://localhost:8080/actuator/metrics
- **Schema Registry**: http://localhost:8081
- **Spanner Emulator**: http://localhost:9010

## Configuration

### Environment Variables

The application uses the following environment variables:

- `SPRING_PROFILES_ACTIVE`: Set to `test` for local development
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (default: localhost:9092)
- `SCHEMA_REGISTRY_URL`: Schema Registry URL (default: http://localhost:8081)
- `GCP_PROJECT_ID`: GCP Project ID (default: anz-fastpayment-sg)
- `SPANNER_INSTANCE`: Spanner instance name (default: payment-gateway)
- `SPANNER_DATABASE`: Spanner database name (default: inward-processor-db)
- `SPANNER_EMULATOR_HOST`: Spanner emulator host (default: localhost:9010)

### Profiles

- **`local`**: For local development with Docker Spanner emulator
- **`test`**: For testing with Spanner emulator
- **`gcp`**: For production with real GCP Spanner

## Database Schema

The Spanner database includes the following tables:

- `message_unique_ids`: For message idempotency tracking
- `currency`: Currency validation data
- `country`: Country validation data

## Troubleshooting

### Check Service Status
```bash
docker-compose ps
```

### View Logs
```bash
# All services
docker-compose logs

# Specific service
docker-compose logs fast-inward-clearing-processor
```

### Restart Services
```bash
docker-compose restart fast-inward-clearing-processor
```

### Clean Up
```bash
docker-compose down --remove-orphans
docker system prune -f
```

## Development Workflow

1. Make code changes
2. Rebuild: `mvn clean compile`
3. Restart service: `docker-compose restart fast-inward-clearing-processor`
4. Check logs: `docker-compose logs -f fast-inward-clearing-processor`

## Testing

### Run Playwright Tests
```bash
cd playwright
./run-tests.sh
```

### Manual Testing
Use the health endpoint to verify the service is running:
```bash
curl http://localhost:8080/actuator/health
```
