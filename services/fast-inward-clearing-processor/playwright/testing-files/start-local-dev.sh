#!/bin/bash

# Fast Inward Clearing Processor - Local Development Startup
# This script starts infrastructure services in Docker and runs the application locally

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Fast Inward Clearing Processor - Local Development${NC}"
echo -e "${BLUE}===================================================${NC}"

# Function to print status
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Step 1: Clean up existing services
echo -e "\n${BLUE}🧹 Step 1: Cleaning up existing services...${NC}"
print_info "Stopping any running Java processes..."
pkill -f "fast-inward-clearing-processor" 2>/dev/null || true

print_info "Stopping Docker services..."
docker-compose down --remove-orphans 2>/dev/null || true
print_status "Cleanup completed"

# Step 2: Start infrastructure services in Docker
echo -e "\n${BLUE}🐳 Step 2: Starting infrastructure services in Docker...${NC}"
print_info "Starting: Zookeeper, Kafka, Schema Registry, Spanner Emulator..."

if docker-compose up -d; then
    print_status "Infrastructure services started successfully"
else
    print_error "Failed to start infrastructure services"
    exit 1
fi

# Wait for services to be ready
print_info "Waiting for services to be ready..."
sleep 30

# Check service health
print_info "Checking service health..."
if ! docker-compose ps | grep -q "kafka.*Up"; then
    print_error "Kafka is not running"
    exit 1
fi

if ! docker-compose ps | grep -q "spanner-emulator.*Up"; then
    print_error "Spanner emulator is not running"
    exit 1
fi

print_status "Infrastructure services are healthy"

# Step 3: Initialize Spanner database
echo -e "\n${BLUE}🗄️  Step 3: Initializing Spanner database...${NC}"
print_info "Waiting for Spanner emulator to be ready..."
sleep 10

print_info "Initializing Spanner database..."
if bash scripts/init-spanner-docker.sh; then
    print_status "Spanner database initialized successfully"
else
    print_warning "Spanner database initialization had issues, but continuing..."
fi

# Step 4: Start the application locally
echo -e "\n${BLUE}☕ Step 4: Starting application locally...${NC}"
print_info "Building the application..."
if mvn clean compile; then
    print_status "Compilation successful"
else
    print_error "Compilation failed"
    exit 1
fi

print_info "Starting Spring Boot application locally..."
print_info "Application will connect to Docker services:"
echo "   Kafka: localhost:9092"
echo "   Schema Registry: http://localhost:8081"
echo "   Spanner Emulator: localhost:9010"
echo ""

# Set environment variables for local development
export SPRING_PROFILES_ACTIVE=local
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export SCHEMA_REGISTRY_URL=http://localhost:8081
export GCP_PROJECT_ID=anz-fastpayment-sg
export SPANNER_INSTANCE=payment-gateway
export SPANNER_DATABASE=inward-processor-db
export SPANNER_EMULATOR_HOST=localhost:9010

print_info "Starting application with Maven..."
mvn spring-boot:run
