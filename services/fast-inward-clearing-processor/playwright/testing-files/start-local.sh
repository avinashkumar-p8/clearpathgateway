#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Fast Inward Clearing Processor - Local Development Setup${NC}"
echo "================================================================"

# Function to print status
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Step 1: Check prerequisites
print_info "Checking prerequisites..."
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

print_status "Prerequisites check passed"

# Step 2: Clean up existing containers
print_info "Cleaning up existing containers..."
docker-compose down --remove-orphans 2>/dev/null || true
print_status "Cleanup completed"

# Step 3: Start infrastructure services
print_info "Starting infrastructure services (Kafka, Zookeeper, Schema Registry, Spanner Emulator)..."
docker-compose up -d zookeeper kafka schema-registry spanner-emulator

# Wait for services to be ready
print_info "Waiting for services to be ready..."
sleep 30

# Check if services are running
print_info "Checking service health..."
if ! docker-compose ps | grep -q "kafka.*Up"; then
    print_error "Kafka is not running"
    exit 1
fi

if ! docker-compose ps | grep -q "spanner-emulator.*Up"; then
    print_error "Spanner emulator is not running"
    exit 1
fi

print_status "Infrastructure services are running"

# Step 4: Initialize Spanner database
print_info "Initializing Spanner database..."
if bash scripts/init-spanner-docker.sh; then
    print_status "Spanner database initialized successfully"
else
    print_error "Spanner database initialization failed"
    exit 1
fi

# Step 5: Build and start the application
print_info "Building the application..."
if mvn clean compile; then
    print_status "Application compiled successfully"
else
    print_error "Application compilation failed"
    exit 1
fi

# Step 6: Start the application
print_info "Starting the Fast Inward Clearing Processor..."
docker-compose up -d fast-inward-clearing-processor

# Wait for application to be ready
print_info "Waiting for application to be ready..."
sleep 30

# Check application health
print_info "Checking application health..."
if curl -s http://localhost:8080/actuator/health >/dev/null; then
    print_status "Application is running and healthy"
    echo -e "${BLUE}📊 Service URLs:${NC}"
    echo "   Application: http://localhost:8080"
    echo "   Health Check: http://localhost:8080/actuator/health"
    echo "   Metrics: http://localhost:8080/actuator/metrics"
    echo "   Schema Registry: http://localhost:8081"
    echo "   Spanner Emulator: http://localhost:9010"
else
    print_error "Application health check failed"
    echo "Check logs with: docker-compose logs fast-inward-clearing-processor"
    exit 1
fi

echo -e "${GREEN}🎉 Fast Inward Clearing Processor is ready for development!${NC}"
echo -e "${YELLOW}ℹ️  To stop all services: docker-compose down${NC}"
echo -e "${YELLOW}ℹ️  To view logs: docker-compose logs -f fast-inward-clearing-processor${NC}"
