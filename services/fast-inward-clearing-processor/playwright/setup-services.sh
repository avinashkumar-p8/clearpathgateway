#!/bin/bash

# Fast Inward Clearing Processor - Service Setup Script
# This script automatically starts all required services for Playwright testing

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PLAYWRIGHT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_DIR="$(cd "$PLAYWRIGHT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$SERVICE_DIR/../.." && pwd)"
DOCKER_COMPOSE_FILE="$PLAYWRIGHT_DIR/docker-compose-inward-only.yml"

echo -e "${BLUE}🚀 Fast Inward Clearing Processor - Service Setup${NC}"
echo -e "${BLUE}================================================${NC}"

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

# Function to check if a port is in use
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        return 0  # Port is in use
    else
        return 1  # Port is free
    fi
}

# Function to wait for service to be ready
wait_for_service() {
    local url=$1
    local service_name=$2
    local max_attempts=30
    local attempt=1
    
    print_info "Waiting for $service_name to be ready..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$url" >/dev/null 2>&1; then
            print_status "$service_name is ready!"
            return 0
        fi
        
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    print_error "$service_name failed to start within 60 seconds"
    return 1
}

# Step 1: Clean up any existing services
echo -e "\n${BLUE}🧹 Step 1: Cleaning up existing services...${NC}"

print_info "Stopping any running Java processes..."
pkill -f "spring-boot:run" || true
pkill -f "fast-inward-clearing-processor" || true

print_info "Stopping Docker services..."
cd "$PLAYWRIGHT_DIR"
docker-compose -f docker-compose-inward-only.yml down || true

print_status "Cleanup completed"

# Step 2: Start Docker services (Kafka, Zookeeper, Schema Registry)
echo -e "\n${BLUE}🐳 Step 2: Starting Docker services for Inward Clearing Processor...${NC}"

print_info "Starting only required services: Kafka, Zookeeper, and Schema Registry..."
cd "$PLAYWRIGHT_DIR"

# Start only the services needed for inward clearing processor
docker-compose -f docker-compose-inward-only.yml up -d

# Wait for Kafka to be ready
print_info "Waiting for Kafka to be ready..."
sleep 10

# Check if Kafka is running
if docker-compose -f docker-compose-inward-only.yml ps | grep -q "kafka.*Up"; then
    print_status "Kafka is running"
else
    print_error "Kafka failed to start"
    exit 1
fi

# Check if Schema Registry is running
if docker-compose -f docker-compose-inward-only.yml ps | grep -q "schema-registry.*Up"; then
    print_status "Schema Registry is running"
else
    print_error "Schema Registry failed to start"
    exit 1
fi

print_status "Docker services started successfully"

# Step 2.5: Initialize Spanner database
echo -e "\n${BLUE}🗄️  Step 2.5: Initializing Spanner database...${NC}"

print_info "Waiting for Spanner emulator to be ready..."
sleep 5

print_info "Initializing Spanner database..."
if bash "$SERVICE_DIR/scripts/init-spanner.sh"; then
    print_status "Spanner database initialized successfully"
else
    print_warning "Spanner database initialization had issues, but continuing..."
fi

# Step 3: Compile and start Java service
echo -e "\n${BLUE}☕ Step 3: Starting Java service...${NC}"

cd "$SERVICE_DIR"

print_info "Compiling the service..."
if mvn clean compile -q; then
    print_status "Compilation successful"
else
    print_error "Compilation failed"
    exit 1
fi

print_info "Starting Spring Boot service..."
# Start the service in background
nohup mvn spring-boot:run > service.log 2>&1 &
SERVICE_PID=$!

# Wait for service to be ready
if wait_for_service "http://localhost:8080/api/v1/health/status" "Fast Inward Clearing Processor"; then
    print_status "Java service started successfully (PID: $SERVICE_PID)"
else
    print_error "Java service failed to start"
    print_info "Check service.log for details:"
    tail -20 service.log
    exit 1
fi

# Step 4: Verify all services
echo -e "\n${BLUE}🔍 Step 4: Verifying all services...${NC}"

# Check Kafka
if check_port 9092; then
    print_status "Kafka is listening on port 9092"
else
    print_error "Kafka is not listening on port 9092"
fi

# Check Schema Registry
if check_port 8081; then
    print_status "Schema Registry is listening on port 8081"
else
    print_error "Schema Registry is not listening on port 8081"
fi

# Check Java service
if check_port 8080; then
    print_status "Java service is listening on port 8080"
else
    print_error "Java service is not listening on port 8080"
fi

# Test health endpoint
print_info "Testing health endpoint..."
if curl -s http://localhost:8080/api/v1/health/status | grep -q "UP"; then
    print_status "Health check passed"
else
    print_warning "Health check failed - service may still be starting"
fi

# Step 5: Create PID file for cleanup
echo -e "\n${BLUE}📝 Step 5: Creating service tracking...${NC}"

echo "$SERVICE_PID" > "$PLAYWRIGHT_DIR/.service.pid"
echo "Docker services started at $(date)" > "$PLAYWRIGHT_DIR/.docker.started"

print_status "Service tracking files created"

# Step 6: Summary
echo -e "\n${GREEN}🎉 All services are ready!${NC}"
echo -e "${GREEN}========================${NC}"
echo -e "${GREEN}✅ Kafka: localhost:9092${NC}"
echo -e "${GREEN}✅ Schema Registry: localhost:8081${NC}"
echo -e "${GREEN}✅ Java Service: localhost:8080${NC}"
echo -e "${GREEN}✅ Health Endpoint: http://localhost:8080/api/v1/health/status${NC}"

echo -e "\n${BLUE}📋 Service Information:${NC}"
echo -e "${BLUE}• Java Service PID: $SERVICE_PID${NC}"
echo -e "${BLUE}• Service Log: $SERVICE_DIR/service.log${NC}"
echo -e "${BLUE}• PID File: $PLAYWRIGHT_DIR/.service.pid${NC}"

echo -e "\n${YELLOW}🚀 Ready to run Playwright tests!${NC}"
echo -e "${YELLOW}Run: npx playwright test${NC}"

echo -e "\n${BLUE}🛑 To stop all services, run: ./cleanup-services.sh${NC}"
