#!/bin/bash

# Fast Inward Clearing Processor - Service Cleanup Script
# This script stops all services started by setup-services.sh

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
PID_FILE="$PLAYWRIGHT_DIR/.service.pid"
DOCKER_COMPOSE_FILE="$PLAYWRIGHT_DIR/docker-compose-inward-only.yml"

echo -e "${BLUE}🛑 Fast Inward Clearing Processor - Service Cleanup${NC}"
echo -e "${BLUE}=================================================${NC}"

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

# Step 1: Stop Java service
echo -e "\n${BLUE}☕ Step 1: Stopping Java service...${NC}"

if [ -f "$PID_FILE" ]; then
    SERVICE_PID=$(cat "$PID_FILE")
    print_info "Stopping Java service (PID: $SERVICE_PID)..."
    
    if kill -0 "$SERVICE_PID" 2>/dev/null; then
        kill "$SERVICE_PID"
        sleep 3
        
        # Force kill if still running
        if kill -0 "$SERVICE_PID" 2>/dev/null; then
            print_warning "Force killing Java service..."
            kill -9 "$SERVICE_PID"
        fi
        
        print_status "Java service stopped"
    else
        print_warning "Java service was not running"
    fi
    
    rm -f "$PID_FILE"
else
    print_info "No PID file found, stopping any Java processes..."
    pkill -f "spring-boot:run" || true
    pkill -f "fast-inward-clearing-processor" || true
    print_status "Java processes stopped"
fi

# Step 2: Stop Docker services
echo -e "\n${BLUE}🐳 Step 2: Stopping Docker services...${NC}"

cd "$PLAYWRIGHT_DIR"
print_info "Stopping Docker Compose services for Inward Clearing Processor..."
docker-compose -f docker-compose-inward-only.yml down

print_status "Docker services stopped"

# Step 3: Clean up tracking files
echo -e "\n${BLUE}🧹 Step 3: Cleaning up tracking files...${NC}"

rm -f "$PLAYWRIGHT_DIR/.service.pid"
rm -f "$PLAYWRIGHT_DIR/.docker.started"

print_status "Tracking files cleaned up"

# Step 4: Verify cleanup
echo -e "\n${BLUE}🔍 Step 4: Verifying cleanup...${NC}"

# Check if ports are free
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        return 0  # Port is in use
    else
        return 1  # Port is free
    fi
}

if check_port 8080; then
    print_warning "Port 8080 is still in use"
else
    print_status "Port 8080 is free"
fi

if check_port 9092; then
    print_warning "Port 9092 is still in use"
else
    print_status "Port 9092 is free"
fi

if check_port 8081; then
    print_warning "Port 8081 is still in use"
else
    print_status "Port 8081 is free"
fi

# Step 5: Summary
echo -e "\n${GREEN}🎉 Cleanup completed!${NC}"
echo -e "${GREEN}===================${NC}"
echo -e "${GREEN}✅ Java service stopped${NC}"
echo -e "${GREEN}✅ Docker services stopped${NC}"
echo -e "${GREEN}✅ Tracking files cleaned${NC}"

echo -e "\n${BLUE}📋 All services have been stopped successfully.${NC}"
echo -e "${BLUE}To start services again, run: ./setup-services.sh${NC}"
