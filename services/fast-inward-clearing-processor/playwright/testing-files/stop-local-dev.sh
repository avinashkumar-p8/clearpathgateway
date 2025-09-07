#!/bin/bash

# Fast Inward Clearing Processor - Local Development Stop Script
# This script stops the local application and Docker infrastructure services

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🛑 Fast Inward Clearing Processor - Local Development Stop${NC}"
echo -e "${BLUE}=======================================================${NC}"

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

# Step 1: Stop local Java application
print_info "Stopping local Java application..."
pkill -f "fast-inward-clearing-processor" 2>/dev/null || true
print_status "Local application stopped"

# Step 2: Stop Docker services
print_info "Stopping Docker infrastructure services..."
if docker-compose down --remove-orphans; then
    print_status "Docker services stopped successfully"
else
    print_warning "Some Docker services may not have been running"
fi

print_status "All services stopped successfully"
echo -e "${YELLOW}ℹ️  Local development environment stopped${NC}"
