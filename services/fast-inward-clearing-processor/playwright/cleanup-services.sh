#!/bin/bash

# Fast Inward Clearing Processor - Docker-Only Service Cleanup Script
# This script cleans up all Docker services for Playwright testing

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo -e "${BLUE}🧹 Fast Inward Clearing Processor - Docker Service Cleanup${NC}"
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

# Step 1: Stop any running Java processes
print_info "Stopping any running Java processes..."
pkill -f "fast-inward-clearing-processor" 2>/dev/null || true

# Step 2: Stop Docker services
print_info "Stopping Docker services..."
cd "$SERVICE_DIR"

if docker-compose down --remove-orphans; then
    print_status "Docker services stopped successfully"
else
    print_warning "Some Docker services may not have been running"
fi

# Step 3: Optional cleanup (uncomment if needed)
# print_info "Removing unused Docker images..."
# docker image prune -f

# print_info "Removing unused Docker volumes..."
# docker volume prune -f

print_status "Cleanup completed successfully"
echo -e "${YELLOW}ℹ️  All services have been stopped and cleaned up${NC}"