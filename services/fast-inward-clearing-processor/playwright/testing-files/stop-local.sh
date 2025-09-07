#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🛑 Stopping Fast Inward Clearing Processor - Local Development${NC}"
echo "================================================================"

# Function to print status
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ️  $1${NC}"
}

# Stop all services
print_info "Stopping all services..."
docker-compose down --remove-orphans

print_status "All services stopped"

# Optional: Clean up volumes (uncomment if needed)
# print_info "Cleaning up volumes..."
# docker-compose down -v
# print_status "Volumes cleaned up"

echo -e "${GREEN}🎉 Fast Inward Clearing Processor stopped successfully!${NC}"
