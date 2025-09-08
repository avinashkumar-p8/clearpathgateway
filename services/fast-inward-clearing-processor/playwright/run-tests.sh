#!/bin/bash

# Fast Inward Clearing Processor - Automated Test Runner
# This script automatically sets up services and runs Playwright tests

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
SETUP_SCRIPT="$SCRIPT_DIR/setup-services.sh"
CLEANUP_SCRIPT="$SCRIPT_DIR/cleanup-services.sh"

echo -e "${BLUE}🧪 Fast Inward Clearing Processor - Automated Test Runner${NC}"
echo -e "${BLUE}========================================================${NC}"

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

# Function to handle cleanup on exit
cleanup_on_exit() {
    echo -e "\n${YELLOW}🛑 Test execution interrupted. Cleaning up...${NC}"
    cd "$SERVICE_DIR"
    docker-compose down --remove-orphans 2>/dev/null || true
    exit 1
}

# Set up trap for cleanup on exit
trap cleanup_on_exit INT TERM

# Parse command line arguments
AUTO_CLEANUP=true
TEST_PATTERN=""
HEADED_MODE=false
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-cleanup)
            AUTO_CLEANUP=false
            shift
            ;;
        --pattern)
            TEST_PATTERN="$2"
            shift 2
            ;;
        --headed)
            HEADED_MODE=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --no-cleanup    Don't cleanup services after tests"
            echo "  --pattern PAT   Run tests matching pattern"
            echo "  --headed        Run tests in headed mode"
            echo "  --verbose       Verbose output"
            echo "  --help          Show this help"
            echo ""
            echo "Examples:"
            echo "  $0                                    # Run all tests"
            echo "  $0 --pattern 'IDEMPOTENCY'           # Run idempotency tests"
            echo "  $0 --pattern 'VALIDATION' --headed   # Run validation tests in headed mode"
            echo "  $0 --no-cleanup                      # Run tests without cleanup"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Step 1: Check prerequisites
echo -e "\n${BLUE}🔍 Step 1: Checking prerequisites...${NC}"

# Check if setup script exists
if [ ! -f "$SETUP_SCRIPT" ]; then
    print_error "Setup script not found: $SETUP_SCRIPT"
    exit 1
fi

# Check if cleanup script exists
if [ ! -f "$CLEANUP_SCRIPT" ]; then
    print_error "Cleanup script not found: $CLEANUP_SCRIPT"
    exit 1
fi

# Check if Playwright is installed
if ! command -v npx &> /dev/null; then
    print_error "npx is not installed"
    exit 1
fi

print_status "Prerequisites check passed"

# Step 2: Setup services
echo -e "\n${BLUE}🚀 Step 2: Setting up services...${NC}"

print_info "Running service setup..."
if bash "$SETUP_SCRIPT"; then
    print_status "Services setup completed successfully"
else
    print_error "Service setup failed"
    exit 1
fi

# Step 3: Wait for services to be fully ready
echo -e "\n${BLUE}⏳ Step 3: Waiting for services to be fully ready...${NC}"

print_info "Waiting additional 10 seconds for services to stabilize..."
sleep 10

# Verify services are still running
if ! curl -s http://localhost:8080/api/v1/health/status >/dev/null 2>&1; then
    print_error "Java service is not responding"
    exit 1
fi

print_status "All services are ready for testing"

# Step 4: Run Playwright tests
echo -e "\n${BLUE}🧪 Step 4: Running Playwright tests...${NC}"

# Build Playwright command
PLAYWRIGHT_CMD="npx playwright test"

if [ -n "$TEST_PATTERN" ]; then
    PLAYWRIGHT_CMD="$PLAYWRIGHT_CMD -g '$TEST_PATTERN'"
    print_info "Running tests matching pattern: $TEST_PATTERN"
fi

if [ "$HEADED_MODE" = true ]; then
    PLAYWRIGHT_CMD="$PLAYWRIGHT_CMD --headed"
    print_info "Running in headed mode"
fi

if [ "$VERBOSE" = true ]; then
    PLAYWRIGHT_CMD="$PLAYWRIGHT_CMD --reporter=line"
    print_info "Running in verbose mode"
fi

print_info "Executing: $PLAYWRIGHT_CMD"
echo ""

# Run the tests
cd "$SCRIPT_DIR"
if eval "$PLAYWRIGHT_CMD"; then
    print_status "All tests passed!"
    TEST_RESULT=0
else
    print_error "Some tests failed!"
    TEST_RESULT=1
fi

# Step 5: Cleanup (if enabled)
if [ "$AUTO_CLEANUP" = true ]; then
    echo -e "\n${BLUE}🧹 Step 5: Cleaning up services...${NC}"
    
    print_info "Stopping Docker services..."
    cd "$SERVICE_DIR"
    if docker-compose down --remove-orphans; then
        print_status "Services cleaned up successfully"
    else
        print_warning "Service cleanup had issues"
    fi
else
    echo -e "\n${YELLOW}⚠️  Services left running (--no-cleanup specified)${NC}"
    echo -e "${YELLOW}To cleanup manually, run: cd .. && docker-compose down${NC}"
fi

# Step 6: Summary
echo -e "\n${BLUE}📊 Test Execution Summary${NC}"
echo -e "${BLUE}========================${NC}"

if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}✅ Test Result: PASSED${NC}"
else
    echo -e "${RED}❌ Test Result: FAILED${NC}"
fi

echo -e "${BLUE}• Services: ${AUTO_CLEANUP:+Cleaned up}${AUTO_CLEANUP:-Left running}${NC}"
echo -e "${BLUE}• Test Pattern: ${TEST_PATTERN:-All tests}${NC}"
echo -e "${BLUE}• Mode: ${HEADED_MODE:+Headed}${HEADED_MODE:-Headless}${NC}"

# Exit with test result
exit $TEST_RESULT
