#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Automated Test Runner for Fast Inward Clearing Processor${NC}"
echo "=============================================================="

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

print_step() {
    echo -e "\n${BLUE}🔧 Step $1: $2${NC}"
    echo "----------------------------------------"
}

# Step 1: Clean up any existing services
print_step "1" "Cleaning up existing services"
print_info "Stopping any running Java processes..."
pkill -f "spring-boot:run" 2>/dev/null || true

print_info "Stopping Docker services..."
docker-compose down --remove-orphans -v 2>/dev/null || true
print_status "Cleanup completed"

# Step 2: Start Docker infrastructure services
print_step "2" "Starting Docker infrastructure services"
print_info "Starting: Zookeeper, Kafka, Schema Registry, Spanner Emulator..."

if docker-compose up -d; then
    print_status "Docker services started successfully"
else
    print_error "Failed to start Docker services"
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
print_step "3" "Initializing Spanner database"
print_info "Waiting for Spanner emulator to be ready..."
sleep 10

print_info "Initializing Spanner database..."
if bash scripts/init-spanner-rest.sh; then
    print_status "Spanner database initialized successfully"
else
    print_error "Spanner database initialization failed"
    exit 1
fi

# Step 4: Register Avro Schema
print_step "4" "Registering Avro Schema"
print_info "Registering InputMessage schema with Schema Registry..."

if bash scripts/register-schema.sh; then
    print_status "Schema registration completed successfully"
else
    print_warning "Schema registration failed or schema already exists"
fi

# Step 5: Start Spring Boot application
print_step "5" "Starting Spring Boot application"
print_info "Building the application..."
if mvn clean compile -q; then
    print_status "Compilation successful"
else
    print_error "Compilation failed"
    exit 1
fi

print_info "Starting Spring Boot application..."
export SPRING_PROFILES_ACTIVE=test
export SPANNER_EMULATOR_HOST=localhost:9010
export GCP_PROJECT_ID=anz-fastpayment-sg
export SPANNER_INSTANCE=payment-gateway-local
export SPANNER_DATABASE=inward-processor-db
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export SCHEMA_REGISTRY_URL=http://localhost:8081

# Start application in background
mvn spring-boot:run -q > app.log 2>&1 &
APP_PID=$!

# Wait for application to start
print_info "Waiting for application to start..."
for i in {1..60}; do
    if curl -s http://localhost:8080/actuator/health >/dev/null 2>&1; then
        print_status "Application is running and healthy"
        break
    fi
    if [ $i -eq 60 ]; then
        print_error "Application failed to start within 60 seconds"
        print_info "Application logs:"
        tail -20 app.log
        kill $APP_PID 2>/dev/null || true
        exit 1
    fi
    echo -n "."
    sleep 2
done

# Step 6: Run Playwright tests
print_step "6" "Running Playwright tests"
print_info "Starting Playwright test execution..."

cd playwright
if npm test; then
    print_status "All tests completed successfully"
    TEST_RESULT=0
else
    print_error "Some tests failed"
    TEST_RESULT=1
fi

# Step 7: Cleanup
print_step "7" "Cleaning up"
print_info "Stopping Spring Boot application..."
kill $APP_PID 2>/dev/null || true

print_info "Stopping Docker services..."
docker-compose down --remove-orphans 2>/dev/null || true

# Clean up log file
rm -f app.log

if [ $TEST_RESULT -eq 0 ]; then
    print_status "🎉 All tests passed successfully!"
    exit 0
else
    print_error "❌ Some tests failed"
    exit 1
fi
