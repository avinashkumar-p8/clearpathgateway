#!/bin/bash

# Spanner Database Initialization Script
# This script creates the database and tables in the Spanner emulator

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ID="anz-fastpayment-sg"
INSTANCE_ID="payment-gateway"
DATABASE_ID="inward-processor-db"
EMULATOR_HOST="localhost:9010"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="$SCRIPT_DIR/init-spanner-db.sql"

echo -e "${BLUE}🗄️  Spanner Database Initialization${NC}"
echo -e "${BLUE}===================================${NC}"

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

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    print_error "gcloud CLI is not installed. Please install it first."
    exit 1
fi

# Check if Spanner emulator is running
print_info "Checking if Spanner emulator is running..."
if ! curl -s http://localhost:9010 >/dev/null 2>&1; then
    print_error "Spanner emulator is not running on localhost:9010"
    print_info "Please start the emulator first: docker-compose up -d spanner-emulator"
    exit 1
fi

print_status "Spanner emulator is running"

# Configure gcloud to use emulator
print_info "Configuring gcloud to use Spanner emulator..."
gcloud config set auth/disable_credentials true
gcloud config set project $PROJECT_ID
export SPANNER_EMULATOR_HOST=$EMULATOR_HOST

# Create instance if it doesn't exist
print_info "Creating Spanner instance if it doesn't exist..."
if ! gcloud spanner instances describe $INSTANCE_ID --quiet 2>/dev/null; then
    print_info "Creating instance: $INSTANCE_ID"
    gcloud spanner instances create $INSTANCE_ID \
        --config=emulator-config \
        --description="Payment Gateway Instance" \
        --nodes=1
    print_status "Instance created successfully"
else
    print_status "Instance already exists"
fi

# Create database if it doesn't exist
print_info "Creating database if it doesn't exist..."
if ! gcloud spanner databases describe $DATABASE_ID --instance=$INSTANCE_ID --quiet 2>/dev/null; then
    print_info "Creating database: $DATABASE_ID"
    gcloud spanner databases create $DATABASE_ID --instance=$INSTANCE_ID
    print_status "Database created successfully"
else
    print_status "Database already exists"
fi

# Execute SQL script
print_info "Executing database initialization script..."
if [ -f "$SQL_FILE" ]; then
    gcloud spanner databases ddl update $DATABASE_ID --instance=$INSTANCE_ID --ddl-file="$SQL_FILE"
    print_status "Database schema initialized successfully"
else
    print_error "SQL file not found: $SQL_FILE"
    exit 1
fi

# Verify tables were created
print_info "Verifying tables were created..."
TABLES=$(gcloud spanner databases ddl describe $DATABASE_ID --instance=$INSTANCE_ID --quiet 2>/dev/null | grep "CREATE TABLE" | wc -l)

if [ "$TABLES" -ge 3 ]; then
    print_status "Tables created successfully ($TABLES tables found)"
else
    print_warning "Expected at least 3 tables, found $TABLES"
fi

# Summary
echo -e "\n${GREEN}🎉 Spanner Database Initialization Complete!${NC}"
echo -e "${GREEN}===========================================${NC}"
echo -e "${GREEN}✅ Project ID: $PROJECT_ID${NC}"
echo -e "${GREEN}✅ Instance ID: $INSTANCE_ID${NC}"
echo -e "${GREEN}✅ Database ID: $DATABASE_ID${NC}"
echo -e "${GREEN}✅ Emulator Host: $EMULATOR_HOST${NC}"

echo -e "\n${BLUE}📋 Next Steps:${NC}"
echo -e "${BLUE}• Start the Java service: mvn spring-boot:run${NC}"
echo -e "${BLUE}• Run Playwright tests: npm run test:auto${NC}"

echo -e "\n${YELLOW}💡 To reset the database, run:${NC}"
echo -e "${YELLOW}gcloud spanner databases delete $DATABASE_ID --instance=$INSTANCE_ID --quiet${NC}"
echo -e "${YELLOW}Then run this script again.${NC}"
