#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

PROJECT_ID="anz-fastpayment-sg"
INSTANCE_ID="payment-gateway-local"
DATABASE_ID="inward-processor-db"
EMULATOR_HOST="${SPANNER_EMULATOR_HOST:-localhost:9010}"
EMULATOR_REST_HOST="${SPANNER_EMULATOR_REST_HOST:-localhost:9020}"

echo -e "${BLUE}🗄️  Spanner Database Initialization via REST API${NC}"
echo "=================================================="

echo -e "${YELLOW}ℹ️  Configuration:${NC}"
echo "   Project ID: $PROJECT_ID"
echo "   Instance ID: $INSTANCE_ID"
echo "   Database ID: $DATABASE_ID"
echo "   Emulator Host: $EMULATOR_HOST"
echo "   REST Host: $EMULATOR_REST_HOST"

# Wait for Spanner emulator to be ready
echo -e "${YELLOW}ℹ️  Waiting for Spanner emulator to be ready...${NC}"
for i in {1..30}; do
    if curl -s http://$EMULATOR_REST_HOST >/dev/null 2>&1; then
        echo -e "${GREEN}✅ Spanner emulator is ready${NC}"
        break
    fi
    echo -n "."
    sleep 2
done

if [ $i -eq 30 ]; then
    echo -e "${RED}❌ Spanner emulator is not responding${NC}"
    exit 1
fi

# Create instance using REST API
echo -e "${YELLOW}ℹ️  Creating Spanner instance via REST API...${NC}"
INSTANCE_CONFIG='{
  "instanceId": "'$INSTANCE_ID'",
  "config": "emulator-config",
  "displayName": "Payment Gateway Emulator",
  "nodeCount": 1
}'

if curl -s -X POST \
  "http://$EMULATOR_REST_HOST/v1/projects/$PROJECT_ID/instances" \
  -H "Content-Type: application/json" \
  -d "$INSTANCE_CONFIG" >/dev/null 2>&1; then
    echo -e "${GREEN}✅ Instance created successfully${NC}"
else
    echo -e "${YELLOW}ℹ️  Instance may already exist, continuing...${NC}"
fi

# Create database using REST API
echo -e "${YELLOW}ℹ️  Creating Spanner database via REST API...${NC}"
DATABASE_CONFIG='{
  "createStatement": "CREATE DATABASE `'$DATABASE_ID'`",
  "extraStatements": [
    "CREATE TABLE MessageUniqueId (MUID STRING(255) NOT NULL, ProcessedAt TIMESTAMP NOT NULL, Status STRING(50) NOT NULL, ErrorMessage STRING(MAX), CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true), UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (MUID)",
    "CREATE TABLE Currency (Code STRING(3) NOT NULL, Name STRING(100) NOT NULL, IsActive BOOL NOT NULL DEFAULT (true), ValidCountries ARRAY<STRING(2)>, CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true), UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (Code)",
    "CREATE TABLE Country (Code STRING(2) NOT NULL, Name STRING(100) NOT NULL, IsActive BOOL NOT NULL DEFAULT (true), Region STRING(50), CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true), UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY (Code)"
  ]
}'

if curl -s -X POST \
  "http://$EMULATOR_REST_HOST/v1/projects/$PROJECT_ID/instances/$INSTANCE_ID/databases" \
  -H "Content-Type: application/json" \
  -d "$DATABASE_CONFIG" >/dev/null 2>&1; then
    echo -e "${GREEN}✅ Database created successfully${NC}"
else
    echo -e "${YELLOW}ℹ️  Database may already exist, continuing...${NC}"
fi

echo -e "${GREEN}🎉 Spanner database initialization completed successfully!${NC}"
echo -e "${BLUE}📊 Database Details:${NC}"
echo "   Project: $PROJECT_ID"
echo "   Instance: $INSTANCE_ID"
echo "   Database: $DATABASE_ID"
echo "   Emulator: $EMULATOR_HOST"
echo "   REST API: $EMULATOR_REST_HOST"
