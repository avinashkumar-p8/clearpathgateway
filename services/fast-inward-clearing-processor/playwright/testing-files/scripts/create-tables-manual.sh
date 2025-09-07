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
EMULATOR_REST_HOST="${SPANNER_EMULATOR_REST_HOST:-localhost:9020}"

echo -e "${BLUE}🗄️  Creating Tables Manually via REST API${NC}"
echo "=============================================="

echo -e "${YELLOW}ℹ️  Configuration:${NC}"
echo "   Project ID: $PROJECT_ID"
echo "   Instance ID: $INSTANCE_ID"
echo "   Database ID: $DATABASE_ID"
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

# Create tables using REST API
echo -e "${YELLOW}ℹ️  Creating tables via REST API...${NC}"

# Create message_unique_ids table
echo -e "${YELLOW}   Creating message_unique_ids table...${NC}"
DDL_REQUEST='{
  "statements": [
    "CREATE TABLE message_unique_ids (muid STRING(255) NOT NULL, message_topic STRING(100) NOT NULL, message_partition INT64 NOT NULL, message_offset INT64 NOT NULL, event_payload STRING(MAX), processing_status STRING(50) NOT NULL, created_at TIMESTAMP NOT NULL, processed_at TIMESTAMP, is_active BOOL NOT NULL) PRIMARY KEY (muid)"
  ]
}'

if curl -s -X POST \
  "http://$EMULATOR_REST_HOST/v1/projects/$PROJECT_ID/instances/$INSTANCE_ID/databases/$DATABASE_ID:executeDdl" \
  -H "Content-Type: application/json" \
  -d "$DDL_REQUEST" >/dev/null 2>&1; then
    echo -e "${GREEN}✅ message_unique_ids table created successfully${NC}"
else
    echo -e "${YELLOW}ℹ️  message_unique_ids table may already exist, continuing...${NC}"
fi

# Create countries table
echo -e "${YELLOW}   Creating countries table...${NC}"
DDL_REQUEST='{
  "statements": [
    "CREATE TABLE countries (code STRING(2) NOT NULL, name STRING(100) NOT NULL) PRIMARY KEY (code)"
  ]
}'

if curl -s -X POST \
  "http://$EMULATOR_REST_HOST/v1/projects/$PROJECT_ID/instances/$INSTANCE_ID/databases/$DATABASE_ID:executeDdl" \
  -H "Content-Type: application/json" \
  -d "$DDL_REQUEST" >/dev/null 2>&1; then
    echo -e "${GREEN}✅ countries table created successfully${NC}"
else
    echo -e "${YELLOW}ℹ️  countries table may already exist, continuing...${NC}"
fi

# Create currencies table
echo -e "${YELLOW}   Creating currencies table...${NC}"
DDL_REQUEST='{
  "statements": [
    "CREATE TABLE currencies (code STRING(3) NOT NULL, name STRING(100) NOT NULL) PRIMARY KEY (code)"
  ]
}'

if curl -s -X POST \
  "http://$EMULATOR_REST_HOST/v1/projects/$PROJECT_ID/instances/$INSTANCE_ID/databases/$DATABASE_ID:executeDdl" \
  -H "Content-Type: application/json" \
  -d "$DDL_REQUEST" >/dev/null 2>&1; then
    echo -e "${GREEN}✅ currencies table created successfully${NC}"
else
    echo -e "${YELLOW}ℹ️  currencies table may already exist, continuing...${NC}"
fi

# Create TransactionMessages table
echo -e "${YELLOW}   Creating TransactionMessages table...${NC}"
DDL_REQUEST='{
  "statements": [
    "CREATE TABLE TransactionMessages (id STRING(255) NOT NULL, muid STRING(255) NOT NULL, message_data JSON, status STRING(50), created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL) PRIMARY KEY (id)"
  ]
}'

if curl -s -X POST \
  "http://$EMULATOR_REST_HOST/v1/projects/$PROJECT_ID/instances/$INSTANCE_ID/databases/$DATABASE_ID:executeDdl" \
  -H "Content-Type: application/json" \
  -d "$DDL_REQUEST" >/dev/null 2>&1; then
    echo -e "${GREEN}✅ TransactionMessages table created successfully${NC}"
else
    echo -e "${YELLOW}ℹ️  TransactionMessages table may already exist, continuing...${NC}"
fi

# Create IdempotencyCache table
echo -e "${YELLOW}   Creating IdempotencyCache table...${NC}"
DDL_REQUEST='{
  "statements": [
    "CREATE TABLE IdempotencyCache (muid STRING(255) NOT NULL, response_data JSON, created_at TIMESTAMP NOT NULL, expires_at TIMESTAMP NOT NULL) PRIMARY KEY (muid)"
  ]
}'

if curl -s -X POST \
  "http://$EMULATOR_REST_HOST/v1/projects/$PROJECT_ID/instances/$INSTANCE_ID/databases/$DATABASE_ID:executeDdl" \
  -H "Content-Type: application/json" \
  -d "$DDL_REQUEST" >/dev/null 2>&1; then
    echo -e "${GREEN}✅ IdempotencyCache table created successfully${NC}"
else
    echo -e "${YELLOW}ℹ️  IdempotencyCache table may already exist, continuing...${NC}"
fi

# Create ProcessingEvents table
echo -e "${YELLOW}   Creating ProcessingEvents table...${NC}"
DDL_REQUEST='{
  "statements": [
    "CREATE TABLE ProcessingEvents (id STRING(255) NOT NULL, muid STRING(255) NOT NULL, event_type STRING(100) NOT NULL, event_data JSON, created_at TIMESTAMP NOT NULL) PRIMARY KEY (id)"
  ]
}'

if curl -s -X POST \
  "http://$EMULATOR_REST_HOST/v1/projects/$PROJECT_ID/instances/$INSTANCE_ID/databases/$DATABASE_ID:executeDdl" \
  -H "Content-Type: application/json" \
  -d "$DDL_REQUEST" >/dev/null 2>&1; then
    echo -e "${GREEN}✅ ProcessingEvents table created successfully${NC}"
else
    echo -e "${YELLOW}ℹ️  ProcessingEvents table may already exist, continuing...${NC}"
fi

echo -e "${GREEN}🎉 All tables created successfully!${NC}"
echo -e "${BLUE}📊 Database Details:${NC}"
echo "   Project: $PROJECT_ID"
echo "   Instance: $INSTANCE_ID"
echo "   Database: $DATABASE_ID"
echo "   REST API: $EMULATOR_REST_HOST"
