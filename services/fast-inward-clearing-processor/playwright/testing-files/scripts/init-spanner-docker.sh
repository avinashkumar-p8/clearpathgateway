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

echo -e "${BLUE}🗄️  Spanner Database Initialization for Docker${NC}"
echo "================================================"

echo -e "${YELLOW}ℹ️  Configuration:${NC}"
echo "   Project ID: $PROJECT_ID"
echo "   Instance ID: $INSTANCE_ID"
echo "   Database ID: $DATABASE_ID"
echo "   Emulator Host: $EMULATOR_HOST"

# Wait for Spanner emulator to be ready
echo -e "${YELLOW}ℹ️  Waiting for Spanner emulator to be ready...${NC}"
echo -e "${YELLOW}ℹ️  Spanner emulator uses gRPC, not HTTP, so we'll wait a bit and proceed...${NC}"
sleep 10
echo -e "${GREEN}✅ Proceeding with Spanner initialization...${NC}"

# Set up gcloud configuration for emulator
echo -e "${YELLOW}ℹ️  Configuring gcloud for emulator...${NC}"
export SPANNER_EMULATOR_HOST=$EMULATOR_HOST

# Create instance if it doesn't exist
echo -e "${YELLOW}ℹ️  Creating Spanner instance...${NC}"
if ! gcloud spanner instances describe $INSTANCE_ID --quiet 2>/dev/null; then
    echo -e "${YELLOW}   Creating instance $INSTANCE_ID...${NC}"
    gcloud spanner instances create $INSTANCE_ID \
        --config=emulator-config \
        --description="Payment Gateway Emulator" \
        --nodes=1 \
        --quiet
    echo -e "${GREEN}✅ Instance created successfully${NC}"
else
    echo -e "${GREEN}✅ Instance $INSTANCE_ID already exists${NC}"
fi

# Create database if it doesn't exist
echo -e "${YELLOW}ℹ️  Creating Spanner database...${NC}"
if ! gcloud spanner databases describe $DATABASE_ID --instance=$INSTANCE_ID --quiet 2>/dev/null; then
    echo -e "${YELLOW}   Creating database $DATABASE_ID...${NC}"
    gcloud spanner databases create $DATABASE_ID \
        --instance=$INSTANCE_ID \
        --ddl-file="$(dirname "${BASH_SOURCE[0]}")/init-spanner-db.sql" \
        --quiet
    echo -e "${GREEN}✅ Database created successfully${NC}"
else
    echo -e "${GREEN}✅ Database $DATABASE_ID already exists${NC}"
    echo -e "${YELLOW}ℹ️  Applying DDL updates...${NC}"
    gcloud spanner ddl update $DATABASE_ID \
        --instance=$INSTANCE_ID \
        --ddl-file="$(dirname "${BASH_SOURCE[0]}")/init-spanner-db.sql" \
        --quiet
    echo -e "${GREEN}✅ DDL updates applied${NC}"
fi

echo -e "${GREEN}🎉 Spanner database initialization completed successfully!${NC}"
echo -e "${BLUE}📊 Database Details:${NC}"
echo "   Project: $PROJECT_ID"
echo "   Instance: $INSTANCE_ID"
echo "   Database: $DATABASE_ID"
echo "   Emulator: $EMULATOR_HOST"
