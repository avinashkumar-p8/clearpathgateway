#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}📋 Schema Registration for Fast Inward Clearing Processor${NC}"
echo "=============================================================="

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SCHEMA_FILE="src/main/resources/avro/InputMessage.avsc"
SUBJECT_NAME="com.anz.fastpayment.inward.avro.InputMessage"

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

# Check if schema file exists
if [ ! -f "$SCHEMA_FILE" ]; then
    print_error "Schema file not found: $SCHEMA_FILE"
    exit 1
fi

print_info "Schema Registry URL: $SCHEMA_REGISTRY_URL"
print_info "Schema File: $SCHEMA_FILE"
print_info "Subject Name: $SUBJECT_NAME"

# Wait for Schema Registry to be ready
print_info "Waiting for Schema Registry to be ready..."
for i in {1..30}; do
    if curl -s "$SCHEMA_REGISTRY_URL/subjects" >/dev/null 2>&1; then
        print_status "Schema Registry is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        print_error "Schema Registry not ready after 60 seconds"
        exit 1
    fi
    echo -n "."
    sleep 2
done

# Check if schema already exists
print_info "Checking if schema already exists..."
RESPONSE=$(curl -s -w "%{http_code}" "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT_NAME/versions/latest")
HTTP_CODE="${RESPONSE: -3}"
RESPONSE_BODY="${RESPONSE%???}"

if [ "$HTTP_CODE" = "200" ]; then
    print_warning "Schema already exists, skipping registration"
    print_info "Existing schema details:"
    echo "$RESPONSE_BODY" | jq '.' 2>/dev/null || echo "Schema exists but cannot parse details"
    exit 0
elif [ "$HTTP_CODE" = "404" ]; then
    print_info "Schema does not exist, proceeding with registration"
else
    print_warning "Unexpected response code: $HTTP_CODE"
    print_info "Response: $RESPONSE_BODY"
    print_info "Proceeding with registration anyway"
fi

# Register the schema
print_info "Registering schema..."
SCHEMA_CONTENT=$(cat "$SCHEMA_FILE" | jq -c .)

# Create the registration payload
REGISTRATION_PAYLOAD=$(jq -n \
    --arg schema "$SCHEMA_CONTENT" \
    --arg subject "$SUBJECT_NAME" \
    '{schema: $schema, subject: $subject}')

# Register the schema
RESPONSE=$(curl -s -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "$REGISTRATION_PAYLOAD" \
    "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT_NAME/versions")

# Check if registration was successful
if echo "$RESPONSE" | jq -e '.id' >/dev/null 2>&1; then
    SCHEMA_ID=$(echo "$RESPONSE" | jq -r '.id')
    print_status "Schema registered successfully!"
    print_info "Schema ID: $SCHEMA_ID"
    print_info "Subject: $SUBJECT_NAME"
    
    # Verify the registration
    print_info "Verifying schema registration..."
    VERIFICATION_RESPONSE=$(curl -s "$SCHEMA_REGISTRY_URL/subjects/$SUBJECT_NAME/versions/latest")
    if echo "$VERIFICATION_RESPONSE" | jq -e '.id' >/dev/null 2>&1; then
        print_status "Schema verification successful"
        echo "$VERIFICATION_RESPONSE" | jq '.'
    else
        print_warning "Schema registered but verification failed"
    fi
else
    print_error "Schema registration failed"
    echo "Response: $RESPONSE"
    exit 1
fi

print_status "🎉 Schema registration completed successfully!"
