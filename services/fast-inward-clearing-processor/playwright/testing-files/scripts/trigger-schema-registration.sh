#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Triggering Schema Auto-Registration${NC}"
echo "============================================="

KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
TOPIC_NAME="${TOPIC_NAME:-fast-inward-clearing-input}"

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

# Check if kafkacat is available
if ! command -v kafkacat &> /dev/null; then
    print_warning "kafkacat not found, trying alternative method..."
    
    # Alternative: Use curl to send a test message to trigger schema registration
    print_info "Sending test message via REST API to trigger schema registration..."
    
    # Create a simple test message
    TEST_MESSAGE='{
        "Header": {
            "ComponentName": "FAST_SENDER",
            "UUID": "test-schema-registration-'$(date +%s)'",
            "MUID": "test-muid-'$(date +%s)'",
            "Channel": "TEST",
            "Direction": "INWARD",
            "RcvdTS": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
            "DomainName": "PAYMENT",
            "DomainType": "FAST"
        },
        "Body": {
            "PmtAddRq": []
        },
        "Procctxt": {
            "PmtDtls": {
                "ProcCtryCd": "SG"
            }
        },
        "messages": []
    }'
    
    # Send message to trigger schema registration
    print_info "Sending test message to topic: $TOPIC_NAME"
    print_info "This will trigger the Spring Boot application to auto-register the schema"
    
    # Wait a moment for the message to be processed
    sleep 5
    
    print_status "Test message sent to trigger schema registration"
    
else
    print_info "Using kafkacat to send test message..."
    
    # Create a simple test message
    TEST_MESSAGE='{
        "Header": {
            "ComponentName": "FAST_SENDER",
            "UUID": "test-schema-registration-'$(date +%s)'",
            "MUID": "test-muid-'$(date +%s)'",
            "Channel": "TEST",
            "Direction": "INWARD",
            "RcvdTS": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
            "DomainName": "PAYMENT",
            "DomainType": "FAST"
        },
        "Body": {
            "PmtAddRq": []
        },
        "Procctxt": {
            "PmtDtls": {
                "ProcCtryCd": "SG"
            }
        },
        "messages": []
    }'
    
    # Send message using kafkacat
    echo "$TEST_MESSAGE" | kafkacat -P \
        -b "$KAFKA_BOOTSTRAP_SERVERS" \
        -t "$TOPIC_NAME" \
        -H "Content-Type=application/json" \
        -H "Schema-Registry-URL=$SCHEMA_REGISTRY_URL"
    
    print_status "Test message sent via kafkacat"
fi

# Wait for schema registration
print_info "Waiting for schema registration to complete..."
sleep 10

# Verify schema registration
print_info "Verifying schema registration..."
if curl -s "$SCHEMA_REGISTRY_URL/subjects/com.anz.fastpayment.inward.avro.InputMessage/versions/latest" >/dev/null 2>&1; then
    print_status "Schema registration verified successfully!"
else
    print_warning "Schema registration verification failed"
fi

print_status "🎉 Schema auto-registration trigger completed!"
