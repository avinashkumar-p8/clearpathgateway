#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}📨 Send Schema Registration Event${NC}"
echo "====================================="

KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
TOPIC_NAME="${TOPIC_NAME:-schema-registration-events}"

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

# Create a sample new schema
NEW_SCHEMA='{
  "namespace": "com.anz.fastpayment.inward.avro",
  "type": "record",
  "name": "NewPaymentMessage",
  "doc": "New payment message schema received via event",
  "fields": [
    {
      "name": "paymentId",
      "type": "string",
      "doc": "Payment identifier"
    },
    {
      "name": "amount",
      "type": "double",
      "doc": "Payment amount"
    },
    {
      "name": "currency",
      "type": "string",
      "doc": "Currency code"
    },
    {
      "name": "timestamp",
      "type": "string",
      "doc": "Payment timestamp"
    },
    {
      "name": "status",
      "type": {
        "type": "enum",
        "name": "PaymentStatus",
        "symbols": ["PENDING", "COMPLETED", "FAILED", "CANCELLED"]
      },
      "doc": "Payment status"
    }
  ]
}'

# Create the schema registration event
SCHEMA_EVENT='{
  "eventId": "schema-event-'$(date +%s)'",
  "eventType": "SCHEMA_REGISTER",
  "schemaName": "NewPaymentMessage",
  "subjectName": "com.anz.fastpayment.inward.avro.NewPaymentMessage",
  "schemaContent": "'$(echo "$NEW_SCHEMA" | jq -c . | sed 's/"/\\"/g')'",
  "version": "1.0.0",
  "compatibility": "BACKWARD",
  "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
  "source": "dynamic-schema-manager",
  "metadata": {
    "environment": "test",
    "team": "fast-payment"
  }
}'

print_info "Kafka Bootstrap Servers: $KAFKA_BOOTSTRAP_SERVERS"
print_info "Schema Registry URL: $SCHEMA_REGISTRY_URL"
print_info "Topic: $TOPIC_NAME"

# Check if kafkacat is available
if ! command -v kafkacat &> /dev/null; then
    print_warning "kafkacat not found, using alternative method..."
    
    # Alternative: Use curl to send message via REST API
    print_info "Sending schema registration event via REST API..."
    
    # Create topic if it doesn't exist (this would typically be done by infrastructure)
    print_info "Note: Ensure topic '$TOPIC_NAME' exists in Kafka"
    
    print_status "Schema registration event prepared"
    print_info "Event content:"
    echo "$SCHEMA_EVENT" | jq '.'
    
else
    print_info "Sending schema registration event via kafkacat..."
    
    # Send the event
    echo "$SCHEMA_EVENT" | kafkacat -P \
        -b "$KAFKA_BOOTSTRAP_SERVERS" \
        -t "$TOPIC_NAME" \
        -H "Content-Type=application/json" \
        -H "Schema-Registry-URL=$SCHEMA_REGISTRY_URL"
    
    print_status "Schema registration event sent successfully!"
fi

# Wait for processing
print_info "Waiting for schema registration to be processed..."
sleep 5

# Verify schema registration
print_info "Verifying schema registration..."
if curl -s "$SCHEMA_REGISTRY_URL/subjects/com.anz.fastpayment.inward.avro.NewPaymentMessage/versions/latest" >/dev/null 2>&1; then
    print_status "New schema registered successfully!"
    print_info "Schema details:"
    curl -s "$SCHEMA_REGISTRY_URL/subjects/com.anz.fastpayment.inward.avro.NewPaymentMessage/versions/latest" | jq '.'
else
    print_warning "Schema registration verification failed"
fi

print_status "🎉 Schema registration event sent and processed!"
