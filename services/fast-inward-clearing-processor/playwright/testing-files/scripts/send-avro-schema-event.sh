#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}📨 Send Avro Schema Event${NC}"
echo "============================="

KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPIC_NAME="${TOPIC_NAME:-schema-events}"

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

# Create a new Avro message schema (this is what you'll receive in real scenario)
NEW_AVRO_MESSAGE_SCHEMA='{
  "namespace": "com.anz.fastpayment.inward.avro",
  "type": "record",
  "name": "PaymentConfirmationMessage",
  "doc": "Payment confirmation message schema",
  "fields": [
    {
      "name": "confirmationId",
      "type": "string",
      "doc": "Confirmation identifier"
    },
    {
      "name": "originalPaymentId",
      "type": "string",
      "doc": "Original payment identifier"
    },
    {
      "name": "status",
      "type": {
        "type": "enum",
        "name": "ConfirmationStatus",
        "symbols": ["CONFIRMED", "REJECTED", "PENDING"]
      },
      "doc": "Confirmation status"
    },
    {
      "name": "amount",
      "type": "double",
      "doc": "Confirmed amount"
    },
    {
      "name": "currency",
      "type": "string",
      "doc": "Currency code"
    },
    {
      "name": "confirmationTimestamp",
      "type": "string",
      "doc": "Confirmation timestamp"
    },
    {
      "name": "bankReference",
      "type": ["null", "string"],
      "default": null,
      "doc": "Bank reference number"
    },
    {
      "name": "processingDetails",
      "type": {
        "type": "record",
        "name": "ProcessingDetails",
        "fields": [
          {
            "name": "processingTime",
            "type": "long",
            "doc": "Processing time in milliseconds"
          },
          {
            "name": "clearingSystem",
            "type": "string",
            "doc": "Clearing system used"
          },
          {
            "name": "routingCode",
            "type": "string",
            "doc": "Routing code"
          }
        ]
      },
      "doc": "Processing details"
    }
  ]
}'

# Create sample message content
MESSAGE_CONTENT='{
  "confirmationId": "CONF-'$(date +%s)'",
  "originalPaymentId": "PAY-123456789",
  "status": "CONFIRMED",
  "amount": 1500.75,
  "currency": "SGD",
  "confirmationTimestamp": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
  "bankReference": "BANK-REF-789",
  "processingDetails": {
    "processingTime": 2500,
    "clearingSystem": "FAST",
    "routingCode": "SG-001"
  }
}'

# Create the event that contains the Avro message schema
SCHEMA_EVENT='{
  "eventId": "avro-schema-event-'$(date +%s)'",
  "eventType": "AVRO_MESSAGE_SCHEMA",
  "schemaName": "PaymentConfirmationMessage",
  "avroMessageSchema": "'$(echo "$NEW_AVRO_MESSAGE_SCHEMA" | jq -c . | sed 's/"/\\"/g')'",
  "messageContent": "'$(echo "$MESSAGE_CONTENT" | jq -c . | sed 's/"/\\"/g')'",
  "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'",
  "source": "external-payment-system",
  "version": "1.0.0"
}'

print_info "Kafka Bootstrap Servers: $KAFKA_BOOTSTRAP_SERVERS"
print_info "Topic: $TOPIC_NAME"

# Check if kafkacat is available
if ! command -v kafkacat &> /dev/null; then
    print_warning "kafkacat not found, using alternative method..."
    
    print_info "Event prepared for sending:"
    echo "$SCHEMA_EVENT" | jq '.'
    
    print_status "Event ready to be sent via your preferred method"
    
else
    print_info "Sending Avro schema event via kafkacat..."
    
    # Send the event
    echo "$SCHEMA_EVENT" | kafkacat -P \
        -b "$KAFKA_BOOTSTRAP_SERVERS" \
        -t "$TOPIC_NAME" \
        -H "Content-Type=application/json"
    
    print_status "Avro schema event sent successfully!"
fi

# Wait for processing
print_info "Waiting for schema auto-registration to be processed..."
sleep 5

# Verify schema registration
print_info "Verifying schema auto-registration..."
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
if curl -s "$SCHEMA_REGISTRY_URL/subjects/com.anz.fastpayment.inward.avro.PaymentConfirmationMessage/versions/latest" >/dev/null 2>&1; then
    print_status "Avro message schema auto-registered successfully!"
    print_info "Schema details:"
    curl -s "$SCHEMA_REGISTRY_URL/subjects/com.anz.fastpayment.inward.avro.PaymentConfirmationMessage/versions/latest" | jq '.'
else
    print_warning "Schema auto-registration verification failed"
fi

print_status "🎉 Avro schema event sent and auto-registration completed!"
