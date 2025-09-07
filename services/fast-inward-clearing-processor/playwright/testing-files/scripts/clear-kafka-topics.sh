#!/bin/bash

# Script to clear Kafka topics before running tests
# This ensures clean state for reliable testing

echo "🧹 Clearing Kafka topics for clean testing..."

# Kafka topics to clear
TOPICS=(
    "transactions.incoming"
    "fast-outward-clearing"
    "fast-inward-clearing-dlq"
    "schema-registration-events"
    "schema-update-events"
    "schema-delete-events"
    "schema-events"
)

# Function to clear a topic
clear_topic() {
    local topic=$1
    echo "Clearing topic: $topic"
    
    # Get current offset for the topic
    local current_offset=$(kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic "$topic" --time -1 2>/dev/null | awk -F: '{print $3}' | head -1)
    
    if [ -n "$current_offset" ] && [ "$current_offset" != "null" ]; then
        echo "  Current offset: $current_offset"
        
        # Create a temporary consumer group to consume all messages
        local temp_group="clear-topic-$(date +%s)-$$"
        echo "  Using temporary consumer group: $temp_group"
        
        # Consume all messages to clear the topic
        kafka-console-consumer.sh \
            --bootstrap-server localhost:9092 \
            --topic "$topic" \
            --group "$temp_group" \
            --from-beginning \
            --timeout-ms 5000 \
            --max-messages 10000 \
            > /dev/null 2>&1
        
        echo "  ✅ Cleared topic: $topic"
    else
        echo "  ℹ️  Topic $topic is already empty or doesn't exist"
    fi
}

# Clear each topic
for topic in "${TOPICS[@]}"; do
    clear_topic "$topic"
done

echo "🎉 Kafka topics cleared successfully!"
echo "Ready for clean testing..."
