#!/bin/bash

echo "=== CLEANING UP TEMPORARY CHANGES ==="

echo "1. Removing temporary test file..."
rm -f src/test/java/com/anz/fastpayment/inward/handler/impl/MessageParsingHandlerFullMessageTest.java

echo "2. Reverting MessageParsingHandlerImpl to original behavior..."
cat > temp_revert.txt << 'EOF'
    @Override
    public Map<String, Object> extractMessageSection(ProcessingContext context) {
        try {
            GenericRecord avroMessage = context.getAvroMessage();
            
            if (avroMessage == null) {
                logger.warn("Avro message is null for transaction: {}", context.getTransactionId());
                return new HashMap<>();
            }
            
            // Extract only the 'messages' array from the Avro message
            Object messagesArray = avroMessage.get("messages");
            
            if (messagesArray != null && messagesArray instanceof java.util.Collection) {
                java.util.Collection<?> messages = (java.util.Collection<?>) messagesArray;
                
                if (!messages.isEmpty()) {
                    // Extract only the first message (messages[0])
                    Object firstMessage = messages.iterator().next();
                    
                    if (firstMessage instanceof GenericRecord) {
                        logger.debug("Successfully extracted first message from messages array for transaction: {}", 
                                   context.getTransactionId());
                        return AvroMessageParser.convertToMap((GenericRecord) firstMessage);
                    } else {
                        logger.warn("First message is not a GenericRecord: {} for transaction: {}", 
                                  firstMessage.getClass().getSimpleName(), context.getTransactionId());
                        return new HashMap<>();
                    }
                } else {
                    logger.warn("Messages array is empty for transaction: {}", context.getTransactionId());
                    return new HashMap<>();
                }
            } else {
                logger.warn("No 'messages' section found in Avro message or not a collection for transaction: {}", 
                          context.getTransactionId());
                return new HashMap<>();
            }
            
        } catch (Exception e) {
            logger.error("Error extracting Message section from Avro message for transaction: {} - Error: {}", 
                        context.getTransactionId(), e.getMessage(), e);
            return new HashMap<>();
        }
    }
EOF

echo "3. To revert MessageParsingHandlerImpl, replace the extractMessageSection method with the content in temp_revert.txt"
echo "4. Remove this cleanup script: rm cleanup-temp-changes.sh"
echo "5. Remove temp file: rm temp_revert.txt"

echo "=== CLEANUP INSTRUCTIONS COMPLETE ==="
