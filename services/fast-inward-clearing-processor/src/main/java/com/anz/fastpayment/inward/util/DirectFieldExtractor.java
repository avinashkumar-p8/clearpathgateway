package com.anz.fastpayment.inward.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Utility class to extract field values directly from JSON payloads using constant paths.
 * This eliminates the need to create a full HashMap and only extracts the required fields.
 */
public class DirectFieldExtractor {
    
    private static final Logger log = LoggerFactory.getLogger(DirectFieldExtractor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Extract a field value directly from a Map<String, Object> payload using a JSON path.
     * 
     * @param payload The payload to extract from
     * @param jsonPath The JSON path (e.g., "Body.PmtAddRq[0].FromAcct.CurCode")
     * @return The extracted value as String, or null if not found
     */
    public static String extractFieldValue(Map<String, Object> payload, String jsonPath) {
        try {
            // Convert Map to JsonNode for easier path traversal
            String jsonString = objectMapper.writeValueAsString(payload);
            JsonNode rootNode = objectMapper.readTree(jsonString);
            
            return extractValueFromNode(rootNode, jsonPath);
        } catch (Exception e) {
            log.debug("Failed to extract field '{}' from payload: {}", jsonPath, e.getMessage());
            return null;
        }
    }
    
    /**
     * Extract a field value directly from a JSON string using a JSON path.
     * 
     * @param jsonString The JSON string to extract from
     * @param jsonPath The JSON path (e.g., "Body.PmtAddRq[0].FromAcct.CurCode")
     * @return The extracted value as String, or null if not found
     */
    public static String extractFieldValue(String jsonString, String jsonPath) {
        try {
            JsonNode rootNode = objectMapper.readTree(jsonString);
            return extractValueFromNode(rootNode, jsonPath);
        } catch (Exception e) {
            log.debug("Failed to extract field '{}' from JSON string: {}", jsonPath, e.getMessage());
            return null;
        }
    }
    
    /**
     * Extract a field value from a JsonNode using a JSON path.
     * 
     * @param rootNode The root JsonNode
     * @param jsonPath The JSON path (e.g., "Body.PmtAddRq[0].FromAcct.CurCode")
     * @return The extracted value as String, or null if not found
     */
    private static String extractValueFromNode(JsonNode rootNode, String jsonPath) {
        if (rootNode == null || jsonPath == null || jsonPath.trim().isEmpty()) {
            return null;
        }
        
        String[] pathParts = jsonPath.split("\\.");
        JsonNode currentNode = rootNode;
        
        for (String part : pathParts) {
            if (currentNode == null) {
                return null;
            }
            
            // Handle array indexing (e.g., "PmtAddRq[0]")
            if (part.contains("[")) {
                String arrayName = part.substring(0, part.indexOf("["));
                String indexStr = part.substring(part.indexOf("[") + 1, part.indexOf("]"));
                
                try {
                    int index = Integer.parseInt(indexStr);
                    currentNode = currentNode.get(arrayName);
                    if (currentNode != null && currentNode.isArray() && index < currentNode.size()) {
                        currentNode = currentNode.get(index);
                    } else {
                        return null;
                    }
                } catch (NumberFormatException e) {
                    log.debug("Invalid array index in path '{}': {}", part, indexStr);
                    return null;
                }
            } else {
                // Handle regular object properties
                currentNode = currentNode.get(part);
            }
        }
        
        if (currentNode != null && !currentNode.isNull()) {
            if (currentNode.isTextual()) {
                return currentNode.asText();
            } else if (currentNode.isNumber()) {
                return currentNode.asText();
            } else if (currentNode.isBoolean()) {
                return currentNode.asText();
            } else {
                log.debug("Field '{}' is not a primitive value: {}", jsonPath, currentNode.getNodeType());
                return null;
            }
        }
        
        return null;
    }
    
    /**
     * Extract multiple field values from a payload using multiple paths.
     * 
     * @param payload The payload to extract from
     * @param jsonPaths Array of JSON paths
     * @return Map of path to extracted value
     */
    public static Map<String, String> extractMultipleFields(Map<String, Object> payload, String... jsonPaths) {
        Map<String, String> results = new java.util.HashMap<>();
        
        for (String path : jsonPaths) {
            String value = extractFieldValue(payload, path);
            if (value != null) {
                results.put(path, value);
            }
        }
        
        return results;
    }
    
    /**
     * Check if a field exists in the payload.
     * 
     * @param payload The payload to check
     * @param jsonPath The JSON path to check
     * @return true if the field exists and has a non-null value
     */
    public static boolean fieldExists(Map<String, Object> payload, String jsonPath) {
        return extractFieldValue(payload, jsonPath) != null;
    }
}
