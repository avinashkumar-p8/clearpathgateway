package com.anz.fastpayment.inward.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to flatten nested JSON structures into a flat HashMap with dot notation keys.
 * Handles complex Kafka Avro messages with nested objects and arrays.
 * 
 * Features:
 * - Recursive traversal of JSON structures
 * - Dot notation for nested objects (e.g., Header.EventInfo.EventID)
 * - Array indexing (e.g., Body.PmtAddRq[0].FromAcct.CurCode)
 * - Primitive value extraction (String, Number, Boolean)
 * - Null value skipping
 * - Generic and reusable for all Kafka message types
 */
public class JsonFlattener {
    
    private static final Logger log = LoggerFactory.getLogger(JsonFlattener.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Flattens a JsonNode into a flat HashMap with dot notation keys.
     * 
     * @param node The JsonNode to flatten
     * @return Flattened HashMap with dot notation keys and primitive values
     */
    public static Map<String, String> flatten(JsonNode node) {
        Map<String, String> flattenedMap = new HashMap<>();
        flattenNode("", node, flattenedMap);
        return flattenedMap;
    }
    
    /**
     * Flattens a JSON string into a flat HashMap.
     * 
     * @param jsonString The JSON string to flatten
     * @return Flattened HashMap with dot notation keys and primitive values
     * @throws RuntimeException if JSON parsing fails
     */
    public static Map<String, String> flatten(String jsonString) {
        try {
            JsonNode node = objectMapper.readTree(jsonString);
            return flatten(node);
        } catch (Exception e) {
            log.error("Failed to parse JSON string: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to parse JSON string", e);
        }
    }
    
    /**
     * Flattens a Map<String, Object> into a flat HashMap.
     * Useful for converting deserialized Kafka messages.
     * 
     * @param map The Map<String, Object> to flatten
     * @return Flattened HashMap with dot notation keys and primitive values
     */
    public static Map<String, String> flatten(Map<String, Object> map) {
        try {
            String jsonString = objectMapper.writeValueAsString(map);
            return flatten(jsonString);
        } catch (Exception e) {
            log.error("Failed to convert Map to JSON: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to convert Map to JSON", e);
        }
    }
    
    /**
     * Recursively flattens a JsonNode with the given prefix.
     * 
     * @param prefix The current key prefix (empty for root)
     * @param node The current JsonNode to process
     * @param flattenedMap The target HashMap to populate
     */
    private static void flattenNode(String prefix, JsonNode node, Map<String, String> flattenedMap) {
        if (node == null || node.isNull()) {
            return; // Skip null values
        }
        
        if (node.isTextual()) {
            // Handle String values
            String key = prefix.isEmpty() ? "value" : prefix;
            flattenedMap.put(key, node.asText());
            log.debug("Added string value: {} -> {}", key, node.asText());
            
        } else if (node.isNumber()) {
            // Handle Number values
            String key = prefix.isEmpty() ? "value" : prefix;
            flattenedMap.put(key, node.asText());
            log.debug("Added number value: {} -> {}", key, node.asText());
            
        } else if (node.isBoolean()) {
            // Handle Boolean values
            String key = prefix.isEmpty() ? "value" : prefix;
            flattenedMap.put(key, node.asText());
            log.debug("Added boolean value: {} -> {}", key, node.asText());
            
        } else if (node.isObject()) {
            // Handle Object nodes - traverse all fields
            ObjectNode objectNode = (ObjectNode) node;
            objectNode.fieldNames().forEachRemaining(fieldName -> {
                String newPrefix = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;
                flattenNode(newPrefix, objectNode.get(fieldName), flattenedMap);
            });
            
        } else if (node.isArray()) {
            // Handle Array nodes - traverse with indices
            ArrayNode arrayNode = (ArrayNode) node;
            for (int i = 0; i < arrayNode.size(); i++) {
                String newPrefix = prefix + "[" + i + "]";
                flattenNode(newPrefix, arrayNode.get(i), flattenedMap);
            }
            
        } else {
            // Handle other types (Binary, Missing, etc.)
            log.debug("Skipping non-primitive node type: {} at prefix: {}", node.getNodeType(), prefix);
        }
    }
    
    /**
     * Extracts specific fields from a flattened map using pattern matching.
     * Useful for finding values in complex nested structures.
     * 
     * @param flattenedMap The flattened HashMap
     * @param pattern The pattern to match (supports wildcards)
     * @return Map of matching keys and values
     */
    public static Map<String, String> extractFields(Map<String, String> flattenedMap, String pattern) {
        Map<String, String> extracted = new HashMap<>();
        String regexPattern = pattern.replace("*", ".*").replace("?", ".");
        
        flattenedMap.entrySet().stream()
            .filter(entry -> entry.getKey().matches(regexPattern))
            .forEach(entry -> extracted.put(entry.getKey(), entry.getValue()));
        
        return extracted;
    }
    
    /**
     * Finds the first occurrence of a field by partial key match.
     * Useful for finding values when the exact path is unknown.
     * 
     * @param flattenedMap The flattened HashMap
     * @param partialKey The partial key to search for
     * @return The first matching value, or null if not found
     */
    public static String findFirstMatch(Map<String, String> flattenedMap, String partialKey) {
        return flattenedMap.entrySet().stream()
            .filter(entry -> entry.getKey().contains(partialKey))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);
    }
    
    /**
     * Gets a summary of the flattened structure for debugging.
     * 
     * @param flattenedMap The flattened HashMap
     * @return Summary string with key count and sample keys
     */
    public static String getSummary(Map<String, String> flattenedMap) {
        if (flattenedMap.isEmpty()) {
            return "Empty flattened map";
        }
        
        StringBuilder summary = new StringBuilder();
        summary.append("Flattened map contains ").append(flattenedMap.size()).append(" keys\n");
        summary.append("Sample keys:\n");
        
        flattenedMap.keySet().stream()
            .limit(10) // Show first 10 keys
            .forEach(key -> summary.append("  ").append(key).append(" -> ").append(flattenedMap.get(key)).append("\n"));
        
        if (flattenedMap.size() > 10) {
            summary.append("  ... and ").append(flattenedMap.size() - 10).append(" more keys");
        }
        
        return summary.toString();
    }
}
