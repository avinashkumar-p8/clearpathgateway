package com.anz.fastpayment.inward.scheme.validation.util;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Avro Message Parser Utility for Banking Operations
 * Converts Avro GenericRecord messages into HashMap key-value pairs
 * for validation processing
 */
public class AvroMessageParser {
    
    private static final Logger log = LoggerFactory.getLogger(AvroMessageParser.class);
    
    /**
     * Parse Avro message into HashMap of key-value pairs
     * 
     * @param avroMessage The Avro GenericRecord message
     * @return HashMap containing extracted key-value pairs
     */
    public static Map<String, String> parseToHashMap(GenericRecord avroMessage) {
        Map<String, String> result = new HashMap<>();
        
        if (avroMessage == null) {
            log.warn("Attempted to parse null Avro message");
            return result;
        }
        
        try {
            // Extract fields from the Avro message
            extractFields(avroMessage, result, "");
            
            log.debug("Successfully parsed Avro message with {} fields", result.size());
            
        } catch (Exception e) {
            log.error("Error parsing Avro message", e);
            // Return empty map on error to prevent validation failures
        }
        
        return result;
    }
    
    /**
     * Recursively extract fields from Avro message
     * 
     * @param record The Avro record to extract from
     * @param result The result map to populate
     * @param prefix The field name prefix for nested records
     */
    private static void extractFields(GenericRecord record, Map<String, String> result, String prefix) {
        if (record == null) {
            return;
        }
        
        for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
            String fieldName = field.name();
            String fullFieldName = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;
            
            Object value = record.get(fieldName);
            
            if (value == null) {
                // Skip null values
                continue;
            }
            
            if (value instanceof GenericRecord) {
                // Recursively extract nested record
                extractFields((GenericRecord) value, result, fullFieldName);
            } else if (value instanceof java.util.Collection) {
                // Handle collections (arrays)
                handleCollection(value, result, fullFieldName);
            } else {
                // Convert primitive values to string
                String stringValue = value.toString();
                if (stringValue != null && !stringValue.trim().isEmpty()) {
                    result.put(fullFieldName, stringValue.trim());
                }
            }
        }
    }
    
    /**
     * Handle collection values (arrays)
     * 
     * @param collection The collection to process
     * @param result The result map to populate
     * @param fieldName The field name
     */
    private static void handleCollection(Object collection, Map<String, String> result, String fieldName) {
        if (collection instanceof java.util.Collection) {
            java.util.Collection<?> col = (java.util.Collection<?>) collection;
            int index = 0;
            for (Object item : col) {
                String indexedFieldName = fieldName + "[" + index + "]";
                
                if (item instanceof GenericRecord) {
                    extractFields((GenericRecord) item, result, indexedFieldName);
                } else if (item != null) {
                    result.put(indexedFieldName, item.toString().trim());
                }
                index++;
            }
        }
    }
    
    /**
     * Extract specific fields for validation
     * 
     * @param avroMessage The Avro message
     * @return Map containing only the fields needed for validation
     */
    public static Map<String, String> extractValidationFields(GenericRecord avroMessage) {
        Map<String, String> allFields = parseToHashMap(avroMessage);
        Map<String, String> validationFields = new HashMap<>();
        
        // Extract specific fields needed for validation
        // These paths are based on the UnifiedPaymentMessage schema structure
        String[] currencyPaths = {"Body.PmtAddRq[0].FromAcct.CurCode", "Body.PmtAddRq[0].ToAcct.CurCode"};
        String[] countryPaths = {"Body.PmtAddRq[0].FromFIData.Country", "Body.PmtAddRq[0].ToFIData.Country", "Procctxt.PmtDtls.ProcCtryCd"};
        String[] mmbidPaths = {"Header.MUID"};
        
        // Extract currency
        for (String path : currencyPaths) {
            if (allFields.containsKey(path)) {
                validationFields.put("currency", allFields.get(path));
                break;
            }
        }
        
        // Extract country
        for (String path : countryPaths) {
            if (allFields.containsKey(path)) {
                validationFields.put("country", allFields.get(path));
                break;
            }
        }
        
        // Extract MMBID
        for (String path : mmbidPaths) {
            if (allFields.containsKey(path)) {
                validationFields.put("mmbid", allFields.get(path));
                break;
            }
        }
        
        log.debug("Extracted {} validation fields from Avro message", validationFields.size());
        return validationFields;
    }
    
    /**
     * Convert Avro message to Map<String, Object> for direct field extraction
     * 
     * @param avroMessage The Avro GenericRecord message
     * @return Map containing the message structure for direct field extraction
     */
    public static Map<String, Object> convertToMap(GenericRecord avroMessage) {
        Map<String, Object> result = new HashMap<>();
        
        if (avroMessage == null) {
            log.warn("Attempted to convert null Avro message");
            return result;
        }
        
        try {
            // Convert the Avro message to a Map structure
            convertRecordToMap(avroMessage, result, "");
            
            log.debug("Successfully converted Avro message to Map with {} top-level entries", result.size());
            
        } catch (Exception e) {
            log.error("Error converting Avro message to Map", e);
            // Return empty map on error to prevent validation failures
        }
        
        return result;
    }
    
    /**
     * Recursively convert Avro record to Map<String, Object>
     * 
     * @param record The Avro record to convert
     * @param result The result map to populate
     * @param prefix The field name prefix for nested records
     */
    private static void convertRecordToMap(GenericRecord record, Map<String, Object> result, String prefix) {
        if (record == null) {
            return;
        }
        
        for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
            String fieldName = field.name();
            String fullFieldName = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;
            
            Object value = record.get(fieldName);
            
            if (value == null) {
                // Skip null values
                continue;
            }
            
            if (value instanceof GenericRecord) {
                // Recursively convert nested record
                Map<String, Object> nestedMap = new HashMap<>();
                convertRecordToMap((GenericRecord) value, nestedMap, fullFieldName);
                result.put(fieldName, nestedMap);
            } else if (value instanceof java.util.Collection) {
                // Handle collections (arrays)
                result.put(fieldName, handleCollectionToObject(value, fullFieldName));
            } else {
                // Store primitive values as-is
                result.put(fieldName, value);
            }
        }
    }
    
    /**
     * Handle collection values (arrays) for Object conversion
     * 
     * @param collection The collection to process
     * @param fieldName The field name
     * @return List containing the converted collection items
     */
    private static java.util.List<Object> handleCollectionToObject(Object collection, String fieldName) {
        java.util.List<Object> result = new java.util.ArrayList<>();
        
        if (collection instanceof java.util.Collection) {
            java.util.Collection<?> col = (java.util.Collection<?>) collection;
            for (Object item : col) {
                if (item instanceof GenericRecord) {
                    Map<String, Object> nestedMap = new HashMap<>();
                    convertRecordToMap((GenericRecord) item, nestedMap, fieldName);
                    result.add(nestedMap);
                } else {
                    result.add(item);
                }
            }
        }
        
        return result;
    }
}
