package com.anz.fastpayment.inward.scheme.validation.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Utility to convert Map-based payload structures to Array-based structures
 * This addresses the mismatch between different message formats and DirectFieldExtractor expectations
 * 
 * ISSUE: Some message sources use Map for messages field, but DirectFieldExtractor expects Array
 * SOLUTION: Convert Map structure to Array structure for consistent validation processing
 * 
 * This is used in the actual service to handle various message formats from different sources
 */
@Component
public class MapToArrayConverter {

    private static final Logger log = LoggerFactory.getLogger(MapToArrayConverter.class);

    /**
     * Convert Map-based payload structure to Array-based structure
     * This ensures consistent processing regardless of input message format
     * 
     * @param originalPayload The original payload that may have Map-based structure
     * @return Converted payload with Array-based structure
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> convertMapToArrayStructure(Map<String, Object> originalPayload) {
        if (originalPayload == null) {
            log.warn("Original payload is null, returning null");
            return null;
        }

        log.debug("Converting Map-based payload structure to Array-based structure");
        
        Map<String, Object> convertedPayload = new HashMap<>();
        
        for (Map.Entry<String, Object> entry : originalPayload.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            if ("Body".equals(key) && value instanceof Map) {
                Map<String, Object> body = (Map<String, Object>) value;
                convertedPayload.put(key, convertBodyStructure(body));
            } else {
                convertedPayload.put(key, value);
            }
        }
        
        log.debug("Successfully converted payload structure");
        return convertedPayload;
    }

    /**
     * Convert Body structure from Map-based to Array-based
     * Handles the messages field conversion specifically
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> convertBodyStructure(Map<String, Object> body) {
        if (body == null) {
            return body;
        }

        Map<String, Object> convertedBody = new HashMap<>();
        
        for (Map.Entry<String, Object> entry : body.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            if ("messages".equals(key) && value instanceof Map) {
                // Convert messages Map to Array
                Map<String, Object> messagesMap = (Map<String, Object>) value;
                List<Object> messagesArray = convertMessagesMapToArray(messagesMap);
                convertedBody.put(key, messagesArray);
                log.debug("Converted messages Map to Array with {} entries", messagesArray.size());
            } else {
                convertedBody.put(key, value);
            }
        }
        
        return convertedBody;
    }

    /**
     * Convert messages Map to Array
     * Map keys like "0", "1", "2" become array indices
     * This handles the common case where messages are indexed as strings
     */
    @SuppressWarnings("unchecked")
    private static List<Object> convertMessagesMapToArray(Map<String, Object> messagesMap) {
        if (messagesMap == null || messagesMap.isEmpty()) {
            return new ArrayList<>();
        }

        List<Object> messagesArray = new ArrayList<>();
        
        // Sort keys to maintain order (handle numeric string keys)
        List<String> sortedKeys = new ArrayList<>(messagesMap.keySet());
        try {
            sortedKeys.sort(Comparator.comparing(Integer::parseInt));
        } catch (NumberFormatException e) {
            // If keys are not numeric, use natural string ordering
            sortedKeys.sort(String::compareTo);
            log.debug("Message keys are not numeric, using string ordering");
        }
        
        for (String key : sortedKeys) {
            Object messageValue = messagesMap.get(key);
            if (messageValue instanceof Map) {
                // Recursively convert nested structures
                messagesArray.add(convertNestedStructure((Map<String, Object>) messageValue));
            } else {
                messagesArray.add(messageValue);
            }
        }
        
        return messagesArray;
    }

    /**
     * Recursively convert nested Map structures
     * Ensures all nested objects maintain proper structure
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> convertNestedStructure(Map<String, Object> nestedMap) {
        if (nestedMap == null) {
            return nestedMap;
        }

        Map<String, Object> convertedNested = new HashMap<>();
        
        for (Map.Entry<String, Object> entry : nestedMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            if (value instanceof Map) {
                convertedNested.put(key, convertNestedStructure((Map<String, Object>) value));
            } else if (value instanceof List) {
                // Lists are already in correct format, but check for nested Maps
                convertedNested.put(key, convertListStructure((List<Object>) value));
            } else {
                convertedNested.put(key, value);
            }
        }
        
        return convertedNested;
    }

    /**
     * Convert List structure to ensure nested Maps are properly converted
     */
    @SuppressWarnings("unchecked")
    private static List<Object> convertListStructure(List<Object> list) {
        if (list == null) {
            return list;
        }

        List<Object> convertedList = new ArrayList<>();
        
        for (Object item : list) {
            if (item instanceof Map) {
                convertedList.add(convertNestedStructure((Map<String, Object>) item));
            } else {
                convertedList.add(item);
            }
        }
        
        return convertedList;
    }

    /**
     * Check if a payload needs conversion from Map-based to Array-based structure
     * This helps determine if conversion is necessary before processing
     * 
     * @param payload The payload to check
     * @return true if conversion is needed, false otherwise
     */
    @SuppressWarnings("unchecked")
    public static boolean needsConversion(Map<String, Object> payload) {
        if (payload == null) {
            return false;
        }

        Object body = payload.get("Body");
        if (!(body instanceof Map)) {
            return false;
        }

        Map<String, Object> bodyMap = (Map<String, Object>) body;
        Object messages = bodyMap.get("messages");
        
        // If messages is a Map, it needs conversion
        return messages instanceof Map;
    }

    /**
     * Safely convert payload structure, only if needed
     * This is the main method to use in the service
     * 
     * @param payload The payload to potentially convert
     * @return Converted payload if conversion was needed, original payload otherwise
     */
    public static Map<String, Object> convertIfNeeded(Map<String, Object> payload) {
        if (payload == null) {
            log.warn("Payload is null, cannot convert");
            return null;
        }

        if (needsConversion(payload)) {
            log.info("Payload structure needs conversion from Map-based to Array-based");
            return convertMapToArrayStructure(payload);
        } else {
            log.debug("Payload structure is already in correct format, no conversion needed");
            return payload;
        }
    }

    /**
     * Create a test payload with Map-based structure for testing purposes
     * This method is primarily for testing and development
     */
    public static Map<String, Object> createMapBasedPayload(String currency, String country) {
        Map<String, Object> payload = new HashMap<>();
        
        // Header
        Map<String, Object> header = new HashMap<>();
        header.put("ComponentName", "PSPAPFAFAST");
        header.put("UUID", "G3I400071311436B");
        header.put("MUID", "G3I400071311436B");
        payload.put("Header", header);
        
        // Body with Map-based messages structure
        Map<String, Object> body = new HashMap<>();
        Map<String, Object> messages = new HashMap<>();
        
        // Message 0
        Map<String, Object> message0 = new HashMap<>();
        Map<String, Object> instruction = new HashMap<>();
        Map<String, Object> msgAddRq = new HashMap<>();
        Map<String, Object> msgDtls = new HashMap<>();
        Map<String, Object> drctDbtTxInf = new HashMap<>();
        
        drctDbtTxInf.put("IntrBkSttlmAmt", "215.00");
        drctDbtTxInf.put("IntrBkSttlmCCY", currency);
        drctDbtTxInf.put("IntrBkSttlmDt", "2024-01-15");
        
        Map<String, Object> cdtrAgt = new HashMap<>();
        Map<String, Object> finInstnId = new HashMap<>();
        Map<String, Object> clrSysMmbId = new HashMap<>();
        clrSysMmbId.put("MmbId", "DBSGSGSG");
        finInstnId.put("ClrSysMmbId", clrSysMmbId);
        cdtrAgt.put("FinInstnId", finInstnId);
        drctDbtTxInf.put("CdtrAgt", cdtrAgt);
        
        Map<String, Object> instgAgt = new HashMap<>();
        Map<String, Object> instgFinInstnId = new HashMap<>();
        Map<String, Object> instgClrSysMmbId = new HashMap<>();
        instgClrSysMmbId.put("MmbId", "ANZBSGSG");
        instgFinInstnId.put("ClrSysMmbId", instgClrSysMmbId);
        instgAgt.put("FinInstnId", instgFinInstnId);
        drctDbtTxInf.put("InstgAgt", instgAgt);
        
        Map<String, Object> dbtrAgt = new HashMap<>();
        Map<String, Object> dbtrFinInstnId = new HashMap<>();
        Map<String, Object> dbtrClrSysMmbId = new HashMap<>();
        dbtrClrSysMmbId.put("MmbId", "UOVBSGSG");
        dbtrFinInstnId.put("ClrSysMmbId", dbtrClrSysMmbId);
        dbtrAgt.put("FinInstnId", dbtrFinInstnId);
        drctDbtTxInf.put("DbtrAgt", dbtrAgt);
        
        Map<String, Object> instdAgt = new HashMap<>();
        Map<String, Object> instdFinInstnId = new HashMap<>();
        Map<String, Object> instdClrSysMmbId = new HashMap<>();
        instdClrSysMmbId.put("MmbId", "UOVBSGSG");
        instdFinInstnId.put("ClrSysMmbId", instdClrSysMmbId);
        instdAgt.put("FinInstnId", instdFinInstnId);
        drctDbtTxInf.put("InstdAgt", instdAgt);
        
        msgDtls.put("DrctDbtTxInf", drctDbtTxInf);
        msgAddRq.put("MsgDtls", msgDtls);
        instruction.put("MsgAddRq", msgAddRq);
        message0.put("instruction", instruction);
        
        messages.put("0", message0); // Map-based structure
        body.put("messages", messages);
        payload.put("Body", body);
        
        return payload;
    }

    /**
     * Create a test payload with Array-based structure for testing purposes
     * This method is primarily for testing and development
     */
    public static Map<String, Object> createArrayBasedPayload(String currency, String country) {
        Map<String, Object> payload = new HashMap<>();
        
        // Header
        Map<String, Object> header = new HashMap<>();
        header.put("ComponentName", "PSPAPFAFAST");
        header.put("UUID", "G3I400071311436B");
        header.put("MUID", "G3I400071311436B");
        payload.put("Header", header);
        
        // Body with Array-based messages structure
        Map<String, Object> body = new HashMap<>();
        List<Object> messages = new ArrayList<>();
        
        // Message 0
        Map<String, Object> message0 = new HashMap<>();
        Map<String, Object> instruction = new HashMap<>();
        Map<String, Object> msgAddRq = new HashMap<>();
        Map<String, Object> msgDtls = new HashMap<>();
        Map<String, Object> drctDbtTxInf = new HashMap<>();
        
        drctDbtTxInf.put("IntrBkSttlmAmt", "215.00");
        drctDbtTxInf.put("IntrBkSttlmCCY", currency);
        drctDbtTxInf.put("IntrBkSttlmDt", "2024-01-15");
        
        Map<String, Object> cdtrAgt = new HashMap<>();
        Map<String, Object> finInstnId = new HashMap<>();
        Map<String, Object> clrSysMmbId = new HashMap<>();
        clrSysMmbId.put("MmbId", "DBSGSGSG");
        finInstnId.put("ClrSysMmbId", clrSysMmbId);
        cdtrAgt.put("FinInstnId", finInstnId);
        drctDbtTxInf.put("CdtrAgt", cdtrAgt);
        
        Map<String, Object> instgAgt = new HashMap<>();
        Map<String, Object> instgFinInstnId = new HashMap<>();
        Map<String, Object> instgClrSysMmbId = new HashMap<>();
        instgClrSysMmbId.put("MmbId", "ANZBSGSG");
        instgFinInstnId.put("ClrSysMmbId", instgClrSysMmbId);
        instgAgt.put("FinInstnId", instgFinInstnId);
        drctDbtTxInf.put("InstgAgt", instgAgt);
        
        Map<String, Object> dbtrAgt = new HashMap<>();
        Map<String, Object> dbtrFinInstnId = new HashMap<>();
        Map<String, Object> dbtrClrSysMmbId = new HashMap<>();
        dbtrClrSysMmbId.put("MmbId", "UOVBSGSG");
        dbtrFinInstnId.put("ClrSysMmbId", dbtrClrSysMmbId);
        dbtrAgt.put("FinInstnId", dbtrFinInstnId);
        drctDbtTxInf.put("DbtrAgt", dbtrAgt);
        
        Map<String, Object> instdAgt = new HashMap<>();
        Map<String, Object> instdFinInstnId = new HashMap<>();
        Map<String, Object> instdClrSysMmbId = new HashMap<>();
        instdClrSysMmbId.put("MmbId", "UOVBSGSG");
        instdFinInstnId.put("ClrSysMmbId", instdClrSysMmbId);
        instdAgt.put("FinInstnId", instdFinInstnId);
        drctDbtTxInf.put("InstdAgt", instdAgt);
        
        msgDtls.put("DrctDbtTxInf", drctDbtTxInf);
        msgAddRq.put("MsgDtls", msgDtls);
        instruction.put("MsgAddRq", msgAddRq);
        message0.put("instruction", instruction);
        
        messages.add(message0); // Array-based structure
        body.put("messages", messages);
        payload.put("Body", body);
        
        return payload;
    }
}
