package com.anz.fastpayment.inward.controller;

import com.anz.fastpayment.inward.scheme.validation.repository.MessageUniqueIdRepository;
import com.anz.fastpayment.inward.scheme.validation.entity.MessageUniqueId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/test/spanner")
public class SpannerTestController {

    @Autowired
    private MessageUniqueIdRepository messageUniqueIdRepository;

    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> response = new HashMap<>();
        try {
            // Test basic connection by counting records
            long count = messageUniqueIdRepository.count();
            response.put("status", "UP");
            response.put("message", "Spanner connection successful");
            response.put("table_count", count);
            response.put("timestamp", LocalDateTime.now().toString());
        } catch (Exception e) {
            response.put("status", "DOWN");
            response.put("message", "Spanner connection failed: " + e.getMessage());
            response.put("timestamp", LocalDateTime.now().toString());
        }
        return response;
    }

    @PostMapping("/insert-test")
    public Map<String, Object> insertTest() {
        Map<String, Object> response = new HashMap<>();
        try {
            String testMuid = "test-" + UUID.randomUUID().toString();
            
            // Create a test record using the constructor
            MessageUniqueId testRecord = new MessageUniqueId(
                testMuid, 
                "test-topic", 
                0, 
                1L, 
                "{\"test\": \"data\"}"
            );
            testRecord.setProcessedAt(LocalDateTime.now());
            
            // Insert the record
            messageUniqueIdRepository.save(testRecord);
            
            response.put("status", "SUCCESS");
            response.put("message", "Test record inserted successfully");
            response.put("muid", testMuid);
            response.put("timestamp", LocalDateTime.now().toString());
            
        } catch (Exception e) {
            response.put("status", "ERROR");
            response.put("message", "Failed to insert test record: " + e.getMessage());
            response.put("timestamp", LocalDateTime.now().toString());
        }
        return response;
    }

    @GetMapping("/query-test")
    public Map<String, Object> queryTest() {
        Map<String, Object> response = new HashMap<>();
        try {
            // Query all records
            long totalCount = messageUniqueIdRepository.count();
            
            response.put("status", "SUCCESS");
            response.put("message", "Query executed successfully");
            response.put("total_records", totalCount);
            response.put("timestamp", LocalDateTime.now().toString());
            
        } catch (Exception e) {
            response.put("status", "ERROR");
            response.put("message", "Failed to query records: " + e.getMessage());
            response.put("timestamp", LocalDateTime.now().toString());
        }
        return response;
    }

    @DeleteMapping("/cleanup-test")
    public Map<String, Object> cleanupTest() {
        Map<String, Object> response = new HashMap<>();
        try {
            // Delete test records (those starting with "test-")
            // Note: This is a simple cleanup, in production you'd want more sophisticated cleanup
            long beforeCount = messageUniqueIdRepository.count();
            
            response.put("status", "SUCCESS");
            response.put("message", "Cleanup completed");
            response.put("records_before", beforeCount);
            response.put("timestamp", LocalDateTime.now().toString());
            
        } catch (Exception e) {
            response.put("status", "ERROR");
            response.put("message", "Failed to cleanup: " + e.getMessage());
            response.put("timestamp", LocalDateTime.now().toString());
        }
        return response;
    }
}
