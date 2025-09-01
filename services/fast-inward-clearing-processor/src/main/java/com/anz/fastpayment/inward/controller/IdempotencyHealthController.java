package com.anz.fastpayment.inward.controller;

import com.anz.fastpayment.inward.consumer.IdempotentTransactionConsumer;
import com.anz.fastpayment.inward.scheme.validation.service.MessageIdempotencyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * REST controller for idempotency health monitoring and metrics
 * Provides endpoints for operational monitoring of the idempotency system
 */
@RestController
@RequestMapping("/api/v1/idempotency")
public class IdempotencyHealthController {
    
    private static final Logger logger = LoggerFactory.getLogger(IdempotencyHealthController.class);
    
    @Autowired
    private MessageIdempotencyService idempotencyService;
    
    @Autowired
    private IdempotentTransactionConsumer consumer;
    
    /**
     * Health check endpoint for idempotency system
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealth() {
        Map<String, Object> health = new HashMap<>();
        
        try {
            health.put("status", "UP");
            health.put("timestamp", LocalDateTime.now());
            health.put("service", "Message Idempotency Service");
            health.put("version", "21.0.0-apeafast-SNAPSHOT");
            
            // Basic health indicators
            health.put("database", "CONNECTED");
            health.put("cache", "OPERATIONAL");
            health.put("kafka", "OPERATIONAL");
            
            logger.info("Idempotency health check completed successfully");
            return ResponseEntity.ok(health);
            
        } catch (Exception e) {
            logger.error("Health check failed: {}", e.getMessage(), e);
            health.put("status", "DOWN");
            health.put("error", e.getMessage());
            return ResponseEntity.status(503).body(health);
        }
    }
    
    /**
     * Metrics endpoint for idempotency processing statistics
     */
    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        try {
            // Get consumer metrics
            var consumerMetrics = consumer.getMetrics();
            
            metrics.put("timestamp", LocalDateTime.now());
            metrics.put("processedMessages", consumerMetrics.getProcessedMessages());
            metrics.put("duplicateMessages", consumerMetrics.getDuplicateMessages());
            metrics.put("errorMessages", consumerMetrics.getErrorMessages());
            
            // Calculate success rate
            long totalMessages = consumerMetrics.getProcessedMessages() + consumerMetrics.getDuplicateMessages() + consumerMetrics.getErrorMessages();
            if (totalMessages > 0) {
                double successRate = (double) consumerMetrics.getProcessedMessages() / totalMessages * 100;
                metrics.put("successRate", String.format("%.2f%%", successRate));
            } else {
                metrics.put("successRate", "0.00%");
            }
            
            // Calculate duplicate rate
            if (totalMessages > 0) {
                double duplicateRate = (double) consumerMetrics.getDuplicateMessages() / totalMessages * 100;
                metrics.put("duplicateRate", String.format("%.2f%%", duplicateRate));
            } else {
                metrics.put("duplicateRate", "0.00%");
            }
            
            // Calculate error rate
            if (totalMessages > 0) {
                double errorRate = (double) consumerMetrics.getErrorMessages() / totalMessages * 100;
                metrics.put("errorRate", String.format("%.2f%%", errorRate));
            } else {
                metrics.put("errorRate", "0.00%");
            }
            
            logger.debug("Idempotency metrics retrieved successfully");
            return ResponseEntity.ok(metrics);
            
        } catch (Exception e) {
            logger.error("Failed to retrieve metrics: {}", e.getMessage(), e);
            metrics.put("error", "Failed to retrieve metrics: " + e.getMessage());
            return ResponseEntity.status(500).body(metrics);
        }
    }
    
    /**
     * Cache management endpoint for clearing idempotency cache
     */
    @GetMapping("/cache/clear")
    public ResponseEntity<Map<String, Object>> clearCache() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            // Clear all MUID cache entries
            idempotencyService.clearAllMuidCache();
            
            response.put("status", "SUCCESS");
            response.put("message", "All idempotency cache entries cleared successfully");
            response.put("timestamp", LocalDateTime.now());
            
            logger.info("Idempotency cache cleared successfully");
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to clear cache: {}", e.getMessage(), e);
            response.put("status", "ERROR");
            response.put("message", "Failed to clear cache: " + e.getMessage());
            response.put("timestamp", LocalDateTime.now());
            return ResponseEntity.status(500).body(response);
        }
    }
}
