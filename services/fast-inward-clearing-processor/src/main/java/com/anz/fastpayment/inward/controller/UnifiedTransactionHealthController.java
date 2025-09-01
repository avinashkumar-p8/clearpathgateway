package com.anz.fastpayment.inward.controller;

import com.anz.fastpayment.inward.consumer.UnifiedTransactionConsumer;
import com.anz.fastpayment.inward.handler.TransactionProcessingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Health Controller for Unified Transaction Consumer
 * Provides monitoring and health check endpoints for the new unified consumer
 * Reuses patterns from existing EnhancedValidationHealthController
 */
@RestController
@RequestMapping("/api/v1/health/unified-transaction")
public class UnifiedTransactionHealthController {
    
    private static final Logger log = LoggerFactory.getLogger(UnifiedTransactionHealthController.class);
    
    private final UnifiedTransactionConsumer unifiedConsumer;
    private final TransactionProcessingHandler processingHandler;
    
    public UnifiedTransactionHealthController(UnifiedTransactionConsumer unifiedConsumer,
                                            TransactionProcessingHandler processingHandler) {
        this.unifiedConsumer = unifiedConsumer;
        this.processingHandler = processingHandler;
    }
    
    /**
     * Get unified consumer metrics
     */
    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Long>> getConsumerMetrics() {
        try {
            Map<String, Long> metrics = Map.of(
                "messagesReceived", unifiedConsumer.getMessagesReceived(),
                "messagesProcessed", unifiedConsumer.getMessagesProcessed(),
                "duplicateMessages", unifiedConsumer.getDuplicateMessages(),
                "validationFailures", unifiedConsumer.getValidationFailures(),
                "businessProcessingFailures", unifiedConsumer.getBusinessProcessingFailures(),
                "systemErrors", unifiedConsumer.getSystemErrors()
            );
            
            log.debug("Retrieved unified consumer metrics: {}", metrics);
            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            log.error("Error retrieving unified consumer metrics", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get processing handler statistics
     */
    @GetMapping("/processing-stats")
    public ResponseEntity<Map<String, Object>> getProcessingStatistics() {
        try {
            Map<String, Object> stats = processingHandler.getProcessingStatistics();
            
            log.debug("Retrieved processing statistics: {}", stats);
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            log.error("Error retrieving processing statistics", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Get processing handler metrics
     */
    @GetMapping("/processing-metrics")
    public ResponseEntity<Map<String, Long>> getProcessingMetrics() {
        try {
            Map<String, Long> metrics = processingHandler.getProcessingMetrics();
            
            log.debug("Retrieved processing metrics: {}", metrics);
            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            log.error("Error retrieving processing metrics", e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Reset processing metrics (for testing purposes)
     */
    @PostMapping("/reset-metrics")
    public ResponseEntity<Map<String, String>> resetMetrics() {
        try {
            processingHandler.resetProcessingMetrics();
            
            Map<String, String> response = Map.of(
                "status", "SUCCESS",
                "message", "Processing metrics reset successfully",
                "timestamp", java.time.LocalDateTime.now().toString()
            );
            
            log.info("Processing metrics reset successfully");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error resetting processing metrics", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "status", "ERROR",
                "message", "Failed to reset metrics: " + e.getMessage(),
                "timestamp", java.time.LocalDateTime.now().toString()
            ));
        }
    }
    
    /**
     * Health check endpoint
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, String>> getHealthStatus() {
        try {
            // Check if consumer is healthy by verifying metrics are accessible
            long messagesReceived = unifiedConsumer.getMessagesReceived();
            long messagesProcessed = unifiedConsumer.getMessagesProcessed();
            
            String status = "HEALTHY";
            if (messagesReceived > 0 && messagesProcessed == 0) {
                status = "DEGRADED"; // Messages received but none processed
            }
            
            Map<String, String> healthStatus = Map.of(
                "status", status,
                "service", "Unified Transaction Consumer",
                "messagesReceived", String.valueOf(messagesReceived),
                "messagesProcessed", String.valueOf(messagesProcessed),
                "timestamp", java.time.LocalDateTime.now().toString()
            );
            
            log.debug("Unified transaction consumer health check: {}", healthStatus);
            return ResponseEntity.ok(healthStatus);
            
        } catch (Exception e) {
            log.error("Error during health check", e);
            return ResponseEntity.status(503).body(Map.of(
                "status", "UNHEALTHY",
                "error", e.getMessage(),
                "timestamp", java.time.LocalDateTime.now().toString()
            ));
        }
    }
    
    /**
     * Get detailed health information
     */
    @GetMapping("/health-details")
    public ResponseEntity<Map<String, Object>> getHealthDetails() {
        try {
            Map<String, Object> healthDetails = Map.of(
                "consumer", Map.of(
                    "messagesReceived", unifiedConsumer.getMessagesReceived(),
                    "messagesProcessed", unifiedConsumer.getMessagesProcessed(),
                    "duplicateMessages", unifiedConsumer.getDuplicateMessages(),
                    "validationFailures", unifiedConsumer.getValidationFailures(),
                    "businessProcessingFailures", unifiedConsumer.getBusinessProcessingFailures(),
                    "systemErrors", unifiedConsumer.getSystemErrors()
                ),
                "processingHandler", processingHandler.getProcessingStatistics(),
                "timestamp", java.time.LocalDateTime.now().toString()
            );
            
            log.debug("Retrieved detailed health information: {}", healthDetails);
            return ResponseEntity.ok(healthDetails);
        } catch (Exception e) {
            log.error("Error retrieving detailed health information", e);
            return ResponseEntity.internalServerError().build();
        }
    }
}
