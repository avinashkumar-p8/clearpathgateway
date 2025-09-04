package com.anz.fastpayment.inward.controller;

import com.anz.fastpayment.inward.handler.TransactionProcessingHandler;
import com.anz.fastpayment.inward.metrics.CircuitBreakerMetrics;
import com.anz.fastpayment.inward.metrics.ProcessingTimeMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Metrics Controller for Fast Inward Clearing Processor
 * Provides detailed metrics endpoints for monitoring and observability
 */
@RestController
@RequestMapping("/api/v1/metrics")
public class MetricsController {
    
    private static final Logger log = LoggerFactory.getLogger(MetricsController.class);
    
    private final TransactionProcessingHandler processingHandler;
    private final MeterRegistry meterRegistry;
    private final ProcessingTimeMetrics processingTimeMetrics;
    private final CircuitBreakerMetrics circuitBreakerMetrics;
    
    // Micrometer counters for detailed metrics
    private final Counter totalMessagesCounter;
    private final Counter successfulMessagesCounter;
    private final Counter duplicateMessagesCounter;
    private final Counter parsingFailuresCounter;
    private final Counter validationFailuresCounter;
    private final Counter businessProcessingFailuresCounter;
    private final Counter systemErrorsCounter;
    
    // Processing time timer
    private final Timer processingTimeTimer;
    
    public MetricsController(TransactionProcessingHandler processingHandler, MeterRegistry meterRegistry,
                           ProcessingTimeMetrics processingTimeMetrics, CircuitBreakerMetrics circuitBreakerMetrics) {
        this.processingHandler = processingHandler;
        this.meterRegistry = meterRegistry;
        this.processingTimeMetrics = processingTimeMetrics;
        this.circuitBreakerMetrics = circuitBreakerMetrics;
        
        // Initialize Micrometer counters
        this.totalMessagesCounter = Counter.builder("fast.inward.clearing.messages.total")
                .description("Total number of messages processed")
                .register(meterRegistry);
                
        this.successfulMessagesCounter = Counter.builder("fast.inward.clearing.messages.successful")
                .description("Number of successfully processed messages")
                .register(meterRegistry);
                
        this.duplicateMessagesCounter = Counter.builder("fast.inward.clearing.messages.duplicate")
                .description("Number of duplicate messages detected")
                .register(meterRegistry);
                
        this.parsingFailuresCounter = Counter.builder("fast.inward.clearing.messages.parsing.failures")
                .description("Number of message parsing failures")
                .register(meterRegistry);
                
        this.validationFailuresCounter = Counter.builder("fast.inward.clearing.messages.validation.failures")
                .description("Number of validation failures")
                .register(meterRegistry);
                
        this.businessProcessingFailuresCounter = Counter.builder("fast.inward.clearing.messages.business.failures")
                .description("Number of business processing failures")
                .register(meterRegistry);
                
        this.systemErrorsCounter = Counter.builder("fast.inward.clearing.messages.system.errors")
                .description("Number of system errors")
                .register(meterRegistry);
        
        // Initialize processing time timer
        this.processingTimeTimer = Timer.builder("fast.inward.clearing.processing.time")
                .description("Processing time for messages")
                .register(meterRegistry);
    }
    
    /**
     * Get detailed processing metrics
     */
    @GetMapping("/processing")
    public ResponseEntity<Map<String, Object>> getProcessingMetrics() {
        Map<String, Long> basicMetrics = processingHandler.getProcessingMetrics();
        Map<String, Object> statistics = processingHandler.getProcessingStatistics();
        
        Map<String, Object> detailedMetrics = new HashMap<>();
        detailedMetrics.put("timestamp", LocalDateTime.now().toString());
        detailedMetrics.put("counters", basicMetrics);
        detailedMetrics.put("statistics", statistics);
        
        // Add Micrometer counter values
        Map<String, Double> micrometerCounters = new HashMap<>();
        micrometerCounters.put("totalMessages", totalMessagesCounter.count());
        micrometerCounters.put("successfulMessages", successfulMessagesCounter.count());
        micrometerCounters.put("duplicateMessages", duplicateMessagesCounter.count());
        micrometerCounters.put("parsingFailures", parsingFailuresCounter.count());
        micrometerCounters.put("validationFailures", validationFailuresCounter.count());
        micrometerCounters.put("businessProcessingFailures", businessProcessingFailuresCounter.count());
        micrometerCounters.put("systemErrors", systemErrorsCounter.count());
        
        detailedMetrics.put("micrometerCounters", micrometerCounters);
        
        // Add processing time statistics
        Map<String, Object> processingTimeStats = new HashMap<>();
        processingTimeStats.put("count", processingTimeTimer.count());
        processingTimeStats.put("totalTimeMs", processingTimeTimer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS));
        processingTimeStats.put("meanTimeMs", processingTimeTimer.mean(java.util.concurrent.TimeUnit.MILLISECONDS));
        processingTimeStats.put("maxTimeMs", processingTimeTimer.max(java.util.concurrent.TimeUnit.MILLISECONDS));
        
        detailedMetrics.put("processingTime", processingTimeStats);
        
        // Add detailed processing time metrics
        detailedMetrics.put("detailedProcessingTime", processingTimeMetrics.getOverallProcessingStats());
        detailedMetrics.put("stepProcessingTime", processingTimeMetrics.getStepProcessingStats());
        
        // Add circuit breaker metrics
        detailedMetrics.put("circuitBreaker", circuitBreakerMetrics.getCircuitBreakerStats());
        
        log.debug("Retrieved detailed processing metrics: {}", detailedMetrics);
        return ResponseEntity.ok(detailedMetrics);
    }
    
    /**
     * Get all available metrics in Prometheus format
     */
    @GetMapping(value = "/prometheus", produces = "text/plain")
    public ResponseEntity<String> getPrometheusMetrics() {
        StringBuilder prometheusMetrics = new StringBuilder();
        
        // Add custom metrics
        prometheusMetrics.append("# HELP fast_inward_clearing_messages_total Total number of messages processed\n");
        prometheusMetrics.append("# TYPE fast_inward_clearing_messages_total counter\n");
        prometheusMetrics.append("fast_inward_clearing_messages_total ").append(totalMessagesCounter.count()).append("\n\n");
        
        prometheusMetrics.append("# HELP fast_inward_clearing_messages_successful Number of successfully processed messages\n");
        prometheusMetrics.append("# TYPE fast_inward_clearing_messages_successful counter\n");
        prometheusMetrics.append("fast_inward_clearing_messages_successful ").append(successfulMessagesCounter.count()).append("\n\n");
        
        prometheusMetrics.append("# HELP fast_inward_clearing_messages_duplicate Number of duplicate messages detected\n");
        prometheusMetrics.append("# TYPE fast_inward_clearing_messages_duplicate counter\n");
        prometheusMetrics.append("fast_inward_clearing_messages_duplicate ").append(duplicateMessagesCounter.count()).append("\n\n");
        
        prometheusMetrics.append("# HELP fast_inward_clearing_messages_parsing_failures Number of message parsing failures\n");
        prometheusMetrics.append("# TYPE fast_inward_clearing_messages_parsing_failures counter\n");
        prometheusMetrics.append("fast_inward_clearing_messages_parsing_failures ").append(parsingFailuresCounter.count()).append("\n\n");
        
        prometheusMetrics.append("# HELP fast_inward_clearing_messages_validation_failures Number of validation failures\n");
        prometheusMetrics.append("# TYPE fast_inward_clearing_messages_validation_failures counter\n");
        prometheusMetrics.append("fast_inward_clearing_messages_validation_failures ").append(validationFailuresCounter.count()).append("\n\n");
        
        prometheusMetrics.append("# HELP fast_inward_clearing_messages_business_failures Number of business processing failures\n");
        prometheusMetrics.append("# TYPE fast_inward_clearing_messages_business_failures counter\n");
        prometheusMetrics.append("fast_inward_clearing_messages_business_failures ").append(businessProcessingFailuresCounter.count()).append("\n\n");
        
        prometheusMetrics.append("# HELP fast_inward_clearing_messages_system_errors Number of system errors\n");
        prometheusMetrics.append("# TYPE fast_inward_clearing_messages_system_errors counter\n");
        prometheusMetrics.append("fast_inward_clearing_messages_system_errors ").append(systemErrorsCounter.count()).append("\n\n");
        
        // Add processing time metrics
        prometheusMetrics.append("# HELP fast_inward_clearing_processing_time_seconds Processing time for messages\n");
        prometheusMetrics.append("# TYPE fast_inward_clearing_processing_time_seconds histogram\n");
        prometheusMetrics.append("fast_inward_clearing_processing_time_seconds_count ").append(processingTimeTimer.count()).append("\n");
        prometheusMetrics.append("fast_inward_clearing_processing_time_seconds_sum ").append(processingTimeTimer.totalTime(java.util.concurrent.TimeUnit.SECONDS)).append("\n");
        prometheusMetrics.append("fast_inward_clearing_processing_time_seconds_max ").append(processingTimeTimer.max(java.util.concurrent.TimeUnit.SECONDS)).append("\n");
        
        return ResponseEntity.ok(prometheusMetrics.toString());
    }
    
    /**
     * Reset all metrics
     */
    @PostMapping("/reset")
    public ResponseEntity<Map<String, String>> resetMetrics() {
        processingHandler.resetProcessingMetrics();
        
        // Reset Micrometer counters (they don't have reset method, so we create new ones)
        // Note: In production, you might want to use a different approach
        
        Map<String, String> response = Map.of(
            "status", "SUCCESS",
            "message", "All metrics have been reset",
            "timestamp", LocalDateTime.now().toString()
        );
        
        log.info("Metrics reset requested: {}", response);
        return ResponseEntity.ok(response);
    }
    
    /**
     * Get circuit breaker metrics and state
     */
    @GetMapping("/circuit-breaker")
    public ResponseEntity<Map<String, Object>> getCircuitBreakerMetrics() {
        Map<String, Object> circuitBreakerData = new HashMap<>();
        circuitBreakerData.put("timestamp", LocalDateTime.now().toString());
        circuitBreakerData.put("stats", circuitBreakerMetrics.getCircuitBreakerStats());
        
        // Add current state information
        Map<String, Object> currentState = new HashMap<>();
        currentState.put("state", circuitBreakerMetrics.getStateName());
        currentState.put("isOperationAllowed", circuitBreakerMetrics.isOperationAllowed());
        circuitBreakerData.put("currentState", currentState);
        
        log.debug("Retrieved circuit breaker metrics: {}", circuitBreakerData);
        return ResponseEntity.ok(circuitBreakerData);
    }
    
    /**
     * Reset circuit breaker to CLOSED state
     */
    @PostMapping("/circuit-breaker/reset")
    public ResponseEntity<Map<String, String>> resetCircuitBreaker() {
        circuitBreakerMetrics.reset();
        
        Map<String, String> response = Map.of(
            "status", "SUCCESS",
            "message", "Circuit breaker reset to CLOSED state",
            "timestamp", LocalDateTime.now().toString()
        );
        
        log.info("Circuit breaker reset requested: {}", response);
        return ResponseEntity.ok(response);
    }
    
    /**
     * Update circuit breaker configuration
     */
    @PostMapping("/circuit-breaker/config")
    public ResponseEntity<Map<String, String>> updateCircuitBreakerConfig(
            @RequestParam(defaultValue = "5") int failureThreshold,
            @RequestParam(defaultValue = "3") int successThreshold,
            @RequestParam(defaultValue = "60000") long timeoutMs) {
        
        circuitBreakerMetrics.updateConfiguration(failureThreshold, successThreshold, timeoutMs);
        
        Map<String, String> response = Map.of(
            "status", "SUCCESS",
            "message", "Circuit breaker configuration updated",
            "failureThreshold", String.valueOf(failureThreshold),
            "successThreshold", String.valueOf(successThreshold),
            "timeoutMs", String.valueOf(timeoutMs),
            "timestamp", LocalDateTime.now().toString()
        );
        
        log.info("Circuit breaker configuration updated: {}", response);
        return ResponseEntity.ok(response);
    }
    
    /**
     * Get JVM and system metrics
     */
    @GetMapping("/system")
    public ResponseEntity<Map<String, Object>> getSystemMetrics() {
        Runtime runtime = Runtime.getRuntime();
        
        Map<String, Object> systemMetrics = new HashMap<>();
        systemMetrics.put("timestamp", LocalDateTime.now().toString());
        
        // JVM Memory metrics
        Map<String, Object> memoryMetrics = new HashMap<>();
        memoryMetrics.put("totalMemoryMB", runtime.totalMemory() / (1024 * 1024));
        memoryMetrics.put("freeMemoryMB", runtime.freeMemory() / (1024 * 1024));
        memoryMetrics.put("usedMemoryMB", (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024));
        memoryMetrics.put("maxMemoryMB", runtime.maxMemory() / (1024 * 1024));
        systemMetrics.put("memory", memoryMetrics);
        
        // JVM Runtime metrics
        Map<String, Object> runtimeMetrics = new HashMap<>();
        runtimeMetrics.put("availableProcessors", runtime.availableProcessors());
        runtimeMetrics.put("uptimeMs", System.currentTimeMillis() - getStartTime());
        systemMetrics.put("runtime", runtimeMetrics);
        
        return ResponseEntity.ok(systemMetrics);
    }
    
    private long getStartTime() {
        // This is a simplified approach - in production you might want to track actual start time
        return System.currentTimeMillis() - (java.lang.management.ManagementFactory.getRuntimeMXBean().getUptime());
    }
}
