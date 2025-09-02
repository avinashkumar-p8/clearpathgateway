package com.anz.fastpayment.inward.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Simple Health Controller for Fast Inward Clearing Processor
 * Provides basic health check and service status
 */
@RestController
@RequestMapping("/api/v1/health")
public class HealthController {
    
    private static final Logger log = LoggerFactory.getLogger(HealthController.class);
    
    /**
     * Basic health check endpoint
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, String>> getHealthStatus() {
        Map<String, String> status = Map.of(
            "status", "HEALTHY",
            "service", "Fast Inward Clearing Processor",
            "timestamp", LocalDateTime.now().toString()
        );
        
        log.debug("Health check: {}", status);
        return ResponseEntity.ok(status);
    }
    
    /**
     * Service information endpoint
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, String>> getServiceInfo() {
        Map<String, String> info = Map.of(
            "service", "Fast Inward Clearing Processor",
            "version", "1.0.0",
            "description", "Processes incoming payment messages with idempotency and validation",
            "timestamp", LocalDateTime.now().toString()
        );
        
        return ResponseEntity.ok(info);
    }
}
