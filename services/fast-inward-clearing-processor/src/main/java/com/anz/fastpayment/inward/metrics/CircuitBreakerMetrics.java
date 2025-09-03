package com.anz.fastpayment.inward.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Circuit Breaker Metrics Component
 * Provides circuit breaker state monitoring and metrics
 */
@Component
public class CircuitBreakerMetrics {
    
    private static final Logger logger = LoggerFactory.getLogger(CircuitBreakerMetrics.class);
    
    private final MeterRegistry meterRegistry;
    
    // Circuit breaker states (0=CLOSED, 1=OPEN, 2=HALF_OPEN)
    private final AtomicInteger circuitBreakerState = new AtomicInteger(0);
    
    // Circuit breaker counters
    private final Counter circuitBreakerSuccessCount;
    private final Counter circuitBreakerFailureCount;
    private final Counter circuitBreakerTimeoutCount;
    private final Counter circuitBreakerRejectedCount;
    private final Counter circuitBreakerStateChangeCount;
    
    // Circuit breaker timing
    private final AtomicLong lastFailureTime = new AtomicLong(0);
    private final AtomicLong lastSuccessTime = new AtomicLong(0);
    private final AtomicLong circuitBreakerOpenTime = new AtomicLong(0);
    
    // Circuit breaker configuration
    private final AtomicInteger failureThreshold = new AtomicInteger(5);
    private final AtomicInteger successThreshold = new AtomicInteger(3);
    private final AtomicLong timeoutMs = new AtomicLong(60000); // 60 seconds
    
    public CircuitBreakerMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Initialize counters
        this.circuitBreakerSuccessCount = Counter.builder("fast.inward.clearing.circuit.breaker.success")
                .description("Number of successful operations through circuit breaker")
                .register(meterRegistry);
                
        this.circuitBreakerFailureCount = Counter.builder("fast.inward.clearing.circuit.breaker.failure")
                .description("Number of failed operations through circuit breaker")
                .register(meterRegistry);
                
        this.circuitBreakerTimeoutCount = Counter.builder("fast.inward.clearing.circuit.breaker.timeout")
                .description("Number of timeout operations through circuit breaker")
                .register(meterRegistry);
                
        this.circuitBreakerRejectedCount = Counter.builder("fast.inward.clearing.circuit.breaker.rejected")
                .description("Number of rejected operations by circuit breaker")
                .register(meterRegistry);
                
        this.circuitBreakerStateChangeCount = Counter.builder("fast.inward.clearing.circuit.breaker.state.change")
                .description("Number of circuit breaker state changes")
                .register(meterRegistry);
        
        // Initialize gauges for circuit breaker state
        Gauge.builder("fast.inward.clearing.circuit.breaker.state", this, cb -> (double) cb.getCircuitBreakerState())
                .description("Circuit breaker state (0=CLOSED, 1=OPEN, 2=HALF_OPEN)")
                .register(meterRegistry);
                
        Gauge.builder("fast.inward.clearing.circuit.breaker.failure.threshold", this, cb -> (double) cb.failureThreshold.get())
                .description("Circuit breaker failure threshold")
                .register(meterRegistry);
                
        Gauge.builder("fast.inward.clearing.circuit.breaker.success.threshold", this, cb -> (double) cb.successThreshold.get())
                .description("Circuit breaker success threshold")
                .register(meterRegistry);
                
        Gauge.builder("fast.inward.clearing.circuit.breaker.timeout.ms", this, cb -> (double) cb.timeoutMs.get())
                .description("Circuit breaker timeout in milliseconds")
                .register(meterRegistry);
        
        logger.info("Circuit breaker metrics initialized");
    }
    
    /**
     * Record a successful operation
     */
    public void recordSuccess() {
        circuitBreakerSuccessCount.increment();
        lastSuccessTime.set(System.currentTimeMillis());
        
        // Check if we should transition from HALF_OPEN to CLOSED
        if (circuitBreakerState.get() == 2) { // HALF_OPEN
            // In a real implementation, you would track consecutive successes
            // For now, we'll transition to CLOSED after any success in HALF_OPEN
            transitionToClosed();
        }
        
        logger.debug("Recorded circuit breaker success, state: {}", getStateName());
    }
    
    /**
     * Record a failed operation
     */
    public void recordFailure() {
        circuitBreakerFailureCount.increment();
        lastFailureTime.set(System.currentTimeMillis());
        
        // Check if we should transition to OPEN
        if (circuitBreakerState.get() == 0) { // CLOSED
            // In a real implementation, you would track consecutive failures
            // For now, we'll transition to OPEN after any failure in CLOSED
            transitionToOpen();
        } else if (circuitBreakerState.get() == 2) { // HALF_OPEN
            // Any failure in HALF_OPEN should go back to OPEN
            transitionToOpen();
        }
        
        logger.debug("Recorded circuit breaker failure, state: {}", getStateName());
    }
    
    /**
     * Record a timeout operation
     */
    public void recordTimeout() {
        circuitBreakerTimeoutCount.increment();
        recordFailure(); // Timeout is treated as failure
        logger.debug("Recorded circuit breaker timeout, state: {}", getStateName());
    }
    
    /**
     * Record a rejected operation (circuit breaker is OPEN)
     */
    public void recordRejected() {
        circuitBreakerRejectedCount.increment();
        logger.debug("Recorded circuit breaker rejection, state: {}", getStateName());
    }
    
    /**
     * Check if operation should be allowed
     */
    public boolean isOperationAllowed() {
        int state = circuitBreakerState.get();
        
        if (state == 0) { // CLOSED
            return true;
        } else if (state == 1) { // OPEN
            // Check if timeout has passed
            long timeSinceOpen = System.currentTimeMillis() - circuitBreakerOpenTime.get();
            if (timeSinceOpen >= timeoutMs.get()) {
                transitionToHalfOpen();
                return true;
            }
            return false;
        } else { // HALF_OPEN
            return true;
        }
    }
    
    /**
     * Transition to CLOSED state
     */
    private void transitionToClosed() {
        circuitBreakerState.set(0);
        circuitBreakerStateChangeCount.increment();
        logger.info("Circuit breaker transitioned to CLOSED state");
    }
    
    /**
     * Transition to OPEN state
     */
    private void transitionToOpen() {
        circuitBreakerState.set(1);
        circuitBreakerOpenTime.set(System.currentTimeMillis());
        circuitBreakerStateChangeCount.increment();
        logger.warn("Circuit breaker transitioned to OPEN state");
    }
    
    /**
     * Transition to HALF_OPEN state
     */
    private void transitionToHalfOpen() {
        circuitBreakerState.set(2);
        circuitBreakerStateChangeCount.increment();
        logger.info("Circuit breaker transitioned to HALF_OPEN state");
    }
    
    /**
     * Get current circuit breaker state
     */
    public int getCircuitBreakerState() {
        return circuitBreakerState.get();
    }
    
    /**
     * Get current circuit breaker state name
     */
    public String getStateName() {
        int state = circuitBreakerState.get();
        switch (state) {
            case 0: return "CLOSED";
            case 1: return "OPEN";
            case 2: return "HALF_OPEN";
            default: return "UNKNOWN";
        }
    }
    
    /**
     * Get circuit breaker statistics
     */
    public CircuitBreakerStats getCircuitBreakerStats() {
        return new CircuitBreakerStats(
            getStateName(),
            circuitBreakerSuccessCount.count(),
            circuitBreakerFailureCount.count(),
            circuitBreakerTimeoutCount.count(),
            circuitBreakerRejectedCount.count(),
            circuitBreakerStateChangeCount.count(),
            lastSuccessTime.get(),
            lastFailureTime.get(),
            circuitBreakerOpenTime.get(),
            failureThreshold.get(),
            successThreshold.get(),
            timeoutMs.get()
        );
    }
    
    /**
     * Update circuit breaker configuration
     */
    public void updateConfiguration(int failureThreshold, int successThreshold, long timeoutMs) {
        this.failureThreshold.set(failureThreshold);
        this.successThreshold.set(successThreshold);
        this.timeoutMs.set(timeoutMs);
        logger.info("Circuit breaker configuration updated: failureThreshold={}, successThreshold={}, timeoutMs={}",
                   failureThreshold, successThreshold, timeoutMs);
    }
    
    /**
     * Reset circuit breaker to CLOSED state
     */
    public void reset() {
        transitionToClosed();
        logger.info("Circuit breaker reset to CLOSED state");
    }
    
    /**
     * Circuit breaker statistics data class
     */
    public static class CircuitBreakerStats {
        private final String state;
        private final double successCount;
        private final double failureCount;
        private final double timeoutCount;
        private final double rejectedCount;
        private final double stateChangeCount;
        private final long lastSuccessTime;
        private final long lastFailureTime;
        private final long circuitBreakerOpenTime;
        private final int failureThreshold;
        private final int successThreshold;
        private final long timeoutMs;
        
        public CircuitBreakerStats(String state, double successCount, double failureCount, double timeoutCount,
                                 double rejectedCount, double stateChangeCount, long lastSuccessTime, long lastFailureTime,
                                 long circuitBreakerOpenTime, int failureThreshold, int successThreshold, long timeoutMs) {
            this.state = state;
            this.successCount = successCount;
            this.failureCount = failureCount;
            this.timeoutCount = timeoutCount;
            this.rejectedCount = rejectedCount;
            this.stateChangeCount = stateChangeCount;
            this.lastSuccessTime = lastSuccessTime;
            this.lastFailureTime = lastFailureTime;
            this.circuitBreakerOpenTime = circuitBreakerOpenTime;
            this.failureThreshold = failureThreshold;
            this.successThreshold = successThreshold;
            this.timeoutMs = timeoutMs;
        }
        
        // Getters
        public String getState() { return state; }
        public double getSuccessCount() { return successCount; }
        public double getFailureCount() { return failureCount; }
        public double getTimeoutCount() { return timeoutCount; }
        public double getRejectedCount() { return rejectedCount; }
        public double getStateChangeCount() { return stateChangeCount; }
        public long getLastSuccessTime() { return lastSuccessTime; }
        public long getLastFailureTime() { return lastFailureTime; }
        public long getCircuitBreakerOpenTime() { return circuitBreakerOpenTime; }
        public int getFailureThreshold() { return failureThreshold; }
        public int getSuccessThreshold() { return successThreshold; }
        public long getTimeoutMs() { return timeoutMs; }
    }
}
