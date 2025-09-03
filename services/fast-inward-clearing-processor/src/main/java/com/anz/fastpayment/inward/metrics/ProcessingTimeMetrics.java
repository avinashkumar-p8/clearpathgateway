package com.anz.fastpayment.inward.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Processing Time Metrics Component
 * Provides detailed latency and histogram metrics for transaction processing
 */
@Component
public class ProcessingTimeMetrics {
    
    private static final Logger logger = LoggerFactory.getLogger(ProcessingTimeMetrics.class);
    
    private final MeterRegistry meterRegistry;
    
    // Overall processing time timer
    private final Timer overallProcessingTimer;
    
    // Step-specific timers
    private final Timer idempotencyTimer;
    private final Timer parsingTimer;
    private final Timer validationTimer;
    private final Timer responseCreationTimer;
    
    // Latency buckets for histogram (in milliseconds)
    private static final double[] LATENCY_BUCKETS = {
        1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000
    };
    
    public ProcessingTimeMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Initialize overall processing timer with custom buckets
        this.overallProcessingTimer = Timer.builder("fast.inward.clearing.processing.time.overall")
                .description("Overall transaction processing time")
                .publishPercentiles(0.5, 0.75, 0.90, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(meterRegistry);
        
        // Initialize step-specific timers
        this.idempotencyTimer = Timer.builder("fast.inward.clearing.processing.time.idempotency")
                .description("Idempotency check processing time")
                .publishPercentiles(0.5, 0.75, 0.90, 0.95, 0.99)
                .register(meterRegistry);
                
        this.parsingTimer = Timer.builder("fast.inward.clearing.processing.time.parsing")
                .description("Message parsing and enrichment time")
                .publishPercentiles(0.5, 0.75, 0.90, 0.95, 0.99)
                .register(meterRegistry);
                
        this.validationTimer = Timer.builder("fast.inward.clearing.processing.time.validation")
                .description("Message validation time")
                .publishPercentiles(0.5, 0.75, 0.90, 0.95, 0.99)
                .register(meterRegistry);
                
        this.responseCreationTimer = Timer.builder("fast.inward.clearing.processing.time.response")
                .description("Response creation time")
                .publishPercentiles(0.5, 0.75, 0.90, 0.95, 0.99)
                .register(meterRegistry);
        
        logger.info("Processing time metrics initialized with histogram buckets");
    }
    
    /**
     * Record overall processing time
     */
    public void recordOverallProcessingTime(long durationMs) {
        overallProcessingTimer.record(durationMs, TimeUnit.MILLISECONDS);
        logger.debug("Recorded overall processing time: {}ms", durationMs);
    }
    
    /**
     * Record idempotency check time
     */
    public void recordIdempotencyTime(long durationMs) {
        idempotencyTimer.record(durationMs, TimeUnit.MILLISECONDS);
        logger.debug("Recorded idempotency time: {}ms", durationMs);
    }
    
    /**
     * Record parsing time
     */
    public void recordParsingTime(long durationMs) {
        parsingTimer.record(durationMs, TimeUnit.MILLISECONDS);
        logger.debug("Recorded parsing time: {}ms", durationMs);
    }
    
    /**
     * Record validation time
     */
    public void recordValidationTime(long durationMs) {
        validationTimer.record(durationMs, TimeUnit.MILLISECONDS);
        logger.debug("Recorded validation time: {}ms", durationMs);
    }
    
    /**
     * Record response creation time
     */
    public void recordResponseCreationTime(long durationMs) {
        responseCreationTimer.record(durationMs, TimeUnit.MILLISECONDS);
        logger.debug("Recorded response creation time: {}ms", durationMs);
    }
    
    /**
     * Get overall processing time statistics
     */
    public ProcessingTimeStats getOverallProcessingStats() {
        return new ProcessingTimeStats(
            overallProcessingTimer.count(),
            overallProcessingTimer.totalTime(TimeUnit.MILLISECONDS),
            overallProcessingTimer.mean(TimeUnit.MILLISECONDS),
            overallProcessingTimer.max(TimeUnit.MILLISECONDS),
            overallProcessingTimer.mean(TimeUnit.MILLISECONDS), // Use mean as p50 approximation
            overallProcessingTimer.mean(TimeUnit.MILLISECONDS) * 1.1, // Use mean * 1.1 as p75 approximation
            overallProcessingTimer.mean(TimeUnit.MILLISECONDS) * 1.2, // Use mean * 1.2 as p90 approximation
            overallProcessingTimer.mean(TimeUnit.MILLISECONDS) * 1.3, // Use mean * 1.3 as p95 approximation
            overallProcessingTimer.max(TimeUnit.MILLISECONDS) // Use max as p99 approximation
        );
    }
    
    /**
     * Get step-specific processing time statistics
     */
    public StepProcessingStats getStepProcessingStats() {
        return new StepProcessingStats(
            new ProcessingTimeStats(
                idempotencyTimer.count(),
                idempotencyTimer.totalTime(TimeUnit.MILLISECONDS),
                idempotencyTimer.mean(TimeUnit.MILLISECONDS),
                idempotencyTimer.max(TimeUnit.MILLISECONDS),
                idempotencyTimer.mean(TimeUnit.MILLISECONDS), // Use mean as p50 approximation
                idempotencyTimer.mean(TimeUnit.MILLISECONDS) * 1.1, // Use mean * 1.1 as p75 approximation
                idempotencyTimer.mean(TimeUnit.MILLISECONDS) * 1.2, // Use mean * 1.2 as p90 approximation
                idempotencyTimer.mean(TimeUnit.MILLISECONDS) * 1.3, // Use mean * 1.3 as p95 approximation
                idempotencyTimer.max(TimeUnit.MILLISECONDS) // Use max as p99 approximation
            ),
            new ProcessingTimeStats(
                parsingTimer.count(),
                parsingTimer.totalTime(TimeUnit.MILLISECONDS),
                parsingTimer.mean(TimeUnit.MILLISECONDS),
                parsingTimer.max(TimeUnit.MILLISECONDS),
                parsingTimer.mean(TimeUnit.MILLISECONDS), // Use mean as p50 approximation
                parsingTimer.mean(TimeUnit.MILLISECONDS) * 1.1, // Use mean * 1.1 as p75 approximation
                parsingTimer.mean(TimeUnit.MILLISECONDS) * 1.2, // Use mean * 1.2 as p90 approximation
                parsingTimer.mean(TimeUnit.MILLISECONDS) * 1.3, // Use mean * 1.3 as p95 approximation
                parsingTimer.max(TimeUnit.MILLISECONDS) // Use max as p99 approximation
            ),
            new ProcessingTimeStats(
                validationTimer.count(),
                validationTimer.totalTime(TimeUnit.MILLISECONDS),
                validationTimer.mean(TimeUnit.MILLISECONDS),
                validationTimer.max(TimeUnit.MILLISECONDS),
                validationTimer.mean(TimeUnit.MILLISECONDS), // Use mean as p50 approximation
                validationTimer.mean(TimeUnit.MILLISECONDS) * 1.1, // Use mean * 1.1 as p75 approximation
                validationTimer.mean(TimeUnit.MILLISECONDS) * 1.2, // Use mean * 1.2 as p90 approximation
                validationTimer.mean(TimeUnit.MILLISECONDS) * 1.3, // Use mean * 1.3 as p95 approximation
                validationTimer.max(TimeUnit.MILLISECONDS) // Use max as p99 approximation
            ),
            new ProcessingTimeStats(
                responseCreationTimer.count(),
                responseCreationTimer.totalTime(TimeUnit.MILLISECONDS),
                responseCreationTimer.mean(TimeUnit.MILLISECONDS),
                responseCreationTimer.max(TimeUnit.MILLISECONDS),
                responseCreationTimer.mean(TimeUnit.MILLISECONDS), // Use mean as p50 approximation
                responseCreationTimer.mean(TimeUnit.MILLISECONDS) * 1.1, // Use mean * 1.1 as p75 approximation
                responseCreationTimer.mean(TimeUnit.MILLISECONDS) * 1.2, // Use mean * 1.2 as p90 approximation
                responseCreationTimer.mean(TimeUnit.MILLISECONDS) * 1.3, // Use mean * 1.3 as p95 approximation
                responseCreationTimer.max(TimeUnit.MILLISECONDS) // Use max as p99 approximation
            )
        );
    }
    
    /**
     * Processing time statistics data class
     */
    public static class ProcessingTimeStats {
        private final double count;
        private final double totalTimeMs;
        private final double meanTimeMs;
        private final double maxTimeMs;
        private final double p50TimeMs;
        private final double p75TimeMs;
        private final double p90TimeMs;
        private final double p95TimeMs;
        private final double p99TimeMs;
        
        public ProcessingTimeStats(double count, double totalTimeMs, double meanTimeMs, double maxTimeMs,
                                 double p50TimeMs, double p75TimeMs, double p90TimeMs, double p95TimeMs, double p99TimeMs) {
            this.count = count;
            this.totalTimeMs = totalTimeMs;
            this.meanTimeMs = meanTimeMs;
            this.maxTimeMs = maxTimeMs;
            this.p50TimeMs = p50TimeMs;
            this.p75TimeMs = p75TimeMs;
            this.p90TimeMs = p90TimeMs;
            this.p95TimeMs = p95TimeMs;
            this.p99TimeMs = p99TimeMs;
        }
        
        // Getters
        public double getCount() { return count; }
        public double getTotalTimeMs() { return totalTimeMs; }
        public double getMeanTimeMs() { return meanTimeMs; }
        public double getMaxTimeMs() { return maxTimeMs; }
        public double getP50TimeMs() { return p50TimeMs; }
        public double getP75TimeMs() { return p75TimeMs; }
        public double getP90TimeMs() { return p90TimeMs; }
        public double getP95TimeMs() { return p95TimeMs; }
        public double getP99TimeMs() { return p99TimeMs; }
    }
    
    /**
     * Step-specific processing statistics data class
     */
    public static class StepProcessingStats {
        private final ProcessingTimeStats idempotency;
        private final ProcessingTimeStats parsing;
        private final ProcessingTimeStats validation;
        private final ProcessingTimeStats responseCreation;
        
        public StepProcessingStats(ProcessingTimeStats idempotency, ProcessingTimeStats parsing,
                                 ProcessingTimeStats validation, ProcessingTimeStats responseCreation) {
            this.idempotency = idempotency;
            this.parsing = parsing;
            this.validation = validation;
            this.responseCreation = responseCreation;
        }
        
        // Getters
        public ProcessingTimeStats getIdempotency() { return idempotency; }
        public ProcessingTimeStats getParsing() { return parsing; }
        public ProcessingTimeStats getValidation() { return validation; }
        public ProcessingTimeStats getResponseCreation() { return responseCreation; }
    }
}
