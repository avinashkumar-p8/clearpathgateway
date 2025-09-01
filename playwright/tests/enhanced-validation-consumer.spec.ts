import { test, expect } from '@playwright/test';

/**
 * Enhanced Validation Consumer Tests
 * Tests the enhanced scheme validation consumer with Avro schema and parallel validation
 */
test.describe('Enhanced Validation Consumer', () => {
  const baseUrl = 'http://localhost:8080';
  
  test.describe('Health Endpoints', () => {
    test('should return healthy status', async ({ request }) => {
      const response = await request.get(`${baseUrl}/api/v1/health/enhanced-validation/status`);
      expect(response.status()).toBe(200);
      
      const data = await response.json();
      expect(data.status).toBe('HEALTHY');
      expect(data.service).toBe('Enhanced Scheme Validation Consumer');
      expect(data.timestamp).toBeDefined();
    });
    
    test('should return consumer metrics', async ({ request }) => {
      const response = await request.get(`${baseUrl}/api/v1/health/enhanced-validation/metrics`);
      expect(response.status()).toBe(200);
      
      const data = await response.json();
      expect(data).toHaveProperty('messagesReceived');
      expect(data).toHaveProperty('messagesProcessed');
      expect(data).toHaveProperty('validationErrors');
      expect(data).toHaveProperty('duplicateMuidCount');
    });
    
    test('should return validation statistics', async ({ request }) => {
      const response = await request.get(`${baseUrl}/api/v1/health/enhanced-validation/validation-stats`);
      expect(response.status()).toBe(200);
      
      const data = await response.json();
      expect(data).toHaveProperty('totalValidations');
      expect(data).toHaveProperty('successfulValidations');
      expect(data).toHaveProperty('failedValidations');
      expect(data).toHaveProperty('cacheHitRate');
    });
  });
  
  test.describe('Enhanced Validation Flow', () => {
    test('should process new MUID with Avro schema and parallel validation', async ({ request }) => {
      // Test data with Avro schema format
      const testEvent = {
        muid: `test-muid-${Date.now()}`,
        payload: {
          currency: 'USD',
          country: 'US',
          mmbid: '12345678901',
          amount: '100.00',
          timestamp: new Date().toISOString()
        },
        schema: 'com.anz.fastpayment.UnifiedPaymentMessage'
      };
      
      // Send test event to Kafka topic (simulated via REST endpoint)
      const response = await request.post(`${baseUrl}/api/v1/test/enhanced-validation/send-event`, {
        data: testEvent
      });
      
      expect(response.status()).toBe(200);
      
      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      // Verify metrics show processing
      const metricsResponse = await request.get(`${baseUrl}/api/v1/health/enhanced-validation/metrics`);
      const metrics = await metricsResponse.json();
      
      expect(metrics.messagesReceived).toBeGreaterThan(0);
      expect(metrics.messagesProcessed).toBeGreaterThan(0);
    });
    
    test('should handle duplicate MUID correctly', async ({ request }) => {
      const duplicateMuid = `duplicate-muid-${Date.now()}`;
      
      // Send first event
      const firstEvent = {
        muid: duplicateMuid,
        payload: {
          currency: 'EUR',
          country: 'DE',
          mmbid: '98765432109',
          amount: '200.00'
        }
      };
      
      await request.post(`${baseUrl}/api/v1/test/enhanced-validation/send-event`, {
        data: firstEvent
      });
      
      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      // Send duplicate event
      const duplicateEvent = {
        muid: duplicateMuid,
        payload: {
          currency: 'EUR',
          country: 'DE',
          mmbid: '98765432109',
          amount: '300.00'
        }
      };
      
      await request.post(`${baseUrl}/api/v1/test/enhanced-validation/send-event`, {
        data: duplicateEvent
      });
      
      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      // Verify duplicate handling
      const metricsResponse = await request.get(`${baseUrl}/api/v1/health/enhanced-validation/metrics`);
      const metrics = await metricsResponse.json();
      
      expect(metrics.duplicateMuidCount).toBeGreaterThan(0);
    });
    
    test('should validate all required tags in parallel', async ({ request }) => {
      const validationEvent = {
        muid: `validation-test-${Date.now()}`,
        payload: {
          currency: 'GBP',
          country: 'GB',
          mmbid: '11223344556',
          amount: '500.00'
        }
      };
      
      // Send validation test event
      await request.post(`${baseUrl}/api/v1/test/enhanced-validation/send-event`, {
        data: validationEvent
      });
      
      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      // Verify validation statistics
      const statsResponse = await request.get(`${baseUrl}/api/v1/health/enhanced-validation/validation-stats`);
      const stats = await statsResponse.json();
      
      expect(stats.totalValidations).toBeGreaterThan(0);
      expect(stats.successfulValidations).toBeGreaterThan(0);
      expect(stats.cacheHitRate).toBeGreaterThan(0);
    });
  });
  
  test.describe('Error Handling', () => {
    test('should handle invalid Avro schema gracefully', async ({ request }) => {
      const invalidEvent = {
        muid: `invalid-schema-${Date.now()}`,
        payload: 'invalid-payload-format',
        schema: 'invalid.schema.Reference'
      };
      
      const response = await request.post(`${baseUrl}/api/v1/test/enhanced-validation/send-event`, {
        data: invalidEvent
      });
      
      // Should handle gracefully even with invalid schema
      expect(response.status()).toBe(200);
      
      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      // Verify error metrics
      const metricsResponse = await request.get(`${baseUrl}/api/v1/health/enhanced-validation/metrics`);
      const metrics = await metricsResponse.json();
      
      expect(metrics.validationErrors).toBeGreaterThan(0);
    });
    
    test('should handle missing required fields', async ({ request }) => {
      const incompleteEvent = {
        muid: `incomplete-${Date.now()}`,
        payload: {
          currency: 'USD'
          // Missing country and mmbid
        }
      };
      
      await request.post(`${baseUrl}/api/v1/test/enhanced-validation/send-event`, {
        data: incompleteEvent
      });
      
      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      // Verify validation statistics show failures
      const statsResponse = await request.get(`${baseUrl}/api/v1/health/enhanced-validation/validation-stats`);
      const stats = await statsResponse.json();
      
      expect(stats.failedValidations).toBeGreaterThan(0);
    });
  });
  
  test.describe('Cache Performance', () => {
    test('should demonstrate cache hit rate improvement', async ({ request }) => {
      const baseMuid = `cache-test-${Date.now()}`;
      
      // Send multiple events with same validation data
      for (let i = 0; i < 5; i++) {
        const event = {
          muid: `${baseMuid}-${i}`,
          payload: {
            currency: 'JPY',
            country: 'JP',
            mmbid: '99887766554',
            amount: '1000.00'
          }
        };
        
        await request.post(`${baseUrl}/api/v1/test/enhanced-validation/send-event`, {
          data: event
        });
        
        // Small delay between requests
        await new Promise(resolve => setTimeout(resolve, 500));
      }
      
      // Wait for all processing
      await new Promise(resolve => setTimeout(resolve, 3000));
      
      // Verify cache performance
      const statsResponse = await request.get(`${baseUrl}/api/v1/health/enhanced-validation/validation-stats`);
      const stats = await statsResponse.json();
      
      expect(stats.cacheHitRate).toBeGreaterThan(0.5); // Should have good cache hit rate
      expect(stats.totalValidations).toBeGreaterThanOrEqual(5);
    });
  });
});
