import { test, expect } from '@playwright/test';
import { httpHelper } from '../helpers/http-helper';

test.describe('Health Controller Tests', () => {
  test.beforeEach(async () => {
    // Ensure service is running
    await expect.poll(async () => {
      const response = await httpHelper.healthCheck();
      return response.status;
    }, { timeout: 30000 }).toBe(200);
  });

  test('should return health status successfully', async () => {
    const response = await httpHelper.healthCheck();
    
    expect(response.status).toBe(200);
    expect(response.data).toHaveProperty('status', 'HEALTHY');
    expect(response.data).toHaveProperty('service', 'Fast Inward Clearing Processor');
    expect(response.data).toHaveProperty('timestamp');
  });

  test('should return service information', async () => {
    const response = await httpHelper.getServiceInfo();
    
    expect(response.status).toBe(200);
    expect(response.data).toHaveProperty('service', 'Fast Inward Clearing Processor');
    expect(response.data).toHaveProperty('version', '1.0.0');
    expect(response.data).toHaveProperty('description');
    expect(response.data).toHaveProperty('timestamp');
  });

  test('should handle health check with proper headers', async () => {
    const response = await httpHelper.get('/api/v1/health/status', {
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'Playwright-Test'
      }
    });
    
    expect(response.status).toBe(200);
    expect(response.headers['content-type']).toContain('application/json');
  });
});
