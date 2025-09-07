import { test, expect, request } from '@playwright/test';

test('router health', async () => {
  const healthUrl = 'http://localhost:8080/health';
  const resp = await fetch(healthUrl);
  expect(resp.ok).toBeTruthy();
  const body = await resp.json();
  expect(body.status).toBe('UP');
});


