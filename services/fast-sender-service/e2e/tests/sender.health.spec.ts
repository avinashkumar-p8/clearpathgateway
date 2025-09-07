import { test, expect } from '@playwright/test';

test('sender health', async () => {
  const resp = await fetch('http://localhost:8081/health');
  expect(resp.ok).toBeTruthy();
});


