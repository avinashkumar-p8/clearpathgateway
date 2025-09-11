import { test, expect } from '@playwright/test';

async function waitForHealth(url: string, timeoutMs = 30000) {
  const start = Date.now();
  for (;;) {
    try {
      const resp = await fetch(url);
      if (resp.ok) return true;
    } catch {}
    if (Date.now() - start > timeoutMs) return false;
    await new Promise(r => setTimeout(r, 500));
  }
}

test('router health', async () => {
  const healthUrl = 'http://localhost:8080/health';
  const ok = await waitForHealth(healthUrl, 30000);
  expect(ok).toBeTruthy();
  const resp = await fetch(healthUrl);
  const body = await resp.json();
  expect(body.status).toBe('UP');
});


