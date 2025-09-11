import { test, expect } from '@playwright/test';

test('puid block 100 returns unique IDs', async () => {
  const base = process.env.ID_BASE_URL || 'http://localhost:8091';
  const size = 100;
  const resp = await fetch(`${base}/api/ids/puid-block?channel=G3I&size=${size}`);
  expect(resp.ok).toBeTruthy();
  const json = await resp.json();
  expect(json.count).toBe(size);
  expect(Array.isArray(json.puids)).toBeTruthy();
  expect(json.puids.length).toBe(size);
  const set = new Set(json.puids);
  expect(set.size).toBe(size);
});


