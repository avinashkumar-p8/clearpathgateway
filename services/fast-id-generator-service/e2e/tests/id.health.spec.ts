import { test, expect, request } from '@playwright/test';

test.describe('Fast ID Generator Service', () => {
  const base = process.env.ID_BASE_URL || 'http://localhost:8091';

  test('health', async () => {
    const resp = await fetch(`${base}/health`);
    expect(resp.ok).toBeTruthy();
  });

  test('puid and muid endpoints work', async () => {
    // PUID
    const puidResp = await fetch(`${base}/api/ids/puid?channel=G3I`);
    expect(puidResp.ok).toBeTruthy();
    const puidJson = await puidResp.json();
    expect(puidJson.puid).toBeTruthy();
    expect(puidJson.puid).toHaveLength(16);
    expect(puidJson.puid.startsWith('G3I')).toBeTruthy();

    // MUID (twice) to test cache idempotency
    const muidResp1 = await fetch(`${base}/api/ids/muid?puid=${encodeURIComponent(puidJson.puid)}`);
    expect(muidResp1.ok).toBeTruthy();
    const muidJson1 = await muidResp1.json();

    const muidResp2 = await fetch(`${base}/api/ids/muid?puid=${encodeURIComponent(puidJson.puid)}`);
    expect(muidResp2.ok).toBeTruthy();
    const muidJson2 = await muidResp2.json();

    expect(muidJson1.muid).toBeTruthy();
    expect(muidJson2.muid).toBeTruthy();
    expect(muidJson1.muid).toBe(muidJson2.muid);
  });

  test('puid block returns requested count and unique ids', async () => {
    const resp = await fetch(`${base}/api/ids/puid-block?channel=G3I&size=25`);
    expect(resp.ok).toBeTruthy();
    const json = await resp.json();
    expect(json.count).toBe(25);
    expect(Array.isArray(json.puids)).toBeTruthy();
    const set = new Set(json.puids);
    expect(set.size).toBe(json.puids.length);
  });
});


