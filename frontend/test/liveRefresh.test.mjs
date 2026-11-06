import test from 'node:test';
import assert from 'node:assert/strict';
import { createRefreshScheduler, LIVE_REFRESH_INTERVAL_MS } from '../src/utils/liveRefresh.js';

test('live refresh shares in-flight work and coalesces SSE bursts within the polling budget', async () => {
  let time = 0;
  let calls = 0;
  let complete;
  const refresh = createRefreshScheduler(LIVE_REFRESH_INTERVAL_MS, () => time);
  const first = refresh(() => { calls++; return new Promise(resolve => { complete = resolve; }); });
  await Promise.resolve();
  assert.equal(refresh(async () => { calls++; }), first);
  complete();
  await first;
  for (let i = 0; i < 100; i++) await refresh(async () => { calls++; });
  assert.equal(calls, 1);
  time = LIVE_REFRESH_INTERVAL_MS;
  await refresh(async () => { calls++; });
  assert.equal(calls, 2);
  assert.ok(5 * 60000 / LIVE_REFRESH_INTERVAL_MS <= 60);
});

test('failed refresh releases the in-flight gate for the next poll', async () => {
  let time = 0;
  const refresh = createRefreshScheduler(5000, () => time);
  await assert.rejects(refresh(async () => { throw new Error('offline'); }), /offline/);
  time = 5000;
  let resumed = false;
  await refresh(async () => { resumed = true; });
  assert.equal(resumed, true);
});
