// Five requests per refresh leave room for user operations under 120 requests/minute.
export const LIVE_REFRESH_INTERVAL_MS = 5000;

export function createRefreshScheduler(intervalMs = LIVE_REFRESH_INTERVAL_MS, now = Date.now) {
  let inFlight = null;
  let nextAllowed = 0;
  return function refresh(operation) {
    if (inFlight) return inFlight;
    if (now() < nextAllowed) return Promise.resolve();
    nextAllowed = now() + intervalMs;
    inFlight = Promise.resolve().then(operation).finally(() => { inFlight = null; });
    return inFlight;
  };
}
