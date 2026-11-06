export const LIVE_REFRESH_INTERVAL_MS: number;
export function createRefreshScheduler(intervalMs?: number, now?: () => number): (operation: () => Promise<void>) => Promise<void>;
