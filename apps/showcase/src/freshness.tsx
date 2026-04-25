import { useEffect, useState } from "react";

/**
 * "Updated X ago" helper — shared across every auto-refreshing
 * panel. Returns both the raw timestamp-setter and a pre-formatted
 * string that updates every second so a stalled poll is visible.
 *
 * Usage:
 *   const { bump, pill } = useFreshness();
 *   await fetchStuff();
 *   bump();
 *   return <Panel action={<FreshnessPill {...pill} />}>...</Panel>;
 *
 * The separate `pill` struct lets us render the chip with consistent
 * visuals on every panel without coupling that visual to each tab's
 * own DOM.
 */
export interface FreshnessState {
  lastAt: number | null;
  nowMs: number;
  stale: boolean;
}

export function useFreshness(staleAfterMs = 5000): {
  bump: () => void;
  pill: FreshnessState;
} {
  const [lastAt, setLastAt] = useState<number | null>(null);
  const [nowMs, setNowMs] = useState(() => Date.now());

  // Tick every 1s so "Xs ago" keeps counting even if the fetch
  // hasn't returned yet. Cheap — just a setState per panel.
  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  const stale = lastAt !== null && nowMs - lastAt > staleAfterMs;
  return {
    bump: () => setLastAt(Date.now()),
    pill: { lastAt, nowMs, stale },
  };
}

export function FreshnessPill({ lastAt, nowMs, stale }: FreshnessState) {
  if (lastAt === null) {
    return (
      <span className="text-xs text-gray-400" title="Waiting for first update">
        —
      </span>
    );
  }
  const ageSec = Math.max(0, Math.floor((nowMs - lastAt) / 1000));
  const label = ageSec === 0 ? "just now" : `${ageSec}s ago`;
  return (
    <span
      className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs ${
        stale
          ? "bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-300"
          : "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300"
      }`}
      title={stale ? "Data may be stale" : "Fresh"}
    >
      <span
        className={`h-1.5 w-1.5 rounded-full ${
          stale ? "bg-orange-500" : "bg-green-500 animate-pulse"
        }`}
      />
      {label}
    </span>
  );
}
