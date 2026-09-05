/**
 * Swap rebalance / zero-downtime node replacement (section 23).
 *
 * Entirely simulated — NebulaDB exposes no rebalance controller, and
 * the page says so up front. The value here is showing the *phases* a
 * swap rebalance moves through (snapshot, CDC attach, catch-up,
 * verification, cutover) and the signals an operator watches at each:
 * bytes moved, CDC lag, throughput, shards migrated.
 */
import { useEffect, useState } from "react";
import {
  initialRebalance,
  startRebalance,
  tickRebalance,
  REBALANCE_PHASES,
  type RebalanceState,
} from "../demo/simulation";
import { Panel } from "../components";
import { SimulationNotice } from "../components/Provenance";

const TICK_MS = 500;

const PHASE_DETAIL: Record<string, string> = {
  snapshot: "Stream a consistent base snapshot to the replacement node without blocking writes.",
  cdc: "Attach a change-data-capture stream from the snapshot LSN so new writes follow.",
  catchup: "Drain the backlog until the replica is within one WAL segment of the primary.",
  verify: "Compare checksums across every moved shard before trusting the new node.",
  cutover: "Flip the routing table, drain the old node, remove it from the cluster.",
  done: "Complete — no dropped requests during the swap.",
};

export function RebalanceTab() {
  const [state, setState] = useState<RebalanceState>(() => initialRebalance());
  const [tick, setTick] = useState(0);

  useEffect(() => {
    const id = setInterval(() => {
      setTick((t) => t + 1);
      setState((s) => tickRebalance(s, TICK_MS, tick));
    }, TICK_MS);
    return () => clearInterval(id);
  }, [tick]);

  const running = state.phase !== "idle" && state.phase !== "done";
  const phaseIdx = REBALANCE_PHASES.indexOf(state.phase);
  const overall =
    state.phase === "done"
      ? 1
      : state.phase === "idle"
        ? 0
        : (phaseIdx - 1 + state.progress) / (REBALANCE_PHASES.length - 2);

  return (
    <div className="space-y-5">
      <SimulationNotice
        what="Swap rebalance is fully simulated"
        why="NebulaDB does not expose a rebalance controller endpoint. This page runs a deterministic state machine so the phases and the operator signals can be demonstrated. No data moves and no cluster is touched."
      />

      <Panel
        title="Zero-downtime node replacement"
        subtitle="Node v1 → snapshot → CDC → catch-up → verify → cutover → Node v2"
        action={
          <div className="flex gap-1.5">
            {!running && (
              <button className="btn !text-xs" onClick={() => setState((s) => startRebalance(s))}>
                {state.phase === "done" ? "Run again" : "Start rebalance"}
              </button>
            )}
            {running && (
              <>
                <button
                  className="btn-secondary !text-xs"
                  onClick={() => setState((s) => ({ ...s, paused: !s.paused }))}
                >
                  {state.paused ? "Resume" : "Pause"}
                </button>
                <button
                  className="btn-secondary !text-xs"
                  onClick={() => setState(initialRebalance())}
                  title="Abort and restore the original placement"
                >
                  Rollback
                </button>
              </>
            )}
          </div>
        }
      >
        <div className="space-y-4">
          {/* Phase rail */}
          <div className="flex flex-wrap gap-1.5">
            {REBALANCE_PHASES.filter((p) => p !== "idle").map((p) => {
              const done = REBALANCE_PHASES.indexOf(p) < phaseIdx;
              const now = p === state.phase;
              return (
                <span
                  key={p}
                  className={`rounded border px-2 py-0.5 font-mono text-[10px] transition-colors
                    ${
                      now
                        ? "border-accent/50 bg-accent/15 text-accent"
                        : done
                          ? "border-ok/40 bg-ok/10 text-ok"
                          : "border-gray-200 text-gray-400 dark:border-edge dark:text-faint"
                    }`}
                >
                  {p}
                </span>
              );
            })}
            {state.paused && (
              <span className="rounded border border-warn/50 bg-warn/15 px-2 py-0.5 font-mono text-[10px] text-warn">
                paused
              </span>
            )}
          </div>

          {/* Overall progress */}
          <div>
            <div className="mb-1 flex items-center justify-between font-mono text-[10px] text-gray-500 dark:text-muted">
              <span>{PHASE_DETAIL[state.phase] ?? "Idle — no migration in progress."}</span>
              <span className="tabular-nums">{Math.round(overall * 100)}%</span>
            </div>
            <div className="h-1.5 overflow-hidden rounded-full bg-gray-200 dark:bg-carbon-800">
              <div
                className="h-full bg-accent transition-[width] duration-300"
                style={{ width: `${Math.max(1, overall * 100)}%` }}
              />
            </div>
          </div>

          {/* Signals */}
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
            <Metric label="Bytes moved" value={fmtBytes(state.bytesMoved)} sub={`of ${fmtBytes(state.totalBytes)}`} />
            <Metric label="Throughput" value={`${state.throughputMbSec.toFixed(0)} MB/s`} />
            <Metric
              label="CDC lag"
              value={state.cdcLagMs ? `${state.cdcLagMs} ms` : "—"}
              tone={state.cdcLagMs > 400 ? "warn" : "ok"}
            />
            <Metric label="Shards moved" value={`${state.shardsMoved} / ${state.totalShards}`} />
          </div>

          {state.log.length > 0 && (
            <ol className="space-y-1">
              {state.log.map((l, i) => (
                <li key={i} className="flex gap-3 font-mono text-[11px]">
                  <span className="w-16 shrink-0 text-gray-400 dark:text-faint">{l.phase}</span>
                  <span className="text-gray-700 dark:text-muted">{l.message}</span>
                </li>
              ))}
            </ol>
          )}
        </div>
      </Panel>
    </div>
  );
}

function Metric({
  label,
  value,
  sub,
  tone,
}: {
  label: string;
  value: string;
  sub?: string;
  tone?: "ok" | "warn";
}) {
  return (
    <div className="rounded-md border border-gray-200 bg-white p-2.5 dark:border-edge dark:bg-carbon-900">
      <div className="eyebrow">{label}</div>
      <div
        className={`mt-0.5 font-mono text-sm font-semibold tabular-nums
          ${tone === "warn" ? "text-warn" : "text-gray-900 dark:text-ink"}`}
      >
        {value}
      </div>
      {sub && <div className="text-[10px] text-gray-400 dark:text-faint">{sub}</div>}
    </div>
  );
}

function fmtBytes(n: number): string {
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(0)} KB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(0)} MB`;
  return `${(n / 1024 ** 3).toFixed(1)} GB`;
}
