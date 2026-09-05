/**
 * Provenance badges.
 *
 * A demo that blends live and simulated numbers has to say which is
 * which, everywhere, without the reader hunting for a footnote. These
 * badges sit inline next to the value they describe.
 */
import type { Origin, Sourced } from "../demo/provenance";

const STYLE: Record<Origin, { label: string; cls: string; title: string }> = {
  live: {
    label: "LIVE",
    cls: "border-ok/40 bg-ok/10 text-ok",
    title: "Read from this NebulaDB cluster's REST API",
  },
  derived: {
    label: "DERIVED",
    cls: "border-accent/40 bg-accent/10 text-accent",
    title: "Computed from live values returned by the API",
  },
  simulated: {
    label: "SIMULATED",
    cls: "border-warn/40 bg-warn/10 text-warn",
    title: "Produced by the deterministic demo simulator — not a real measurement",
  },
};

export function OriginBadge({ origin, from }: { origin: Origin; from?: string }) {
  const s = STYLE[origin];
  return (
    <span
      title={from ? `${s.title} — ${from}` : s.title}
      className={`inline-flex items-center rounded border px-1.5 py-px font-mono text-[9px] font-semibold tracking-wider ${s.cls}`}
    >
      {s.label}
    </span>
  );
}

/** A stat tile that can never render a number without its origin. */
export function SourcedStat({
  label,
  sourced,
  format,
  hint,
}: {
  label: string;
  sourced: Sourced<number | string>;
  format?: (v: number | string) => string;
  hint?: string;
}) {
  const shown = format ? format(sourced.value) : String(sourced.value);
  return (
    <div className="card !p-3.5 space-y-1.5" title={hint}>
      <div className="flex items-center justify-between gap-2">
        <span className="eyebrow truncate">{label}</span>
        <OriginBadge origin={sourced.origin} from={sourced.from} />
      </div>
      <div className="font-mono text-xl font-semibold tabular-nums text-gray-900 dark:text-ink">
        {shown}
      </div>
      <div className="text-[10px] text-gray-400 dark:text-faint truncate">{sourced.from}</div>
    </div>
  );
}

/**
 * Banner for a whole screen whose content is simulated. Used on the
 * failover, rebalance and multi-region pages, where every number on
 * the page comes from the simulator.
 */
export function SimulationNotice({ what, why }: { what: string; why: string }) {
  return (
    <div className="rounded-md border border-warn/40 bg-warn/10 px-3 py-2.5 text-xs">
      <div className="flex items-center gap-2 font-semibold text-warn">
        <OriginBadge origin="simulated" />
        <span>{what}</span>
      </div>
      <p className="mt-1 text-gray-600 dark:text-muted leading-relaxed">{why}</p>
    </div>
  );
}
