/**
 * Shared bits of UI — extracted here to keep each tab file focused
 * on its domain logic instead of repeating the same display code.
 */
import type { ReactNode } from "react";

export function Panel({
  title,
  subtitle,
  action,
  children,
}: {
  title: string;
  subtitle?: string;
  action?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="card space-y-3">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h3 className="font-display text-base font-semibold text-gray-900 dark:text-ink">
            {title}
          </h3>
          {subtitle && (
            <p className="text-xs text-gray-500 dark:text-muted mt-0.5">{subtitle}</p>
          )}
        </div>
        {action}
      </div>
      {children}
    </section>
  );
}

export function ErrorBanner({ err }: { err: string | null }) {
  if (!err) return null;
  return (
    <div className="rounded-md border border-red-300/60 bg-red-50 text-red-800 dark:border-red-500/30 dark:bg-red-950/40 dark:text-red-300 px-3 py-2 text-sm font-mono">
      {err}
    </div>
  );
}

/**
 * Rich JSON viewer — backed by the collapsible tree in `tree.tsx`.
 * Exported under the old `JsonView` name so every call site
 * upgrades automatically. For the small number of cases where the
 * old flat `<pre>` rendering is still wanted (raw blob paste,
 * copy-friendly output), `JsonBlob` is kept around.
 */
import { JsonTree } from "./tree";

export function JsonView({ value }: { value: unknown }) {
  return (
    <div className="rounded-md border border-gray-200 bg-gray-50 p-2 overflow-x-auto max-h-[28rem] dark:border-edge dark:bg-carbon-950">
      <JsonTree value={value} />
    </div>
  );
}

export function JsonBlob({ value }: { value: unknown }) {
  return (
    <pre className="text-xs rounded-md border border-gray-200 bg-gray-50 p-2 overflow-x-auto max-h-80 whitespace-pre-wrap break-words dark:border-edge dark:bg-carbon-950">
      {JSON.stringify(value, null, 2)}
    </pre>
  );
}

export function Spinner({ label }: { label?: string }) {
  return (
    <span className="inline-flex items-center gap-2 text-sm text-gray-500 dark:text-muted">
      <span className="h-3 w-3 rounded-full border-2 border-accent border-t-transparent animate-spin" />
      {label ?? "working…"}
    </span>
  );
}

/**
 * Little labeled chip used for timings and doc counts. Keeps the
 * information-density high without dropping into raw text.
 */
export function Stat({ label, value }: { label: string; value: string | number }) {
  return (
    <span className="inline-flex items-baseline gap-1.5 text-xs text-gray-500 dark:text-muted">
      <span className="font-mono uppercase tracking-wider text-[10px]">{label}</span>
      <span className="font-mono font-semibold text-gray-900 dark:text-ink tabular-nums">{value}</span>
    </span>
  );
}

/**
 * The signature element: a semantic-similarity spectrum. `value` is a
 * normalized 0..1 relevance (1 = most similar) — a higher score lands
 * further along the violet->cyan spectrum, so relevance is legible at a
 * glance rather than hidden in a raw float. `label` is the honest
 * underlying number (a cosine score or a distance) shown in mono.
 */
export function Spectrum({ value, label }: { value: number; label: string }) {
  const pct = Math.round(Math.max(0, Math.min(1, value)) * 100);
  return (
    <div className="flex items-center gap-2 w-32 shrink-0" title={`relevance ${pct}%`}>
      <div className="spectrum-track flex-1">
        <div className="spectrum-fill" style={{ width: `${Math.max(4, pct)}%` }} />
      </div>
      <span className="font-mono text-xs text-accent tabular-nums w-14 text-right">
        {label}
      </span>
    </div>
  );
}
