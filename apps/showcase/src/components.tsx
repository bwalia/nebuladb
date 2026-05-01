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
          <h2 className="text-base font-semibold">{title}</h2>
          {subtitle && (
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">{subtitle}</p>
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
    <div className="rounded-md border border-red-200 bg-red-50 text-red-800 dark:border-red-800 dark:bg-red-950/30 dark:text-red-300 px-3 py-2 text-sm">
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
    <div className="bg-gray-100 dark:bg-gray-950 rounded-md p-2 overflow-x-auto max-h-[28rem]">
      <JsonTree value={value} />
    </div>
  );
}

export function JsonBlob({ value }: { value: unknown }) {
  return (
    <pre className="text-xs bg-gray-100 dark:bg-gray-950 rounded-md p-2 overflow-x-auto max-h-80 whitespace-pre-wrap break-words">
      {JSON.stringify(value, null, 2)}
    </pre>
  );
}

export function Spinner({ label }: { label?: string }) {
  return (
    <span className="inline-flex items-center gap-2 text-sm text-gray-500 dark:text-gray-400">
      <span className="h-3 w-3 rounded-full border-2 border-brand-500 border-t-transparent animate-spin" />
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
    <span className="inline-flex items-baseline gap-1 text-xs text-gray-500 dark:text-gray-400">
      <span>{label}</span>
      <span className="font-semibold text-gray-900 dark:text-gray-100">{value}</span>
    </span>
  );
}
