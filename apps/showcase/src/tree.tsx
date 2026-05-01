import { useState } from "react";

/**
 * Expandable JSON tree viewer. Replaces `<pre>JSON.stringify</pre>`
 * on panels where users actually want to browse nested metadata
 * (SQL row inspector, Search hit metadata, RAG debug frame, EXPLAIN
 * plan tree).
 *
 * Design:
 * - Recursive. Every object / array can expand / collapse.
 * - Root defaults expanded, nested nodes collapsed — keeps the
 *   initial view compact.
 * - Type colouring (strings / numbers / booleans / null) so a
 *   stringified number doesn't look the same as an integer.
 * - Monospace throughout — matches the rest of the code UI.
 */

interface JsonTreeProps {
  value: unknown;
  /** Override the root's initial expanded state. Defaults true. */
  defaultExpanded?: boolean;
}

export function JsonTree({ value, defaultExpanded = true }: JsonTreeProps) {
  return (
    <div className="text-xs font-mono leading-6 overflow-x-auto">
      <Node value={value} name={null} depth={0} defaultExpanded={defaultExpanded} />
    </div>
  );
}

function Node({
  value,
  name,
  depth,
  defaultExpanded,
}: {
  value: unknown;
  name: string | null;
  depth: number;
  defaultExpanded: boolean;
}) {
  const isObject = value !== null && typeof value === "object" && !Array.isArray(value);
  const isArray = Array.isArray(value);

  if (isObject || isArray) {
    return (
      <CollapsibleNode
        value={value as Record<string, unknown> | unknown[]}
        name={name}
        depth={depth}
        isArray={isArray}
        defaultExpanded={defaultExpanded}
      />
    );
  }
  return <LeafNode value={value} name={name} depth={depth} />;
}

function CollapsibleNode({
  value,
  name,
  depth,
  isArray,
  defaultExpanded,
}: {
  value: Record<string, unknown> | unknown[];
  name: string | null;
  depth: number;
  isArray: boolean;
  defaultExpanded: boolean;
}) {
  const [open, setOpen] = useState(depth === 0 ? defaultExpanded : false);
  const entries = isArray
    ? (value as unknown[]).map((v, i) => [String(i), v] as const)
    : Object.entries(value as Record<string, unknown>);
  const empty = entries.length === 0;
  const opener = isArray ? "[" : "{";
  const closer = isArray ? "]" : "}";
  const countLabel = `${entries.length} ${isArray ? "items" : "keys"}`;

  return (
    <div style={{ marginLeft: depth === 0 ? 0 : "1rem" }}>
      <div className="flex items-baseline gap-1">
        <button
          className="text-gray-400 hover:text-gray-700 dark:hover:text-gray-200 w-3 select-none"
          onClick={() => !empty && setOpen((v) => !v)}
          title={empty ? "empty" : open ? "collapse" : "expand"}
          disabled={empty}
        >
          {empty ? "·" : open ? "▾" : "▸"}
        </button>
        {name !== null && <NameLabel name={name} />}
        <span className="text-gray-500">{opener}</span>
        {!open && !empty && (
          <span
            className="text-gray-400 italic cursor-pointer"
            onClick={() => setOpen(true)}
          >
            {" "}
            {countLabel}
          </span>
        )}
        {!open && <span className="text-gray-500">{closer}</span>}
      </div>
      {open && (
        <>
          {entries.map(([k, v]) => (
            <Node
              key={k}
              value={v}
              name={k}
              depth={depth + 1}
              defaultExpanded={defaultExpanded}
            />
          ))}
          <div style={{ marginLeft: "1rem" }} className="text-gray-500">
            {closer}
          </div>
        </>
      )}
    </div>
  );
}

function LeafNode({
  value,
  name,
  depth,
}: {
  value: unknown;
  name: string | null;
  depth: number;
}) {
  return (
    <div
      style={{ marginLeft: depth === 0 ? 0 : "1rem" }}
      className="flex items-baseline gap-1"
    >
      <span className="w-3" />
      {name !== null && <NameLabel name={name} />}
      <ValueLabel value={value} />
    </div>
  );
}

function NameLabel({ name }: { name: string }) {
  return (
    <>
      <span className="text-brand-700 dark:text-brand-500">{JSON.stringify(name)}</span>
      <span className="text-gray-500">:</span>
    </>
  );
}

function ValueLabel({ value }: { value: unknown }) {
  if (value === null) return <span className="text-gray-400 italic">null</span>;
  if (typeof value === "boolean") {
    return <span className="text-purple-600 dark:text-purple-400">{String(value)}</span>;
  }
  if (typeof value === "number") {
    return <span className="text-orange-600 dark:text-orange-400">{String(value)}</span>;
  }
  if (typeof value === "string") {
    return (
      <span className="text-green-700 dark:text-green-400 break-all">
        {JSON.stringify(value)}
      </span>
    );
  }
  // Shouldn't be reachable (objects/arrays go through CollapsibleNode);
  // render defensively as a stringified blob.
  return <span className="text-gray-700 dark:text-gray-300">{String(value)}</span>;
}
