/**
 * Minimal, injection-safe Markdown for LLM answers.
 *
 * Builds React nodes directly (no innerHTML), so model output can never
 * smuggle markup into the page. Covers what answers actually use:
 * paragraphs, `-`/`*`/`1.` lists, `#` headings, **bold**, `code`, and
 * `[n]` citations, which render as chips that call `onCite(n)`.
 */
import { Fragment, type ReactNode } from "react";

export function Markdown({
  text,
  onCite,
  citeLabel,
}: {
  text: string;
  onCite?: (n: number) => void;
  /** Tooltip / short label for citation `n` (e.g. the candidate's name). */
  citeLabel?: (n: number) => string | undefined;
}) {
  const blocks = toBlocks(text);
  return (
    <div className="space-y-2 leading-relaxed">
      {blocks.map((b, i) => {
        const inl = (s: string) => inline(s, onCite, citeLabel);
        if (b.kind === "h") {
          return (
            <div key={i} className="font-semibold text-gray-900 dark:text-ink pt-1">
              {inl(b.text)}
            </div>
          );
        }
        if (b.kind === "ul" || b.kind === "ol") {
          const List = b.kind === "ul" ? "ul" : "ol";
          return (
            <List
              key={i}
              className={`${b.kind === "ul" ? "list-disc" : "list-decimal"} pl-5 space-y-1`}
            >
              {b.items.map((it, j) => (
                <li key={j}>{inl(it)}</li>
              ))}
            </List>
          );
        }
        return <p key={i}>{inl(b.text)}</p>;
      })}
    </div>
  );
}

type Block =
  | { kind: "p"; text: string }
  | { kind: "h"; text: string }
  | { kind: "ul"; items: string[] }
  | { kind: "ol"; items: string[] };

function toBlocks(src: string): Block[] {
  const out: Block[] = [];
  let para: string[] = [];
  const flushPara = () => {
    if (para.length) out.push({ kind: "p", text: para.join(" ") });
    para = [];
  };
  for (const raw of src.split("\n")) {
    const line = raw.trimEnd();
    const bullet = /^\s*[-*•+]\s+(.*)$/.exec(line);
    const numbered = /^\s*\d+[.)]\s+(.*)$/.exec(line);
    const heading = /^\s*#{1,6}\s+(.*)$/.exec(line);
    if (!line.trim()) {
      flushPara();
    } else if (heading) {
      flushPara();
      out.push({ kind: "h", text: heading[1] });
    } else if (bullet || numbered) {
      flushPara();
      const kind: "ul" | "ol" = bullet ? "ul" : "ol";
      const item = (bullet ?? numbered)![1];
      const last = out[out.length - 1];
      if (last && (last.kind === "ul" || last.kind === "ol") && last.kind === kind) last.items.push(item);
      else out.push({ kind, items: [item] });
    } else {
      // A wrapped continuation of the previous list item stays in it.
      const last = out[out.length - 1];
      if (!para.length && last && (last.kind === "ul" || last.kind === "ol") && /^\s{2,}/.test(raw)) {
        last.items[last.items.length - 1] += " " + line.trim();
      } else {
        para.push(line.trim());
      }
    }
  }
  flushPara();
  return out;
}

const TOKEN = /(\*\*[^*]+\*\*|`[^`]+`|\[\d+(?:\s*,\s*\d+)*\])/g;

function inline(
  s: string,
  onCite?: (n: number) => void,
  citeLabel?: (n: number) => string | undefined
): ReactNode {
  const parts = s.split(TOKEN);
  return parts.map((part, i) => {
    if (!part) return null;
    if (part.startsWith("**") && part.endsWith("**")) {
      return (
        <strong key={i} className="font-semibold text-gray-900 dark:text-ink">
          {part.slice(2, -2)}
        </strong>
      );
    }
    if (part.startsWith("`") && part.endsWith("`")) {
      return (
        <code key={i} className="px-1 rounded bg-gray-100 dark:bg-carbon-800 text-[0.85em]">
          {part.slice(1, -1)}
        </code>
      );
    }
    const cite = /^\[(\d+(?:\s*,\s*\d+)*)\]$/.exec(part);
    if (cite) {
      const ns = cite[1].split(",").map((x) => Number(x.trim()));
      return (
        <Fragment key={i}>
          {ns.map((n) => (
            <button
              key={n}
              type="button"
              onClick={() => onCite?.(n)}
              title={citeLabel?.(n) ?? `source ${n}`}
              className="mx-0.5 inline-flex items-center rounded border border-accent/40 bg-accent/10
                         px-1 font-mono text-[10px] leading-4 text-accent hover:bg-accent/20 align-baseline"
            >
              {citeLabel?.(n) ? `${n} · ${truncate(citeLabel(n)!, 18)}` : n}
            </button>
          ))}
        </Fragment>
      );
    }
    return <Fragment key={i}>{part}</Fragment>;
  });
}

function truncate(s: string, n: number) {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}
