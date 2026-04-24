/**
 * Tiny utilities shared across Admin + SQL tabs. Deliberately
 * framework-free so the state survives react remounts, storage
 * eviction, and dev-tools hot-reload.
 */

const HISTORY_KEY = "nebula:sql-history";
const HISTORY_CAP = 50;

export interface HistoryEntry {
  ts: number;
  sql: string;
  ok: boolean;
  took_ms?: number;
  rows?: number;
  error?: string;
}

/** Read the SQL history ring buffer. Returns newest-first. */
export function loadHistory(): HistoryEntry[] {
  try {
    const raw = localStorage.getItem(HISTORY_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.slice(0, HISTORY_CAP) : [];
  } catch {
    return [];
  }
}

/** Push an entry, drop oldest past the cap. Safe under quota errors. */
export function recordHistory(entry: HistoryEntry): void {
  try {
    const cur = loadHistory();
    const next = [entry, ...cur].slice(0, HISTORY_CAP);
    localStorage.setItem(HISTORY_KEY, JSON.stringify(next));
  } catch {
    /* quota exceeded / private mode — history is ephemeral */
  }
}

export function clearHistory(): void {
  try {
    localStorage.removeItem(HISTORY_KEY);
  } catch {
    /* private mode */
  }
}

/**
 * Push a Blob as a browser download. No fancy file-saver dep — a
 * throw-away anchor element does the trick on every modern browser.
 */
export function downloadBlob(filename: string, body: Blob): void {
  const url = URL.createObjectURL(body);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  // Defer cleanup so Safari has time to pick up the download.
  setTimeout(() => {
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }, 0);
}

/**
 * Convert SQL rows to CSV. Column order follows the first row's
 * key insertion order (same rule the SQL tab uses for its table),
 * so exports match what the user sees.
 *
 * Escaping rule: wrap in quotes if the value contains `,`, `"`, or
 * a newline; double any embedded quotes. Matches RFC 4180.
 */
export function rowsToCsv(
  rows: Array<{
    id: string;
    bucket: string;
    score: number;
    fields: Record<string, unknown>;
  }>
): string {
  if (rows.length === 0) return "";

  const seen = new Set<string>();
  const cols: string[] = ["id", "bucket", "score"];
  for (const c of cols) seen.add(c);
  for (const r of rows) {
    for (const k of Object.keys(r.fields ?? {})) {
      if (!seen.has(k)) {
        seen.add(k);
        cols.push(k);
      }
    }
  }

  const esc = (v: unknown): string => {
    let s: string;
    if (v === null || v === undefined) s = "";
    else if (typeof v === "string") s = v;
    else s = JSON.stringify(v);
    if (/[",\n]/.test(s)) {
      return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
  };

  const lines = [cols.map(esc).join(",")];
  for (const r of rows) {
    const line = cols.map((c) => {
      if (c === "id") return esc(r.id);
      if (c === "bucket") return esc(r.bucket);
      if (c === "score") return esc(r.score);
      return esc((r.fields ?? {})[c]);
    });
    lines.push(line.join(","));
  }
  return lines.join("\n");
}
