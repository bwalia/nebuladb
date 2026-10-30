/**
 * Register explorer: search the bulk-loaded Companies House register by
 * meaning ("software consultancy"), then narrow by distance from a place
 * and by the size of accounts filed, on a map and a table you can export.
 *
 * Honest limits, shown in the UI:
 * - NebulaDB filters run on the semantic search's candidates (the top N
 *   matches), not a scan of all 5.1M companies.
 * - The basic register has no turnover, so "revenue > £1m" can't be
 *   answered from it; accounts category is offered as a size proxy.
 * - The register has postcodes, not coordinates: postcodes are geocoded
 *   in the browser (postcodes.io) to measure distance.
 */
import { useMemo, useState } from "react";
import { ApiError } from "../api";
import { ErrorBanner, Panel, Spinner, Stat } from "../components";
import { CompaniesMap, geocodePostcodes } from "../CompaniesMap";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { field, lit, runSql, semanticText } from "../rag/sql";

const BUCKET = "companies_house";

const PLACES = {
  anywhere: { label: "Anywhere in the UK", lat: NaN, lng: NaN },
  london: { label: "Central London", lat: 51.5074, lng: -0.1278 },
  manchester: { label: "Manchester", lat: 53.4808, lng: -2.2426 },
  birmingham: { label: "Birmingham", lat: 52.4862, lng: -1.8904 },
  leeds: { label: "Leeds", lat: 53.8008, lng: -1.5491 },
  bristol: { label: "Bristol", lat: 51.4545, lng: -2.5879 },
  edinburgh: { label: "Edinburgh", lat: 55.9533, lng: -3.1883 },
} as const;
type PlaceKey = keyof typeof PLACES;

/** Accounts category (from the register) grouped into size bands. */
const SIZES = {
  any: { label: "Any", match: () => true },
  larger: {
    label: "Full / group / medium accounts (larger)",
    match: (c: string) => /^(FULL|GROUP|MEDIUM)$/.test(c),
  },
  small: {
    label: "Small / abridged / total exemption",
    match: (c: string) => /SMALL|ABRIDGED|TOTAL EXEMPTION|PARTIAL EXEMPTION/.test(c),
  },
  micro: { label: "Micro-entity", match: (c: string) => c === "MICRO ENTITY" },
  dormant: { label: "Dormant", match: (c: string) => c === "DORMANT" },
} as const;
type SizeKey = keyof typeof SIZES;

interface Options {
  q: string;
  place: PlaceKey;
  miles: number;
  size: SizeKey;
  activeOnly: boolean;
  candidates: number;
}

const EXAMPLES: Array<{ label: string; opts: Options }> = [
  {
    label: "Software firms within 100 mi of central London",
    opts: { q: "software development and IT consultancy", place: "london", miles: 100, size: "any", activeOnly: true, candidates: 250 },
  },
  {
    label: "Larger fintech companies near London",
    opts: { q: "financial technology, payments and banking software", place: "london", miles: 100, size: "larger", activeOnly: true, candidates: 250 },
  },
  {
    label: "Builders within 30 mi of Manchester",
    opts: { q: "construction, building contractors and civil engineering", place: "manchester", miles: 30, size: "any", activeOnly: true, candidates: 250 },
  },
  {
    label: "Restaurants and cafés in Edinburgh",
    opts: { q: "restaurants, cafes and takeaway food", place: "edinburgh", miles: 10, size: "any", activeOnly: true, candidates: 250 },
  },
  {
    label: "Dormant property companies near Bristol",
    opts: { q: "buying, selling and letting of own real estate", place: "bristol", miles: 25, size: "dormant", activeOnly: false, candidates: 250 },
  },
];

interface Row {
  number: string;
  name: string;
  town: string;
  postcode: string;
  accounts: string;
  sic: string;
  incorporated: string;
  status: string;
  lat?: number;
  lng?: number;
  miles?: number;
}

const ACCOUNTS_RE = /Accounts category: ([^;]+);/;

/** Great-circle distance in miles. */
function haversineMiles(aLat: number, aLng: number, bLat: number, bLng: number): number {
  const rad = (d: number) => (d * Math.PI) / 180;
  const dLat = rad(bLat - aLat);
  const dLng = rad(bLng - aLng);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(rad(aLat)) * Math.cos(rad(bLat)) * Math.sin(dLng / 2) ** 2;
  return 3958.8 * 2 * Math.asin(Math.sqrt(h));
}

const normPc = (p: string) => p.toUpperCase().replace(/\s+/g, "");

function toCsv(rows: Row[]): string {
  const cols: Array<[string, (r: Row) => string]> = [
    ["company_number", (r) => r.number],
    ["company_name", (r) => r.name],
    ["status", (r) => r.status],
    ["post_town", (r) => r.town],
    ["postcode", (r) => r.postcode],
    ["distance_miles", (r) => (r.miles != null ? r.miles.toFixed(1) : "")],
    ["accounts_category", (r) => r.accounts],
    ["sic", (r) => r.sic],
    ["incorporated", (r) => r.incorporated],
  ];
  const esc = (v: string) => (/[",\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v);
  return [cols.map((c) => c[0]).join(","), ...rows.map((r) => cols.map(([, f]) => esc(f(r))).join(","))].join("\n");
}

function buildSql(o: Options): string {
  let s = `SELECT * FROM ${BUCKET}\nWHERE semantic_match(text, ${lit(semanticText(o.q))})\n  AND kind = 'register_basic'`;
  if (o.activeOnly) s += `\n  AND status = 'Active'`;
  return s + `\nLIMIT ${o.candidates}`;
}

export function CompaniesExplorer({ onPick }: { onPick: (companyNumber: string) => void }) {
  const [opts, setOpts] = useState<Options>(EXAMPLES[0].opts);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [rows, setRows] = useState<Row[] | null>(null);
  const [ran, setRan] = useState<Options | null>(null);
  const [selected, setSelected] = useState<string | null>(null);

  const set = <K extends keyof Options>(k: K, v: Options[K]) => setOpts((o) => ({ ...o, [k]: v }));
  const hasPlace = opts.place !== "anywhere";

  const sql = useMemo(() => buildSql(opts), [opts]);

  const run = async (o: Options = opts) => {
    if (!o.q.trim()) return;
    setErr(null);
    setBusy(true);
    setSelected(null);
    const p = PLACES[o.place];
    const withPlace = o.place !== "anywhere";
    const q = buildSql(o);
    const trace: PipelineStep[] = [
      { label: "Semantic search + register filters (NebulaDB SQL)", detail: q, status: "running" },
      { label: withPlace ? `Geocode postcodes, keep ≤ ${o.miles} mi of ${p.label}` : "Distance filter", status: withPlace ? "running" : "skipped" },
      { label: `Accounts size: ${SIZES[o.size].label}`, status: o.size === "any" ? "skipped" : "running" },
    ];
    setSteps([...trace]);
    try {
      const r = await runSql(q);
      let out: Row[] = r.rows.map((row) => {
        const text = String(field(row, "text") ?? "");
        const sics = field<string[]>(row, "sic_codes") ?? [];
        return {
          number: String(field(row, "company_number") ?? row.id),
          name: String(field(row, "company_name") ?? ""),
          town: String(field(row, "post_town") ?? ""),
          postcode: String(field(row, "postcode") ?? ""),
          accounts: (ACCOUNTS_RE.exec(text)?.[1] ?? "").trim().toUpperCase(),
          sic: Array.isArray(sics) ? sics.join("; ") : String(sics),
          incorporated: String(field(row, "incorporation_date") ?? ""),
          status: String(field(row, "status") ?? ""),
        };
      });
      trace[0] = { ...trace[0], rowsOut: out.length, ms: r.tookMs, status: "ok" };
      setSteps([...trace]);

      const started = performance.now();
      const ctrl = new AbortController();
      const coords = await geocodePostcodes(out.map((x) => x.postcode).filter(Boolean), ctrl.signal);
      out = out.map((x) => {
        const ll = coords[normPc(x.postcode)];
        if (!ll) return x;
        return { ...x, ...ll, miles: withPlace ? haversineMiles(p.lat, p.lng, ll.lat, ll.lng) : undefined };
      });
      if (withPlace) {
        out = out.filter((x) => x.miles != null && x.miles <= o.miles);
        trace[1] = { ...trace[1], rowsOut: out.length, ms: Math.round(performance.now() - started), status: "ok" };
      }
      if (o.size !== "any") {
        out = out.filter((x) => SIZES[o.size].match(x.accounts));
        trace[2] = { ...trace[2], rowsOut: out.length, status: "ok" };
      }
      setSteps([...trace]);
      setRows(out);
      setRan(o);
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message);
      setSteps(trace.map((s) => (s.status === "running" ? { ...s, status: "error" } : s)));
    } finally {
      setBusy(false);
    }
  };

  const circle = useMemo(
    () => (ran && ran.place !== "anywhere" ? { lat: PLACES[ran.place].lat, lng: PLACES[ran.place].lng, miles: ran.miles } : undefined),
    [ran]
  );

  const download = () => {
    if (!rows) return;
    const url = URL.createObjectURL(new Blob([toCsv(rows)], { type: "text/csv" }));
    const a = document.createElement("a");
    a.href = url;
    a.download = `companies-house-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <Panel
      title="Explore the register"
      subtitle="Describe the kind of business → semantic search over the loaded register → narrow by distance and accounts size → map, table, CSV"
    >
      <div className="flex flex-wrap gap-1.5 mb-3">
        <span className="eyebrow self-center mr-1">Examples</span>
        {EXAMPLES.map((ex) => (
          <button
            key={ex.label}
            type="button"
            onClick={() => {
              setOpts(ex.opts);
              void run(ex.opts);
            }}
            className={`rounded-full border px-2.5 py-1 text-xs transition-colors ${
              opts === ex.opts
                ? "border-accent text-accent bg-accent/10"
                : "border-gray-200 dark:border-edge text-gray-600 dark:text-muted hover:border-accent/60"
            }`}
          >
            {ex.label}
          </button>
        ))}
      </div>

      <label className="block mb-3">
        <span className="block text-xs font-medium mb-1">What kind of business?</span>
        <input
          className="input"
          value={opts.q}
          onChange={(e) => set("q", e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") void run();
          }}
          placeholder="e.g. software consultancy, dental practice, logistics…"
        />
      </label>

      <div className="flex flex-wrap gap-3 items-end">
        <label className="text-xs">
          <span className="block font-medium mb-1">Near</span>
          <select className="code-input !py-1.5 !text-sm" value={opts.place} onChange={(e) => set("place", e.target.value as PlaceKey)}>
            {Object.entries(PLACES).map(([k, v]) => (
              <option key={k} value={k}>
                {v.label}
              </option>
            ))}
          </select>
        </label>
        <label className="text-xs">
          <span className="block font-medium mb-1">Within (miles)</span>
          <input
            type="number"
            min={1}
            max={600}
            disabled={!hasPlace}
            className="code-input !py-1.5 !text-sm w-24"
            value={opts.miles}
            onChange={(e) => set("miles", Math.max(1, Number(e.target.value) || 1))}
          />
        </label>
        <label className="text-xs">
          <span className="block font-medium mb-1">Accounts filed (size proxy)</span>
          <select className="code-input !py-1.5 !text-sm" value={opts.size} onChange={(e) => set("size", e.target.value as SizeKey)}>
            {Object.entries(SIZES).map(([k, v]) => (
              <option key={k} value={k}>
                {v.label}
              </option>
            ))}
          </select>
        </label>
        <label className="text-xs" title="Not in the Companies House register: it publishes no turnover figures.">
          <span className="block font-medium mb-1 opacity-60">2025 revenue</span>
          <select className="code-input !py-1.5 !text-sm opacity-60" disabled value="">
            <option value="">not on the register</option>
          </select>
        </label>
        <label className="text-xs">
          <span className="block font-medium mb-1">Candidates</span>
          <select className="code-input !py-1.5 !text-sm" value={opts.candidates} onChange={(e) => set("candidates", Number(e.target.value))}>
            {[100, 250, 500].map((n) => (
              <option key={n} value={n}>
                top {n}
              </option>
            ))}
          </select>
        </label>
        <label className="text-xs flex items-center gap-1.5 pb-2">
          <input type="checkbox" checked={opts.activeOnly} onChange={(e) => set("activeOnly", e.target.checked)} />
          Active only
        </label>
        <button className="btn" disabled={busy || !opts.q.trim()} onClick={() => void run()}>
          {busy ? <Spinner label="searching…" /> : "Search register"}
        </button>
      </div>

      <details className="mt-3 text-xs">
        <summary className="cursor-pointer opacity-70">How this works, and its limits</summary>
        <ul className="mt-2 space-y-1 list-disc pl-5 opacity-80">
          <li>
            NebulaDB returns the top {opts.candidates} register entries closest in meaning to your description. The status filter is applied in
            the same SQL query; distance and accounts size are then applied to those candidates. It is not a scan of every company, so raise
            “Candidates” or make the description more specific if few results survive.
          </li>
          <li>
            <strong>Revenue:</strong> the Companies House register does not publish turnover, so “revenue in 2025 over £1m” can't be answered
            from it. Many small and micro companies don't disclose turnover in their accounts at all. “Full / group / medium accounts” is a
            rough size proxy (it usually means a larger company, though small ones can file full accounts too). Real turnover would need
            loading from the accounts filings (iXBRL) into each company's metadata, which would make it an exact filter like{" "}
            <code>turnover_gbp &gt; 1000000</code>.
          </li>
          <li>
            The register holds postcodes, not coordinates. Postcodes are looked up with postcodes.io in your browser to measure the distance to
            the chosen place.
          </li>
        </ul>
        <pre className="mt-2 font-mono text-[11px] whitespace-pre-wrap opacity-80">{sql}</pre>
      </details>

      <ErrorBanner err={err} />
      <PipelineTrace steps={steps} />

      {rows && ran && (
        <div className="mt-4">
          <div className="flex flex-wrap items-center gap-4 mb-2">
            <Stat label="matches" value={rows.length} />
            {ran.place !== "anywhere" && <Stat label="area" value={`${ran.miles} mi of ${PLACES[ran.place].label}`} />}
            {ran.size !== "any" && <Stat label="size" value={SIZES[ran.size].label} />}
            <button type="button" className="btn-secondary text-xs ml-auto" disabled={rows.length === 0} onClick={download}>
              Download CSV
            </button>
          </div>
          {rows.length === 0 ? (
            <p className="text-sm text-gray-500 dark:text-muted">
              No candidates survived the filters. Try more candidates, a wider radius, or a different description.
            </p>
          ) : (
            <>
              <CompaniesMap
                companies={rows.map((r) => ({
                  company_number: r.number,
                  title: r.name,
                  company_status: r.accounts ? `${r.status} · ${r.accounts.toLowerCase()} accounts` : r.status,
                  address_snippet: [r.town, r.postcode, r.miles != null ? `${r.miles.toFixed(0)} mi away` : ""].filter(Boolean).join(" · "),
                  postcode: r.postcode,
                  lat: r.lat,
                  lng: r.lng,
                }))}
                selected={selected}
                onSelect={setSelected}
                circle={circle}
              />
              <div className="mt-3 max-h-96 overflow-auto rounded border border-gray-200 dark:border-edge">
                <table className="w-full text-xs">
                  <thead className="sticky top-0 bg-gray-50 dark:bg-carbon-900 text-left">
                    <tr>
                      <th className="px-2 py-1.5">Company</th>
                      <th className="px-2 py-1.5">Town</th>
                      {ran.place !== "anywhere" && <th className="px-2 py-1.5 text-right">Miles</th>}
                      <th className="px-2 py-1.5">Accounts</th>
                      <th className="px-2 py-1.5">Nature of business</th>
                      <th className="px-2 py-1.5" />
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200 dark:divide-edge">
                    {rows.map((r) => (
                      <tr
                        key={r.number}
                        onClick={() => setSelected(r.number)}
                        className={`cursor-pointer ${selected === r.number ? "bg-accent/10" : "hover:bg-gray-50 dark:hover:bg-carbon-900"}`}
                      >
                        <td className="px-2 py-1.5">
                          <div className="font-medium">{r.name}</div>
                          <div className="opacity-60 font-mono">{r.number}</div>
                        </td>
                        <td className="px-2 py-1.5">
                          {r.town}
                          <div className="opacity-60">{r.postcode}</div>
                        </td>
                        {ran.place !== "anywhere" && (
                          <td className="px-2 py-1.5 text-right tabular-nums">{r.miles != null ? r.miles.toFixed(0) : "—"}</td>
                        )}
                        <td className="px-2 py-1.5">{r.accounts ? r.accounts.toLowerCase() : "—"}</td>
                        <td className="px-2 py-1.5 max-w-[16rem] truncate" title={r.sic}>
                          {r.sic || "—"}
                        </td>
                        <td className="px-2 py-1.5 text-right">
                          <button
                            type="button"
                            className="text-accent hover:underline whitespace-nowrap"
                            onClick={(e) => {
                              e.stopPropagation();
                              onPick(r.number);
                            }}
                          >
                            Open →
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      )}
    </Panel>
  );
}
