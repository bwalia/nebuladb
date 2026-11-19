/**
 * Engagement (internal showcase only): who visited the customer demo
 * sites, from where, which videos they watched and how far, and what they
 * clicked or typed. Reads the `showcase_events` bucket that
 * njs/analytics.js writes (identity, IP and country are stamped server-side).
 */
import { useCallback, useEffect, useMemo, useState } from "react";
import { ErrorBanner, Panel, Spinner } from "../components";
import { field, lit, runSql } from "../rag/sql";

const BUCKET = "showcase_events";
const LIMIT = 2500; // filters run over LIMIT × 4 candidates: covers 10k events

interface Ev {
  id: string;
  type: string;
  user: string;
  host: string;
  ip: string;
  country: string;
  countrySource: string;
  lang: string;
  tz: string;
  ua: string;
  title: string;
  path: string;
  video: string;
  pct: number | null;
  seconds: number | null;
  label: string;
  value: string;
  session: string;
  ts: number;
}

interface Person {
  user: string;
  events: Ev[];
  first: number;
  last: number;
  sessions: number;
  countries: string[];
  ips: string[];
  langs: string[];
  devices: string[];
  pages: number;
  videos: Map<string, { pct: number; seconds: number }>;
  clicks: number;
  inputs: Ev[];
}

const s = (v: unknown) => (v === undefined || v === null ? "" : String(v));
const n = (v: unknown) => (v === undefined || v === null || v === "" || !Number.isFinite(Number(v)) ? null : Number(v));

function flag(cc: string): string {
  return /^[A-Z]{2}$/.test(cc) ? String.fromCodePoint(...[...cc].map((c) => 0x1f1a5 + c.charCodeAt(0))) : "";
}
function when(ts: number): string {
  return new Date(ts * 1000).toLocaleString("en-GB", { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" });
}
function device(ua: string): string {
  const os = /iPhone|iPad/.test(ua) ? "iOS" : /Android/.test(ua) ? "Android" : /Mac OS X/.test(ua) ? "Mac" : /Windows/.test(ua) ? "Windows" : /Linux/.test(ua) ? "Linux" : "";
  const br = /Edg\//.test(ua) ? "Edge" : /Chrome\//.test(ua) ? "Chrome" : /Firefox\//.test(ua) ? "Firefox" : /Safari\//.test(ua) ? "Safari" : "";
  return [br, os].filter(Boolean).join(" on ") || "unknown";
}
function mmss(sec: number): string {
  const t = Math.round(sec);
  return `${Math.floor(t / 60)}:${String(t % 60).padStart(2, "0")}`;
}
const uniq = (xs: string[]) => [...new Set(xs.filter(Boolean))];

function describe(e: Ev): string {
  switch (e.type) {
    case "page_view":
      return `Viewed ${e.title || e.path}`;
    case "video_play":
      return `Started “${e.video}”`;
    case "video_progress":
      return `Watched ${e.pct}% of “${e.video}”`;
    case "video_complete":
      return `Finished “${e.video}” (${mmss(e.seconds ?? 0)} watched)`;
    case "video_stop":
      return `Stopped “${e.video}” at ${e.pct}% (${mmss(e.seconds ?? 0)} watched)`;
    case "click":
      return `Clicked “${e.label}”`;
    case "input":
      return `Typed “${e.value}” into ${e.label}`;
    case "session_end":
      return `Left after ${mmss(e.seconds ?? 0)}`;
    default:
      return e.type;
  }
}

export function EngagementTab() {
  const [events, setEvents] = useState<Ev[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [site, setSite] = useState("sourcesupplies.nebuladb.net");
  const [sel, setSel] = useState<string | null>(null);

  const load = useCallback(async () => {
    setBusy(true);
    setErr(null);
    try {
      const where = site ? `\n   AND host = ${lit(site)}` : "";
      const r = await runSql(`SELECT * FROM ${BUCKET}\n WHERE semantic_match(text, ${lit("visitor activity on the demo site")})${where}\n LIMIT ${LIMIT}`);
      setEvents(
        r.rows
          .map((row) => ({
            id: row.id,
            type: s(field(row, "type")),
            user: s(field(row, "user")) || "anonymous",
            host: s(field(row, "host")),
            ip: s(field(row, "ip")),
            country: s(field(row, "country")),
            countrySource: s(field(row, "country_source")),
            lang: s(field(row, "langs")) || s(field(row, "lang")),
            tz: s(field(row, "tz")),
            ua: s(field(row, "ua")),
            title: s(field(row, "title")),
            path: s(field(row, "path")),
            video: s(field(row, "video")),
            pct: n(field(row, "pct")),
            seconds: n(field(row, "seconds")),
            label: s(field(row, "label")),
            value: s(field(row, "value")),
            session: s(field(row, "session")),
            ts: n(field(row, "ts")) ?? 0,
          }))
          .sort((a, b) => b.ts - a.ts)
      );
    } catch (e) {
      const msg = (e as Error).message;
      // No events yet: the bucket is created by the first write.
      if (/not found|unknown bucket|no such bucket/i.test(msg)) setEvents([]);
      else setErr(msg);
    } finally {
      setBusy(false);
    }
  }, [site]);

  useEffect(() => {
    void load();
  }, [load]);

  const people = useMemo<Person[]>(() => {
    const by = new Map<string, Ev[]>();
    for (const e of events ?? []) by.set(e.user, [...(by.get(e.user) ?? []), e]);
    return [...by.entries()]
      .map(([user, evs]) => {
        const videos = new Map<string, { pct: number; seconds: number }>();
        for (const e of evs) {
          if (!e.type.startsWith("video") || !e.video) continue;
          const v = videos.get(e.video) ?? { pct: 0, seconds: 0 };
          videos.set(e.video, { pct: Math.max(v.pct, e.pct ?? 0), seconds: Math.max(v.seconds, e.seconds ?? 0) });
        }
        return {
          user,
          events: evs,
          first: Math.min(...evs.map((e) => e.ts)),
          last: Math.max(...evs.map((e) => e.ts)),
          sessions: uniq(evs.map((e) => e.session)).length,
          countries: uniq(evs.map((e) => e.country)),
          ips: uniq(evs.map((e) => e.ip)),
          langs: uniq(evs.map((e) => e.lang)),
          devices: uniq(evs.map((e) => device(e.ua))),
          pages: evs.filter((e) => e.type === "page_view").length,
          videos,
          clicks: evs.filter((e) => e.type === "click").length,
          inputs: evs.filter((e) => e.type === "input"),
        };
      })
      .sort((a, b) => b.last - a.last);
  }, [events]);

  const person = people.find((p) => p.user === sel) ?? null;

  return (
    <div className="space-y-4">
      <Panel
        title="Engagement"
        subtitle="Who opened the customer demo sites, from where, what they watched and what they tried. Identity, IP and country are recorded by the server."
        action={
          <div className="flex items-center gap-2">
            <select className="input !w-auto !py-1.5 text-sm" value={site} onChange={(e) => setSite(e.target.value)} aria-label="Site">
              <option value="sourcesupplies.nebuladb.net">sourcesupplies.nebuladb.net</option>
              <option value="">All sites</option>
            </select>
            <button className="btn-secondary !py-1.5" onClick={() => void load()} disabled={busy}>
              {busy ? <Spinner label="loading…" /> : "Refresh"}
            </button>
          </div>
        }
      >
        <ErrorBanner err={err} />
        {events && events.length === 0 && <p className="text-sm text-gray-500 dark:text-muted">No visits recorded yet.</p>}
        {people.length > 0 && (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs text-gray-500 dark:text-muted border-b border-gray-200 dark:border-edge">
                  <th className="py-2 pr-3">Person</th>
                  <th className="py-2 pr-3">From</th>
                  <th className="py-2 pr-3">Visits</th>
                  <th className="py-2 pr-3">Videos watched</th>
                  <th className="py-2 pr-3">Activity</th>
                  <th className="py-2 pr-3">Last seen</th>
                </tr>
              </thead>
              <tbody>
                {people.map((p) => (
                  <tr
                    key={p.user}
                    onClick={() => setSel(sel === p.user ? null : p.user)}
                    className={`cursor-pointer border-b border-gray-100 dark:border-edge align-top hover:bg-gray-50 dark:hover:bg-carbon-900 ${sel === p.user ? "bg-accent/5" : ""}`}
                  >
                    <td className="py-2 pr-3 font-medium">{p.user}</td>
                    <td className="py-2 pr-3">
                      {p.countries.map((c) => `${flag(c)} ${c}`).join(", ") || "—"}
                      <div className="text-[11px] text-gray-500 dark:text-muted">{p.ips.slice(0, 2).join(", ")}</div>
                    </td>
                    <td className="py-2 pr-3 tabular-nums">{p.sessions}</td>
                    <td className="py-2 pr-3">
                      {p.videos.size === 0
                        ? "—"
                        : [...p.videos.entries()].map(([t, v]) => (
                            <div key={t} className="text-xs">
                              <span className={v.pct >= 95 ? "text-ok" : ""}>{v.pct >= 95 ? "✓" : `${v.pct}%`}</span> {t}
                            </div>
                          ))}
                    </td>
                    <td className="py-2 pr-3 text-xs text-gray-600 dark:text-muted">
                      {p.pages} pages · {p.clicks} clicks · {p.inputs.length} typed
                    </td>
                    <td className="py-2 pr-3 text-xs whitespace-nowrap">{when(p.last)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>

      {person && (
        <Panel title={person.user} subtitle={`First seen ${when(person.first)} · last seen ${when(person.last)} · ${person.events.length} events`}>
          <dl className="grid gap-x-6 gap-y-1 text-sm sm:grid-cols-2">
            <div>
              <dt className="eyebrow">Country</dt>
              <dd>
                {person.countries.map((c) => `${flag(c)} ${c}`).join(", ") || "unknown"}
                {person.events[0]?.countrySource && (
                  <span className="text-xs text-gray-500 dark:text-muted"> ({person.events[0].countrySource === "ip" ? "from IP" : "from browser settings"})</span>
                )}
              </dd>
            </div>
            <div>
              <dt className="eyebrow">IP addresses</dt>
              <dd className="font-mono text-xs">{person.ips.join(", ") || "—"}</dd>
            </div>
            <div>
              <dt className="eyebrow">Browser language</dt>
              <dd>{person.langs.join(" / ") || "—"}</dd>
            </div>
            <div>
              <dt className="eyebrow">Device</dt>
              <dd>
                {person.devices.join(", ")}
                {person.events[0]?.tz && <span className="text-xs text-gray-500 dark:text-muted"> · {person.events[0].tz}</span>}
              </dd>
            </div>
          </dl>
          <ol className="mt-3 space-y-1 border-l border-gray-200 dark:border-edge pl-3">
            {person.events.map((e) => (
              <li key={e.id} className="text-sm">
                <span className="mr-2 text-xs tabular-nums text-gray-500 dark:text-muted">{when(e.ts)}</span>
                {describe(e)}
              </li>
            ))}
          </ol>
        </Panel>
      )}
    </div>
  );
}
