/**
 * Engagement (internal showcase only): who visited the customer demo
 * sites and the websites carrying the pixel, from where, which videos they
 * watched and how far, and what they clicked or typed. Reads the buckets
 * njs/analytics.js writes (`showcase_events`, `web_events`; IP and country
 * are stamped server-side). Sites are listed in ./engagementSites.ts.
 */
import { useCallback, useEffect, useMemo, useState } from "react";
import { ErrorBanner, Panel, Spinner } from "../components";
import { type SqlRow } from "../api";
import { field, lit, runSql } from "../rag/sql";
import { BUCKETS, SITES } from "./engagementSites";

const LIMIT = 2500; // filters run over LIMIT × 4 candidates: covers 10k events

interface Ev {
  id: string;
  type: string;
  user: string;
  host: string;
  ip: string;
  country: string;
  countrySource: string;
  countryIp: string;
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
  visitor: string;
  referrer: string;
  ts: number;
}

interface Person {
  /** Grouping key: the identity, or the anonymous visitor id. */
  key: string;
  user: string;
  anonymous: boolean;
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
const refHost = (u: string) => {
  try {
    return new URL(u).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
};

function toEv(row: SqlRow): Ev {
  return {
    id: row.id,
    type: s(field(row, "type")),
    user: s(field(row, "user")) || "anonymous",
    host: s(field(row, "host")),
    ip: s(field(row, "ip")),
    country: s(field(row, "country")),
    countrySource: s(field(row, "country_source")),
    countryIp: s(field(row, "country_ip")),
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
    visitor: s(field(row, "visitor")),
    referrer: s(field(row, "referrer")),
    ts: n(field(row, "ts")) ?? 0,
  };
}

/** Top entries of a count map, most first. */
function top(counts: Map<string, number>, k = 8): [string, number][] {
  return [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, k);
}

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

/** How the visitor's country was worked out (njs/analytics.js countryOf). */
function countrySourceLabel(e: { countrySource: string; countryIp: string }): string {
  if (e.countrySource === "ip") return "from IP";
  if (e.countrySource === "timezone") return e.countryIp ? `from device time zone; IP says ${e.countryIp}` : "from device time zone";
  return "from browser settings";
}

export function EngagementTab() {
  const [events, setEvents] = useState<Ev[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [site, setSite] = useState(SITES[0].host);
  const [sel, setSel] = useState<string | null>(null);

  const load = useCallback(async () => {
    setBusy(true);
    setErr(null);
    try {
      const where = site ? `\n   AND host = ${lit(site)}` : "";
      const buckets = site ? [SITES.find((x) => x.host === site)?.bucket ?? "showcase_events"] : BUCKETS;
      const parts = await Promise.all(
        buckets.map(async (bucket) => {
          try {
            const r = await runSql(`SELECT * FROM ${bucket}\n WHERE semantic_match(text, ${lit("visitor activity on the site")})${where}\n LIMIT ${LIMIT}`);
            return r.rows.map(toEv);
          } catch (e) {
            // No events yet: a bucket is created by its first write.
            if (/not found|unknown bucket|no such bucket/i.test((e as Error).message)) return [];
            throw e;
          }
        })
      );
      setEvents(parts.flat().sort((a, b) => b.ts - a.ts));
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  }, [site]);

  useEffect(() => {
    void load();
  }, [load]);

  const people = useMemo<Person[]>(() => {
    // Anonymous pixel visitors are grouped by their browser's visitor id;
    // once that visitor is identified, their earlier events join the person.
    const named = new Map<string, string>();
    for (const e of events ?? []) if (e.user !== "anonymous" && e.visitor && !named.has(e.visitor)) named.set(e.visitor, e.user);
    const keyOf = (e: Ev) => (e.user !== "anonymous" ? e.user : e.visitor ? (named.get(e.visitor) ?? `visitor:${e.visitor}`) : "anonymous");
    const by = new Map<string, Ev[]>();
    for (const e of events ?? []) {
      const k = keyOf(e);
      by.set(k, [...(by.get(k) ?? []), e]);
    }
    return [...by.entries()]
      .map(([key, evs]) => {
        const anonymous = key === "anonymous" || key.startsWith("visitor:");
        const user = key.startsWith("visitor:") ? `Anonymous visitor ${key.slice(8, 14)}` : key;
        const videos = new Map<string, { pct: number; seconds: number }>();
        for (const e of evs) {
          if (!e.type.startsWith("video") || !e.video) continue;
          const v = videos.get(e.video) ?? { pct: 0, seconds: 0 };
          videos.set(e.video, { pct: Math.max(v.pct, e.pct ?? 0), seconds: Math.max(v.seconds, e.seconds ?? 0) });
        }
        return {
          key,
          user,
          anonymous,
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

  const person = people.find((p) => p.key === sel) ?? null;

  const summary = useMemo(() => {
    const pages = new Map<string, number>();
    const refs = new Map<string, number>();
    const names = new Map<string, string>();
    for (const e of events ?? []) {
      if (e.type !== "page_view") continue;
      const p = e.path || "/";
      pages.set(p, (pages.get(p) ?? 0) + 1);
      if (e.title && !names.has(p)) names.set(p, e.title);
      const h = refHost(e.referrer);
      if (e.referrer && h !== e.host.replace(/^www\./, "")) refs.set(h || "other", (refs.get(h || "other") ?? 0) + 1);
    }
    const anon = people.filter((p) => p.anonymous).length;
    return { pages: top(pages), names, refs: top(refs), anon, known: people.length - anon };
  }, [events, people]);

  return (
    <div className="space-y-4">
      <Panel
        title="Engagement"
        subtitle="Who opened the customer demo sites and the websites carrying the pixel, from where, what they watched and what they tried. IP and country are recorded by the server; pixel visitors stay anonymous until the site identifies them."
        action={
          <div className="flex items-center gap-2">
            <select className="input !w-auto !py-1.5 text-sm" value={site} onChange={(e) => setSite(e.target.value)} aria-label="Site">
              {SITES.map((x) => (
                <option key={x.host} value={x.host}>
                  {x.label} · {x.host}
                </option>
              ))}
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
                    key={p.key}
                    onClick={() => setSel(sel === p.key ? null : p.key)}
                    className={`cursor-pointer border-b border-gray-100 dark:border-edge align-top hover:bg-gray-50 dark:hover:bg-carbon-900 ${sel === p.key ? "bg-accent/5" : ""}`}
                  >
                    <td className={`py-2 pr-3 ${p.anonymous ? "text-gray-500 dark:text-muted" : "font-medium"}`}>{p.user}</td>
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

      {events && events.length > 0 && (
        <Panel title="Top pages and referrers" subtitle={`${summary.known} identified · ${summary.anon} anonymous visitors in the loaded events`}>
          <div className="grid gap-6 sm:grid-cols-2">
            <div>
              <h4 className="eyebrow mb-1">Top pages</h4>
              {summary.pages.length === 0 ? (
                <p className="text-sm text-gray-500 dark:text-muted">No page views.</p>
              ) : (
                <ol className="space-y-1 text-sm">
                  {summary.pages.map(([p, c]) => (
                    <li key={p} className="flex justify-between gap-3">
                      <span className="truncate" title={p}>
                        {summary.names.get(p) || p}
                        {summary.names.get(p) && <span className="ml-1 text-xs text-gray-500 dark:text-muted">{p}</span>}
                      </span>
                      <span className="tabular-nums">{c}</span>
                    </li>
                  ))}
                </ol>
              )}
            </div>
            <div>
              <h4 className="eyebrow mb-1">Referrers</h4>
              {summary.refs.length === 0 ? (
                <p className="text-sm text-gray-500 dark:text-muted">Direct visits only.</p>
              ) : (
                <ol className="space-y-1 text-sm">
                  {summary.refs.map(([h, c]) => (
                    <li key={h} className="flex justify-between gap-3">
                      <span className="truncate">{h}</span>
                      <span className="tabular-nums">{c}</span>
                    </li>
                  ))}
                </ol>
              )}
            </div>
          </div>
        </Panel>
      )}

      {person && (
        <Panel title={person.user} subtitle={`First seen ${when(person.first)} · last seen ${when(person.last)} · ${person.events.length} events`}>
          <dl className="grid gap-x-6 gap-y-1 text-sm sm:grid-cols-2">
            <div>
              <dt className="eyebrow">Country</dt>
              <dd>
                {person.countries.map((c) => `${flag(c)} ${c}`).join(", ") || "unknown"}
                {person.events[0]?.countrySource && (
                  <span className="text-xs text-gray-500 dark:text-muted"> ({countrySourceLabel(person.events[0])})</span>
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
