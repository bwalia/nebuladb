import { useCallback, useEffect, useMemo, useState } from "react";
import { api, ApiError, type SqlRow } from "../api";
import { ErrorBanner, Panel, Spectrum, Spinner, Stat } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ensureSeeded } from "../rag/seed";
import { field, lit, minMax, runSql, semanticText, similarity } from "../rag/sql";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { GroundedChat, type ContextDoc } from "../rag/GroundedChat";
import {
  INCIDENTS,
  INCIDENT_BUCKET,
  ROOT_CAUSES,
  RUNBOOKS,
  RUNBOOK_BUCKET,
  SAMPLE_ALERTS,
  SEED_VERSION,
  SERVICES,
  SIGNALS,
  type RootCause,
} from "../rag/data/incidents";
import { ArchitecturePanel } from "../components/RagArchitecture";

interface Incident {
  id: string;
  text: string;
  sim: number;
  title: string;
  service: string;
  severity: number;
  startedAt: string;
  mttr: number;
  rootCause: string;
  fix: string;
  signals: string[];
}

interface Runbook {
  id: string;
  text: string;
  sim: number;
  title: string;
  service: string;
  risk: string;
}

interface Triage {
  causes: Array<{ cause: string; share: number; support: string[] }>;
  mttrMinutes: number | null;
  services: string[];
  teams: string[];
  deploySuspected: boolean;
}

const LOOKBACK = [
  { label: "any time", months: 0 },
  { label: "last 3 months", months: 3 },
  { label: "last 6 months", months: 6 },
  { label: "last 12 months", months: 12 },
  { label: "last 24 months", months: 24 },
];

const SEVERITY = [
  { label: "all severities", max: 3 },
  { label: "SEV1–2", max: 2 },
  { label: "SEV1 only", max: 1 },
];

const SYSTEM = `You are an incident-commander assistant for an SRE team at a UK fintech.
Source [0] is the LIVE alert under investigation. Other sources are past incident postmortems (INC-...) and runbooks (RB-...).
Rules:
- Rank up to 3 root-cause hypotheses, most likely first. For each, cite the past incidents [n] that support it and what matches the alert.
- Recommend diagnostic or mitigation commands ONLY if they appear in a runbook source, and cite that runbook [n]. Never invent commands.
- Any step from a runbook marked "risk: destructive" (or a step labelled DESTRUCTIVE) must be flagged "⚠ DESTRUCTIVE — needs IC approval".
- If the evidence is weak or sources disagree, say so plainly instead of guessing.
- Be brief: short bullet points, no preamble.`;

const DEPLOY_RE = /\b(deploy(ed|ment)?|release[ds]?|rollout|rolled out|promoted|canary|upgrade[ds]?|v\d+\.\d+(\.\d+)?)\b/i;

function monthsAgo(n: number): string {
  const d = new Date();
  d.setMonth(d.getMonth() - n);
  return d.toISOString().slice(0, 10);
}

function toIncident(r: SqlRow): Incident {
  return {
    id: r.id,
    text: field<string>(r, "text") ?? "",
    sim: similarity(r.score),
    title: field<string>(r, "title") ?? r.id,
    service: field<string>(r, "service") ?? "unknown",
    severity: Number(field(r, "severity") ?? 3),
    startedAt: field<string>(r, "started_at") ?? "",
    mttr: Number(field(r, "mttr_minutes") ?? 0),
    rootCause: field<string>(r, "root_cause") ?? "unknown",
    fix: field<string>(r, "fix") ?? "",
    signals: field<string[]>(r, "signals") ?? [],
  };
}

function toRunbook(r: SqlRow): Runbook {
  return {
    id: r.id,
    text: field<string>(r, "text") ?? "",
    sim: similarity(r.score),
    title: field<string>(r, "title") ?? r.id,
    service: field<string>(r, "service") ?? "",
    risk: field<string>(r, "risk") ?? "caution",
  };
}

/** Similarity-weighted root-cause vote, MTTR and blast radius over the top matches. */
function triage(alert: string, incidents: Incident[]): Triage {
  const top = incidents.slice(0, 8);
  const votes = new Map<string, { w: number; support: string[] }>();
  let total = 0;
  for (const inc of top) {
    // Squaring sharpens the vote toward the closest matches.
    const w = inc.sim * inc.sim;
    total += w;
    const v = votes.get(inc.rootCause) ?? { w: 0, support: [] };
    v.w += w;
    v.support.push(inc.id);
    votes.set(inc.rootCause, v);
  }
  const causes = [...votes.entries()]
    .map(([cause, v]) => ({ cause, share: total > 0 ? v.w / total : 0, support: v.support }))
    .sort((a, b) => b.share - a.share);

  // Weighted median of MTTR across the top 5.
  const five = incidents.slice(0, 5).slice().sort((a, b) => a.mttr - b.mttr);
  const wsum = five.reduce((n, i) => n + i.sim, 0);
  let acc = 0;
  let mttrMinutes: number | null = null;
  for (const i of five) {
    acc += i.sim;
    if (acc >= wsum / 2) {
      mttrMinutes = i.mttr;
      break;
    }
  }

  const top5 = incidents.slice(0, 5);
  return {
    causes,
    mttrMinutes,
    services: [...new Set(top5.map((i) => i.service))],
    teams: [...new Set(top5.map((i) => (i.text.match(/team ([\w-]+)/)?.[1] ?? "")).filter(Boolean))],
    deploySuspected: DEPLOY_RE.test(alert),
  };
}

function compact(s: string, n = 1200) {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

export function IncidentCopilotTab() {
  const [seedMsg, setSeedMsg] = useState<string>("Checking demo corpus…");
  const [seeded, setSeeded] = useState(false);
  const [alert, setAlert] = useState(SAMPLE_ALERTS[0].body);
  const [service, setService] = useState("");
  const [sevMax, setSevMax] = useState(3);
  const [lookback, setLookback] = useState(0);
  const [signal, setSignal] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [runbooks, setRunbooks] = useState<Runbook[]>([]);
  const [tri, setTri] = useState<Triage | null>(null);
  const [analysisId, setAnalysisId] = useState(0);
  const [analysedAlert, setAnalysedAlert] = useState("");
  const [expanded, setExpanded] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const a = await ensureSeeded(INCIDENT_BUCKET, INCIDENTS, SEED_VERSION, (d, t) =>
          !cancelled && setSeedMsg(`Seeding incidents ${d}/${t}…`)
        );
        const b = await ensureSeeded(RUNBOOK_BUCKET, RUNBOOKS, SEED_VERSION, (d, t) =>
          !cancelled && setSeedMsg(`Seeding runbooks ${d}/${t}…`)
        );
        if (cancelled) return;
        setSeeded(true);
        setSeedMsg(
          a.written + b.written > 0
            ? `Seeded ${a.written} incidents and ${b.written} runbooks.`
            : `${a.total} postmortems + ${b.total} runbooks ready.`
        );
      } catch (e) {
        if (!cancelled) setSeedMsg(`Seeding failed: ${(e as Error).message}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const analyse = async () => {
    const text = alert.trim();
    if (!text) return;
    setErr(null);
    setBusy(true);
    setExpanded(null);
    const q = lit(semanticText(text));
    const preds: string[] = [];
    if (service) preds.push(`service = ${lit(service)}`);
    if (sevMax < 3) preds.push(`severity <= ${lit(sevMax)}`);
    if (lookback > 0) preds.push(`started_at >= ${lit(monthsAgo(lookback))}`);
    if (signal) preds.push(`array_contains(signals, ${lit(signal)})`);
    const incSql =
      `SELECT * FROM ${INCIDENT_BUCKET}\nWHERE semantic_match(text, ${q})` +
      preds.map((p) => `\n  AND ${p}`).join("") +
      `\nLIMIT 30`;
    const trace: PipelineStep[] = [
      { label: "Similar past incidents (SQL + vector)", detail: incSql, status: "running" },
    ];
    setSteps([...trace]);
    try {
      const inc = await runSql(incSql);
      const incs = inc.rows.map(toIncident).sort((a, b) => b.sim - a.sim);
      trace[0] = { ...trace[0], rowsOut: incs.length, ms: inc.tookMs, status: "ok" };

      let rbSql =
        `SELECT * FROM ${RUNBOOK_BUCKET}\nWHERE semantic_match(text, ${q})` +
        (service ? `\n  AND service = ${lit(service)}` : "") +
        `\nLIMIT 5`;
      trace.push({ label: "Relevant runbooks", detail: rbSql, status: "running" });
      setSteps([...trace]);
      let rb = await runSql(rbSql);
      let rbNote = "";
      if (rb.rows.length === 0 && service) {
        // No runbook for this service — fall back to every runbook.
        rbSql = `SELECT * FROM ${RUNBOOK_BUCKET}\nWHERE semantic_match(text, ${q})\nLIMIT 5`;
        rb = await runSql(rbSql);
        rbNote = `\n-- no runbook for service '${service}', widened to all services`;
      }
      const rbs = rb.rows.map(toRunbook).sort((a, b) => b.sim - a.sim);
      trace[1] = { ...trace[1], detail: rbSql + rbNote, rowsOut: rbs.length, ms: rb.tookMs, status: "ok" };

      const t0 = performance.now();
      const t = triage(text, incs);
      trace.push({
        label: "Triage (in browser)",
        detail:
          "root cause = Σ similarity² vote over top 8 · MTTR = similarity-weighted median of top 5\n" +
          `deploy suspected = alert matches ${DEPLOY_RE.source.slice(0, 48)}…`,
        rowsIn: Math.min(8, incs.length),
        rowsOut: t.causes.length,
        ms: Math.round(performance.now() - t0),
        status: "ok",
      });
      setSteps([...trace]);
      setIncidents(incs);
      setRunbooks(rbs);
      setTri(t);
      setAnalysedAlert(text);
      setAnalysisId((n) => n + 1);
    } catch (e) {
      const msg = e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message;
      setErr(msg);
      setSteps(trace.map((s) => (s.status === "running" ? { ...s, status: "error" } : s)));
    } finally {
      setBusy(false);
    }
  };

  const getContext = useCallback(async (): Promise<ContextDoc[]> => {
    const docs: ContextDoc[] = [
      {
        id: "alert",
        label: "Current alert",
        text: `LIVE ALERT under investigation (not a past incident):\n${compact(analysedAlert)}`,
      },
    ];
    for (const i of incidents.slice(0, 5)) {
      docs.push({ id: i.id, label: i.id, bucket: INCIDENT_BUCKET, text: compact(i.text) });
    }
    for (const r of runbooks.slice(0, 3)) {
      docs.push({ id: r.id, label: r.title, bucket: RUNBOOK_BUCKET, text: compact(r.text) });
    }
    return docs;
  }, [analysedAlert, incidents, runbooks]);

  const norm = useMemo(() => minMax(incidents.map((i) => i.sim)), [incidents]);
  const analysed = analysisId > 0;

  return (
    <div className="space-y-4">
      <ArchitecturePanel id="incidents" />
      <Panel
        title="Incident Copilot"
        subtitle="Paste an alert → find similar past incidents and runbooks → triage with evidence → chat as incident commander"
        action={
          <span className="metachip">
            <span className={`dot ${seeded ? "dot-ok" : seedMsg.startsWith("Seeding failed") ? "dot-bad" : "dot-warn animate-pulseok"}`} />
            {seedMsg}
          </span>
        }
      >
        <div className="flex flex-wrap gap-1.5">
          <span className="eyebrow self-center mr-1">Sample alerts</span>
          {SAMPLE_ALERTS.map((a) => (
            <button
              key={a.title}
              type="button"
              onClick={() => setAlert(a.body)}
              className={`rounded-full border px-2.5 py-1 text-xs transition-colors ${
                alert === a.body
                  ? "border-accent text-accent bg-accent/10"
                  : "border-gray-200 dark:border-edge text-gray-600 dark:text-muted hover:border-accent/60"
              }`}
            >
              {a.title}
            </button>
          ))}
        </div>
        <label className="block">
          <span className="block text-xs font-medium mb-1">Alert / symptoms</span>
          <textarea
            className="code-input w-full h-32"
            value={alert}
            onChange={(e) => setAlert(e.target.value)}
            placeholder="Paste the alert payload, error messages, or what users are seeing…"
          />
        </label>
        <div className="flex flex-wrap gap-3 items-end">
          <Select label="Service" value={service} onChange={setService}
            options={[["", "any service"], ...SERVICES.map((s) => [s, s] as [string, string])]} />
          <Select label="Severity" value={String(sevMax)} onChange={(v) => setSevMax(Number(v))}
            options={SEVERITY.map((s) => [String(s.max), s.label] as [string, string])} />
          <Select label="Lookback" value={String(lookback)} onChange={(v) => setLookback(Number(v))}
            options={LOOKBACK.map((l) => [String(l.months), l.label] as [string, string])} />
          <Select label="Signal" value={signal} onChange={setSignal}
            options={[["", "any signal"], ...SIGNALS.map((s) => [s, s] as [string, string])]} />
          <button className="btn" disabled={busy || !seeded || !alert.trim()} onClick={() => void analyse()}>
            {busy ? <Spinner label="analysing…" /> : "Analyse alert"}
          </button>
        </div>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {analysed && tri && (
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.3fr)]">
          <Panel title="Triage" subtitle="Deterministic — computed from the matched incidents, before any LLM">
            {incidents.length === 0 ? (
              <p className="text-sm text-gray-500 dark:text-muted">
                No similar incidents under these filters. Try widening the lookback, severity or service filter.
              </p>
            ) : (
              <>
                <div className="flex flex-wrap gap-4">
                  <Stat label="expected MTTR" value={tri.mttrMinutes !== null ? `${tri.mttrMinutes} min` : "—"} />
                  <Stat label="matches" value={incidents.length} />
                  <Stat label="top similarity" value={incidents[0].sim.toFixed(3)} />
                </div>
                {tri.deploySuspected && (
                  <div className="rounded-md border border-warn/40 bg-warn/10 px-3 py-2 text-xs text-gray-800 dark:text-ink">
                    <span className="font-semibold text-warn">Recent change suspected</span> — the alert mentions a
                    deploy, release or version. Check the rollout history first; rollback is usually the fastest mitigation.
                  </div>
                )}
                <div>
                  <div className="eyebrow mb-1.5">Likely root causes</div>
                  <ul className="space-y-1.5">
                    {tri.causes.map((c) => (
                      <li key={c.cause} className="text-xs">
                        <div className="flex items-center gap-2">
                          <span className="w-24 font-mono text-gray-700 dark:text-ink">{c.cause}</span>
                          <div className="flex-1 h-2 rounded-full bg-gray-100 dark:bg-carbon-800 overflow-hidden">
                            <div className="h-full bg-accent" style={{ width: `${Math.max(3, c.share * 100)}%` }} />
                          </div>
                          <span className="w-10 text-right font-mono tabular-nums">{Math.round(c.share * 100)}%</span>
                        </div>
                        <div className="pl-[6.5rem] text-[11px] text-gray-400 dark:text-faint font-mono">
                          {c.support.join(", ")}
                        </div>
                      </li>
                    ))}
                  </ul>
                </div>
                <div className="text-xs text-gray-600 dark:text-muted">
                  <span className="eyebrow mr-2">Blast radius</span>
                  similar incidents touched <b>{tri.services.join(", ")}</b>
                  {tri.teams.length > 0 && <> · owners: {tri.teams.join(", ")}</>}
                </div>
              </>
            )}
          </Panel>

          <GroundedChat
            title="Incident commander chat"
            subtitle="Grounded on the alert [0], the top 5 incidents and top 3 runbooks"
            system={SYSTEM}
            getContext={getContext}
            resetKey={String(analysisId)}
            disabledReason={incidents.length === 0 && runbooks.length === 0 ? "No evidence retrieved — widen the filters" : undefined}
            suggestions={[
              "What's the most likely root cause?",
              "First 3 diagnostic steps?",
              "Should we roll back?",
              "Draft a status page update",
            ]}
            onCite={(d) => setExpanded(d.id)}
          />
        </div>
      )}

      {analysed && incidents.length > 0 && (
        <Panel title="Similar past incidents" subtitle={`${incidents.length} matches, most similar first`}>
          <ul className="space-y-2">
            {incidents.slice(0, 10).map((i) => (
              <li key={i.id} className={`rounded-md border px-3 py-2 ${expanded === i.id ? "border-accent" : "border-gray-200 dark:border-edge"}`}>
                <div className="flex items-center gap-3">
                  <SevBadge sev={i.severity} />
                  <button className="text-left flex-1 min-w-0" onClick={() => setExpanded(expanded === i.id ? null : i.id)}>
                    <div className="text-sm font-medium text-gray-900 dark:text-ink truncate">
                      <span className="font-mono text-xs text-gray-400 dark:text-faint mr-2">{i.id}</span>
                      {i.title}
                    </div>
                    <div className="text-xs text-gray-500 dark:text-muted">
                      {i.service} · {i.startedAt} · MTTR {i.mttr} min · cause <b>{i.rootCause}</b> · fix: {i.fix}
                    </div>
                  </button>
                  <Spectrum value={norm(i.sim)} label={i.sim.toFixed(3)} />
                </div>
                {expanded === i.id && (
                  <pre className="mt-2 text-xs whitespace-pre-wrap font-mono text-gray-600 dark:text-muted">{i.text}</pre>
                )}
              </li>
            ))}
          </ul>
        </Panel>
      )}

      {analysed && runbooks.length > 0 && (
        <Panel title="Runbooks" subtitle="Only these commands may be suggested in chat">
          <ul className="space-y-2">
            {runbooks.map((r) => (
              <li key={r.id} className={`rounded-md border px-3 py-2 ${expanded === r.id ? "border-accent" : "border-gray-200 dark:border-edge"}`}>
                <button className="w-full text-left flex items-center gap-3" onClick={() => setExpanded(expanded === r.id ? null : r.id)}>
                  <RiskBadge risk={r.risk} />
                  <span className="text-sm font-medium text-gray-900 dark:text-ink flex-1">{r.title}</span>
                  <span className="text-xs font-mono text-gray-400 dark:text-faint">{r.service} · {r.sim.toFixed(3)}</span>
                </button>
                {expanded === r.id && (
                  <pre className="mt-2 text-xs whitespace-pre-wrap font-mono text-gray-600 dark:text-muted">{r.text}</pre>
                )}
              </li>
            ))}
          </ul>
        </Panel>
      )}

      {analysed && (
        <RecordIncident
          alert={analysedAlert}
          defaultService={service || incidents[0]?.service || "unknown"}
          defaultCause={(tri?.causes[0]?.cause as RootCause) ?? "deploy"}
          signal={signal}
        />
      )}

      <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "/api/v1/bucket/sre_"]} />
    </div>
  );
}

function RecordIncident({
  alert,
  defaultService,
  defaultCause,
  signal,
}: {
  alert: string;
  defaultService: string;
  defaultCause: RootCause;
  signal: string;
}) {
  const [cause, setCause] = useState<RootCause>(defaultCause);
  const [fix, setFix] = useState("");
  const [mttr, setMttr] = useState(30);
  const [sev, setSev] = useState(2);
  const [busy, setBusy] = useState(false);
  const [done, setDone] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    setCause(defaultCause);
    setDone(null);
  }, [alert, defaultCause]);

  const save = async () => {
    setBusy(true);
    setErr(null);
    const today = new Date().toISOString().slice(0, 10);
    const id = `INC-${today.replace(/-/g, "")}-${Math.random().toString(36).slice(2, 6).toUpperCase()}`;
    const title = alert.split("\n")[0].replace(/^\[[^\]]*\]\s*/, "").slice(0, 120);
    const text = [
      `${id} · ${title}`,
      `Service: ${defaultService} · SEV${sev} · ${today} · MTTR ${mttr} min · team on-call`,
      `Alerts seen: ${alert}`,
      `Root cause (${cause}): recorded from Incident Copilot.`,
      `Resolution: ${fix}.`,
    ].join("\n");
    try {
      await api.upsertDoc(INCIDENT_BUCKET, id, text, {
        title,
        service: defaultService,
        severity: sev,
        started_at: today,
        mttr_minutes: mttr,
        root_cause: cause,
        signals: signal ? [signal] : [],
        fix,
        team: "on-call",
        source: "copilot",
      });
      setDone(id);
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Panel title="Record as incident" subtitle="Close the loop: resolved incidents become retrievable evidence for the next alert">
      <div className="flex flex-wrap gap-3 items-end">
        <Select label="Root cause" value={cause} onChange={(v) => setCause(v as RootCause)}
          options={ROOT_CAUSES.map((c) => [c, c] as [string, string])} />
        <Select label="Severity" value={String(sev)} onChange={(v) => setSev(Number(v))}
          options={[["1", "SEV1"], ["2", "SEV2"], ["3", "SEV3"]]} />
        <label className="block">
          <span className="block text-xs font-medium mb-1">MTTR (min)</span>
          <input type="number" min={1} className="input !w-24" value={mttr}
            onChange={(e) => setMttr(Math.max(1, Number(e.target.value) || 1))} />
        </label>
        <label className="block flex-1 min-w-[14rem]">
          <span className="block text-xs font-medium mb-1">Fix applied</span>
          <input className="input" value={fix} onChange={(e) => setFix(e.target.value)}
            placeholder="e.g. Rolled back payments-api to v4.18.2" />
        </label>
        <button className="btn" disabled={busy || !fix.trim()} onClick={() => void save()}>
          {busy ? <Spinner label="saving…" /> : "Record"}
        </button>
      </div>
      <ErrorBanner err={err} />
      {done && (
        <p className="text-xs text-gray-600 dark:text-muted">
          <span className="dot dot-ok mr-1.5" />
          Saved <span className="font-mono">{done}</span> to <span className="font-mono">{INCIDENT_BUCKET}</span> (service {defaultService}).
          Re-run the analysis and it will appear as a match.
        </p>
      )}
    </Panel>
  );
}

function Select({
  label,
  value,
  onChange,
  options,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  options: Array<[string, string]>;
}) {
  return (
    <label className="block">
      <span className="block text-xs font-medium mb-1">{label}</span>
      <select className="input !w-auto" value={value} onChange={(e) => onChange(e.target.value)}>
        {options.map(([v, l]) => (
          <option key={v} value={v}>{l}</option>
        ))}
      </select>
    </label>
  );
}

function SevBadge({ sev }: { sev: number }) {
  const cls =
    sev === 1
      ? "border-bad/50 text-bad bg-bad/10"
      : sev === 2
        ? "border-warn/50 text-warn bg-warn/10"
        : "border-gray-300 text-gray-500 dark:border-edge dark:text-muted";
  return <span className={`shrink-0 rounded border px-1.5 py-0.5 font-mono text-[10px] ${cls}`}>SEV{sev}</span>;
}

function RiskBadge({ risk }: { risk: string }) {
  const cls =
    risk === "destructive"
      ? "border-bad/50 text-bad bg-bad/10"
      : risk === "caution"
        ? "border-warn/50 text-warn bg-warn/10"
        : "border-ok/50 text-ok bg-ok/10";
  return <span className={`shrink-0 rounded border px-1.5 py-0.5 font-mono text-[10px] uppercase ${cls}`}>{risk}</span>;
}
