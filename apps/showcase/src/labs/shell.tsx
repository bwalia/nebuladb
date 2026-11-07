/**
 * Shared chrome for every lab: environment detection (embedder / LLM /
 * mock), pack seeding, the lab card (method, limits, challenge) and the
 * small pickers the labs reuse.
 */
import { useEffect, useState, type ReactNode } from "react";
import { api, type AiModelInfo, type Health } from "../api";
import { ensureSeeded } from "../rag/seed";
import { UnderTheHood } from "../components/UnderTheHood";
import { Spinner } from "../components";
import type { Pack, Persona } from "./types";
import type { LabMeta } from "./meta";
import { PACKS } from "./packs";
import type { LabState } from "./engine/core";

// ------------------------------------------------------------ environment

export interface LabEnv {
  health: Health | null;
  embedModel: string;
  embedDim: number;
  mockEmbedder: boolean;
  models: AiModelInfo[];
  defaultProvider: string;
  mockLlmOnly: boolean;
  error: string | null;
}

let envPromise: Promise<LabEnv> | null = null;

async function loadEnv(): Promise<LabEnv> {
  const [h, cfg, ms] = await Promise.allSettled([api.health(), api.aiConfig(), api.aiModels()]);
  const health = h.status === "fulfilled" ? h.value : null;
  const config = cfg.status === "fulfilled" ? cfg.value : null;
  const models = ms.status === "fulfilled" ? ms.value.models.filter((m) => m.capabilities?.chat !== false) : [];
  const embedModel = config?.embedding.model ?? health?.model ?? "unknown";
  return {
    health,
    embedModel,
    embedDim: config?.embedding.dim ?? health?.dim ?? 0,
    mockEmbedder: /mock/i.test(health?.model ?? embedModel),
    models,
    defaultProvider: config?.default_provider ?? (ms.status === "fulfilled" ? ms.value.default_provider : ""),
    mockLlmOnly: config?.dev_mock_only ?? models.every((m) => m.is_mock),
    error: h.status === "rejected" ? String((h.reason as Error)?.message ?? h.reason) : null,
  };
}

export function useLabEnv(): LabEnv | null {
  const [env, setEnv] = useState<LabEnv | null>(null);
  useEffect(() => {
    envPromise ??= loadEnv();
    let live = true;
    envPromise.then((e) => live && setEnv(e));
    return () => {
      live = false;
    };
  }, []);
  return env;
}

// ------------------------------------------------------------ seeding

export interface SeedState {
  status: "seeding" | "ready" | "error";
  done: number;
  total: number;
  written: number;
  error?: string;
}

export function usePackSeed(pack: Pack): SeedState {
  const [s, setS] = useState<SeedState>({ status: "seeding", done: 0, total: pack.docs.length, written: 0 });
  useEffect(() => {
    let live = true;
    setS({ status: "seeding", done: 0, total: pack.docs.length, written: 0 });
    ensureSeeded(pack.bucket, pack.docs, `lab-${pack.version}`, (done, total) => live && setS((p) => ({ ...p, done, total })))
      .then((r) => live && setS({ status: "ready", done: r.total, total: r.total, written: r.written }))
      .catch((e: Error) => live && setS((p) => ({ ...p, status: "error", error: e.message })));
    return () => {
      live = false;
    };
  }, [pack]);
  return s;
}

// ------------------------------------------------------------ challenge tick

function useDone(key: string): [boolean, (v: boolean) => void] {
  const [done, setDone] = useState(() => {
    try {
      return localStorage.getItem(key) === "1";
    } catch {
      return false;
    }
  });
  useEffect(() => {
    try {
      setDone(localStorage.getItem(key) === "1");
    } catch {
      setDone(false);
    }
  }, [key]);
  return [
    done,
    (v) => {
      setDone(v);
      try {
        localStorage.setItem(key, v ? "1" : "0");
      } catch {
        /* private mode: tick lasts for the session */
      }
    },
  ];
}

// ------------------------------------------------------------ frame

export function LabFrame({
  meta,
  pack,
  state,
  update,
  env,
  seed,
  children,
}: {
  meta: LabMeta;
  pack: Pack;
  state: LabState;
  update: (p: Partial<LabState>) => void;
  env: LabEnv | null;
  seed: SeedState;
  children: ReactNode;
}) {
  const [done, setDone] = useDone(`nebula-lab-${meta.id}-${pack.id}`);
  const [copied, setCopied] = useState(false);
  return (
    <div className="space-y-4">
      <div className="card space-y-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <a href={`#/labs?pack=${pack.id}`} className="eyebrow hover:text-accent">
              ← All labs
            </a>
            <h3 className="mt-1 font-display text-xl font-semibold text-black dark:text-ink">{meta.title}</h3>
            <p className="mt-1 max-w-3xl text-sm text-gray-600 dark:text-muted">{meta.lead}</p>
          </div>
          <div className="flex items-center gap-2">
            <PackPicker value={pack.id} onChange={(id) => update({ pack: id, q: undefined, f: undefined, persona: undefined })} />
            <button
              className="btn-secondary"
              title="Copy a link to exactly this view"
              onClick={() => {
                void navigator.clipboard?.writeText(window.location.href).then(() => {
                  setCopied(true);
                  setTimeout(() => setCopied(false), 1500);
                });
              }}
            >
              {copied ? "Copied" : "Share"}
            </button>
          </div>
        </div>

        <div className="grid gap-3 md:grid-cols-3">
          <InfoBlock title="How it works">{meta.method}</InfoBlock>
          <InfoBlock title="What it doesn't show">{meta.limits}</InfoBlock>
          <div className="rounded-lg border border-accent/30 bg-accent/5 p-3">
            <div className="eyebrow text-accent">Challenge</div>
            <p className="mt-1 text-sm text-gray-700 dark:text-ink">{pack.challenges[meta.id]}</p>
            <label className="mt-2 inline-flex items-center gap-2 text-xs text-gray-500 dark:text-muted">
              <input type="checkbox" checked={done} onChange={(e) => setDone(e.target.checked)} />
              {done ? "Done" : "Mark as done"}
            </label>
          </div>
        </div>

        <DataBadge pack={pack} env={env} seed={seed} needs={meta.needs} />
      </div>

      {seed.status === "error" ? (
        <div className="card text-sm text-bad">Couldn't load the {pack.name} data into NebulaDB: {seed.error}</div>
      ) : seed.status === "seeding" ? (
        <div className="card">
          <Spinner label={`Loading ${pack.name} into NebulaDB… ${seed.done}/${seed.total}`} />
        </div>
      ) : (
        children
      )}

      <UnderTheHood label="Show the API calls behind this lab" filter={["/api/v1/"]} />
      <p className="sr-only" aria-live="polite">
        {state.q ? `Question: ${state.q}` : ""}
      </p>
    </div>
  );
}

function InfoBlock({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="rounded-lg border border-gray-200 dark:border-edge p-3">
      <div className="eyebrow">{title}</div>
      <p className="mt-1 text-xs leading-relaxed text-gray-600 dark:text-muted">{children}</p>
    </div>
  );
}

export function DataBadge({ pack, env, seed, needs }: { pack: Pack; env: LabEnv | null; seed: SeedState; needs: LabMeta["needs"] }) {
  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-gray-500 dark:text-muted">
        <span>
          <span className="eyebrow mr-1.5">Data</span>
          {pack.name} · {pack.docs.length} demo docs in <code>{pack.bucket}</code>
          {seed.status === "ready" && ` (${seed.written} written this visit)`}
        </span>
        {env && (
          <span>
            <span className="eyebrow mr-1.5">Embedder</span>
            {env.embedModel} · {env.embedDim}-d
          </span>
        )}
        {env && (
          <span>
            <span className="eyebrow mr-1.5">LLM</span>
            {env.models.length ? env.models.map((m) => m.id).join(", ") : "none configured"}
          </span>
        )}
        <span className="text-gray-400 dark:text-faint">All people, places and records are invented.</span>
      </div>
      {env?.mockEmbedder && needs.includes("embedder") && (
        <Banner kind="warn">
          <b>Mock embedder.</b> This server embeds with <code>{env.embedModel}</code>, a hash rather than a language model, so vector
          scores here are noise. Filters, keyword search and the in-browser model are still real.
        </Banner>
      )}
      {env?.mockLlmOnly && needs.includes("llm") && (
        <Banner kind="warn">
          <b>Mock LLM.</b> Only the offline mock model is configured, so generated text is a placeholder. Retrieval, citations
          checks and the reference plans still run for real.
        </Banner>
      )}
      {env?.error && <Banner kind="bad">NebulaDB is unreachable: {env.error}</Banner>}
    </div>
  );
}

export function Banner({ kind, children }: { kind: "warn" | "bad" | "info"; children: ReactNode }) {
  const cls =
    kind === "bad"
      ? "border-bad/40 bg-bad/10"
      : kind === "warn"
        ? "border-warn/40 bg-warn/10"
        : "border-accent/30 bg-accent/5";
  return <div className={`rounded-md border px-3 py-2 text-xs text-gray-700 dark:text-muted ${cls}`}>{children}</div>;
}

// ------------------------------------------------------------ pickers

export function PackPicker({ value, onChange }: { value: string; onChange: (id: string) => void }) {
  return (
    <label className="inline-flex items-center gap-2 text-xs text-gray-500 dark:text-muted">
      <span className="eyebrow">Business</span>
      <select className="input !w-auto !py-1.5" value={value} onChange={(e) => onChange(e.target.value)}>
        {PACKS.map((p) => (
          <option key={p.id} value={p.id}>
            {p.name} — {p.industry}
          </option>
        ))}
      </select>
    </label>
  );
}

export function PersonaPicker({ pack, value, onChange }: { pack: Pack; value?: string; onChange: (id?: string) => void }) {
  return (
    <label className="inline-flex items-center gap-2 text-xs text-gray-500 dark:text-muted">
      <span className="eyebrow">Asking as</span>
      <select className="input !w-auto !py-1.5" value={value ?? ""} onChange={(e) => onChange(e.target.value || undefined)}>
        <option value="">Admin (sees everything)</option>
        {pack.personas.map((p) => (
          <option key={p.id} value={p.id}>
            {p.label}
          </option>
        ))}
      </select>
    </label>
  );
}

export function personaOf(pack: Pack, id?: string): Persona | undefined {
  return pack.personas.find((p) => p.id === id);
}

export function ModelPicker({
  env,
  value,
  onChange,
  label = "Model",
}: {
  env: LabEnv | null;
  value?: string;
  onChange: (provider: string) => void;
  label?: string;
}) {
  const models = env?.models ?? [];
  return (
    <label className="inline-flex items-center gap-2 text-xs text-gray-500 dark:text-muted">
      <span className="eyebrow">{label}</span>
      <select className="input !w-auto !py-1.5" value={value ?? env?.defaultProvider ?? ""} onChange={(e) => onChange(e.target.value)}>
        {models.length === 0 && <option value="">none configured</option>}
        {models.map((m) => (
          <option key={m.provider} value={m.provider}>
            {m.display_name || m.id} ({m.provider}){m.is_mock ? " — mock" : ""}
          </option>
        ))}
      </select>
    </label>
  );
}

/** Question box with the pack's sample questions as one-click chips. */
export function QuestionBox({
  value,
  onSubmit,
  samples,
  busy,
  cta = "Run",
}: {
  value: string;
  onSubmit: (q: string) => void;
  samples: string[];
  busy?: boolean;
  cta?: string;
}) {
  const [q, setQ] = useState(value);
  useEffect(() => setQ(value), [value]);
  return (
    <div className="space-y-2">
      <form
        className="flex gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          if (q.trim()) onSubmit(q.trim());
        }}
      >
        <input className="input" value={q} onChange={(e) => setQ(e.target.value)} aria-label="Question" />
        <button className="btn" disabled={busy || !q.trim()}>
          {busy ? "Running…" : cta}
        </button>
      </form>
      <div className="flex flex-wrap gap-1.5">
        {samples.slice(0, 6).map((s) => (
          <button
            key={s}
            type="button"
            onClick={() => {
              setQ(s);
              onSubmit(s);
            }}
            className="rounded-full border border-gray-200 dark:border-edge px-2.5 py-0.5 text-[11px] text-gray-600 dark:text-muted hover:border-accent hover:text-accent"
          >
            {s.length > 70 ? s.slice(0, 68) + "…" : s}
          </button>
        ))}
      </div>
    </div>
  );
}

/** Horizontal 0–1 bar with an honest numeric label. */
export function Bar({ value, label, tone = "accent" }: { value: number; label: string; tone?: "accent" | "ok" | "warn" | "faint" }) {
  const pct = Math.round(Math.max(0, Math.min(1, value)) * 100);
  const bg = tone === "ok" ? "bg-ok" : tone === "warn" ? "bg-warn" : tone === "faint" ? "bg-gray-300 dark:bg-faint" : "bg-accent";
  return (
    <div className="flex items-center gap-2">
      <div className="h-1.5 flex-1 rounded-full bg-gray-100 dark:bg-carbon-800 overflow-hidden" role="img" aria-label={label}>
        <div className={`h-full ${bg}`} style={{ width: `${Math.max(2, pct)}%` }} />
      </div>
      <span className="w-14 text-right font-mono text-[11px] tabular-nums text-gray-600 dark:text-muted">{label}</span>
    </div>
  );
}

export function fmtPct(x: number): string {
  return `${Math.round(x * 100)}%`;
}
