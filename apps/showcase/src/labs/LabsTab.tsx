/**
 * NebulaDB Labs — focused 2–5 minute experiments that teach one
 * retrieval or reasoning idea on a business's own data, against real
 * NebulaDB. Routing is hash-based so every view is shareable:
 *
 *   #/labs?pack=kisaan                     the lab grid
 *   #/labs/rag?pack=carehome&q=…&k=5       one lab, exact state
 */
import { useCallback, useEffect, useState, type ComponentType } from "react";
import { labHash, parseLabHash, type LabState } from "./engine/core";
import { LABS, LAB_BY_ID, START_PATH, type Concept } from "./meta";
import { PACKS, getPack } from "./packs";
import { LabFrame, PackPicker, useLabEnv, usePackSeed } from "./shell";
import type { LabId } from "./types";
import type { LabProps } from "./labs/props";
import { SemanticLab } from "./labs/SemanticLab";
import { ModelsLab } from "./labs/ModelsLab";
import { FiltersLab } from "./labs/FiltersLab";
import { RagLab } from "./labs/RagLab";
import { ReasonLab } from "./labs/ReasonLab";
import { RefuseLab } from "./labs/RefuseLab";
import { ScaleLab } from "./labs/ScaleLab";
import { MapLab } from "./labs/MapLab";

const COMPONENTS: Record<LabId, ComponentType<LabProps>> = {
  semantic: SemanticLab,
  models: ModelsLab,
  filters: FiltersLab,
  rag: RagLab,
  reason: ReasonLab,
  refuse: RefuseLab,
  scale: ScaleLab,
  map: MapLab,
};

const PACK_KEY = "nebula-labs-pack";

function storedPack(): string {
  try {
    return localStorage.getItem(PACK_KEY) ?? "campus";
  } catch {
    return "campus";
  }
}

function readState(): LabState {
  const s = parseLabHash(window.location.hash, storedPack()) ?? { lab: null, pack: storedPack() };
  if (s.lab && !(s.lab in COMPONENTS)) s.lab = null;
  return s;
}

export function LabsTab() {
  const [state, setState] = useState<LabState>(readState);
  const env = useLabEnv();

  useEffect(() => {
    const on = () => {
      if (window.location.hash.startsWith("#/labs")) setState(readState());
    };
    window.addEventListener("hashchange", on);
    if (!window.location.hash.startsWith("#/labs")) history.replaceState(null, "", labHash(state));
    return () => window.removeEventListener("hashchange", on);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    try {
      localStorage.setItem(PACK_KEY, state.pack);
    } catch {
      /* private mode */
    }
  }, [state.pack]);

  /** Param tweaks replace the history entry; switching lab or pack pushes one. */
  const update = useCallback((p: Partial<LabState>) => {
    setState((prev) => {
      const next = { ...prev, ...p };
      const h = labHash(next);
      if (p.lab !== undefined || p.pack !== undefined) window.location.hash = h;
      else history.replaceState(null, "", h);
      return next;
    });
  }, []);

  const pack = getPack(state.pack);
  if (!state.lab) return <LabsHome pack={pack.id} onPack={(id) => update({ pack: id })} />;
  return <LabView key={`${state.lab}-${pack.id}`} lab={state.lab} state={state} update={update} env={env} />;
}

function LabView({ lab, state, update, env }: { lab: LabId; state: LabState; update: (p: Partial<LabState>) => void; env: ReturnType<typeof useLabEnv> }) {
  const pack = getPack(state.pack);
  const seed = usePackSeed(pack);
  const Comp = COMPONENTS[lab];
  return (
    <LabFrame meta={LAB_BY_ID[lab]} pack={pack} state={state} update={update} env={env} seed={seed}>
      <Comp pack={pack} state={state} update={update} env={env} />
    </LabFrame>
  );
}

const CONCEPTS: { id: Concept | "all"; label: string }[] = [
  { id: "all", label: "All" },
  { id: "search", label: "Search" },
  { id: "models", label: "Models" },
  { id: "filters", label: "Filters" },
  { id: "rag", label: "RAG" },
  { id: "reasoning", label: "Reasoning" },
  { id: "safety", label: "Safety" },
  { id: "scale", label: "Scale" },
];

function LabsHome({ pack, onPack }: { pack: string; onPack: (id: string) => void }) {
  const [concept, setConcept] = useState<Concept | "all">("all");
  const [quick, setQuick] = useState(false);
  const p = getPack(pack);
  const shown = LABS.filter((l) => (concept === "all" || l.concept === concept) && (!quick || l.minutes === 2));
  const href = (id: LabId) => `#/labs/${id}?pack=${p.id}`;

  return (
    <div className="space-y-6">
      <div className="card space-y-4">
        <p className="max-w-3xl text-sm text-gray-600 dark:text-muted">
          Eight short experiments, each on a real business's (invented) data and a real NebulaDB. Each lab says how it works, what its result
          doesn't show, and gives you a challenge. Pick a business — every lab works with every one.
        </p>
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
          {PACKS.map((x) => (
            <button
              key={x.id}
              onClick={() => onPack(x.id)}
              className={`rounded-xl border p-3 text-left transition-colors ${x.id === p.id ? "border-accent bg-accent/5" : "border-gray-200 dark:border-edge hover:border-gray-300 dark:hover:border-faint"}`}
              aria-pressed={x.id === p.id}
            >
              <div className="text-sm font-semibold text-black dark:text-ink">{x.name}</div>
              <div className="text-[11px] text-gray-500 dark:text-muted">{x.industry}</div>
              <div className="mt-1 text-[11px] text-gray-500 dark:text-muted line-clamp-2">{x.tagline}</div>
              <div className="mt-1.5 font-mono text-[10px] text-gray-400 dark:text-faint">
                {x.docs.length} docs · {x.questions.length} labelled questions
              </div>
            </button>
          ))}
        </div>
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <span className="eyebrow mr-1">Start here</span>
          {START_PATH.map((id, i) => (
            <span key={id} className="inline-flex items-center gap-2">
              <a href={href(id)} className="rounded-full border border-gray-200 dark:border-edge px-2.5 py-0.5 hover:border-accent hover:text-accent">
                {LAB_BY_ID[id].title}
              </a>
              {i < START_PATH.length - 1 && <span className="text-gray-300 dark:text-faint">→</span>}
            </span>
          ))}
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        {CONCEPTS.map((c) => (
          <button key={c.id} onClick={() => setConcept(c.id)} className={`tab-btn ${concept === c.id ? "tab-btn-active" : ""}`}>
            {c.label}
          </button>
        ))}
        <label className="ml-auto inline-flex items-center gap-2 text-xs text-gray-500 dark:text-muted">
          <input type="checkbox" checked={quick} onChange={(e) => setQuick(e.target.checked)} /> 2-minute labs only
        </label>
        <PackPicker value={p.id} onChange={onPack} />
      </div>

      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        {shown.map((l) => (
          <a key={l.id} href={href(l.id)} className="instrument !p-4 flex flex-col gap-2 group">
            <div className="flex items-center justify-between">
              <span className="eyebrow">{l.concept}</span>
              <span className="font-mono text-[10px] text-gray-400 dark:text-faint">{l.minutes} min</span>
            </div>
            <div className="text-[13px] text-accent">{l.hook}</div>
            <div className="font-display text-base font-semibold text-black dark:text-ink group-hover:underline">{l.title}</div>
            <p className="text-xs text-gray-600 dark:text-muted line-clamp-4">{l.lead}</p>
            <p className="mt-auto pt-1 text-[11px] text-gray-500 dark:text-muted line-clamp-2">
              <span className="text-accent">Challenge:</span> {p.challenges[l.id]}
            </p>
            <div className="flex flex-wrap gap-1">
              {l.needs.map((n) => (
                <span key={n} className="rounded bg-gray-100 dark:bg-carbon-800 px-1.5 py-0.5 font-mono text-[10px] text-gray-500 dark:text-muted">
                  {n === "browser" ? "runs in browser" : n === "llm" ? "needs LLM" : "uses embedder"}
                </span>
              ))}
            </div>
          </a>
        ))}
      </div>
    </div>
  );
}
