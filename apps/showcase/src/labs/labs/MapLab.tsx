import { useMemo, useState } from "react";
import { ErrorBanner } from "../../components";
import { cosine, pcaFit, project, type PcaFit } from "../engine/core";
import { BROWSER_MODEL_LABEL, embedInBrowser, loadBrowserModel } from "../engine/browserEmbed";
import { QuestionBox } from "../shell";
import type { LabProps } from "./props";

const PALETTE = ["#4C8DFF", "#3FB950", "#D29922", "#F85149", "#A371F7", "#39C5CF", "#F778BA", "#8B949E", "#E3B341", "#1F6FEB"];
const W = 640;
const H = 420;
const PAD = 24;

interface Space {
  vecs: number[][];
  pts: number[][];
  fit: PcaFit;
}

export function MapLab({ pack, state, update }: LabProps) {
  const [space, setSpace] = useState<Space | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [query, setQuery] = useState<{ q: string; pt: number[]; vec: number[]; nn: { i: number; s: number }[] } | null>(null);
  const [hover, setHover] = useState<number | null>(null);
  const [hidden, setHidden] = useState<Set<string>>(new Set());

  const groups = useMemo(() => [...new Set(pack.docs.map((d) => String(d.metadata[pack.colorField])))], [pack]);
  const colour = (g: string) => PALETTE[groups.indexOf(g) % PALETTE.length];

  const build = async () => {
    setErr(null);
    try {
      setBusy("Downloading the model (once)…");
      await loadBrowserModel((p) => setBusy(`Downloading the model… ${Math.round(p)}%`));
      setBusy("Embedding the documents on your device…");
      const vecs = await embedInBrowser(pack.docs.map((d) => d.text), (done, total) => setBusy(`Embedding on your device… ${done}/${total}`));
      const fit = pcaFit(vecs, 2);
      setSpace({ vecs, pts: vecs.map((v) => project(v, fit)), fit });
      setQuery(null);
      if (state.q) await ask(state.q, { vecs, fit });
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(null);
    }
  };

  const ask = async (q: string, s: { vecs: number[][]; fit: PcaFit } | null = space) => {
    if (!s) return;
    update({ q });
    const [qv] = await embedInBrowser([q]);
    const nn = s.vecs
      .map((v, i) => ({ i, s: cosine(qv, v) }))
      .sort((a, b) => b.s - a.s)
      .slice(0, 5);
    setQuery({ q, pt: project(qv, s.fit), vec: qv, nn });
  };

  const bounds = useMemo(() => {
    if (!space) return null;
    const all = query ? [...space.pts, query.pt] : space.pts;
    const xs = all.map((p) => p[0]);
    const ys = all.map((p) => p[1]);
    return { x0: Math.min(...xs), x1: Math.max(...xs), y0: Math.min(...ys), y1: Math.max(...ys) };
  }, [space, query]);
  const sx = (x: number) => (bounds ? PAD + ((x - bounds.x0) / (bounds.x1 - bounds.x0 || 1)) * (W - 2 * PAD) : 0);
  const sy = (y: number) => (bounds ? H - PAD - ((y - bounds.y0) / (bounds.y1 - bounds.y0 || 1)) * (H - 2 * PAD) : 0);

  return (
    <div className="space-y-4">
      {!space ? (
        <div className="card space-y-3">
          <p className="text-sm text-gray-600 dark:text-muted">
            The map embeds all {pack.docs.length} {pack.name} documents with {BROWSER_MODEL_LABEL}. The model (~23 MB) downloads from Hugging Face
            once; after that everything runs on your device.
          </p>
          <button className="btn" onClick={build} disabled={!!busy}>
            {busy ?? "Build the map"}
          </button>
        </div>
      ) : (
        <div className="card space-y-3">
          <QuestionBox value={state.q ?? ""} samples={pack.questions.slice(0, 5).map((x) => x.q)} onSubmit={(q) => void ask(q)} cta="Place it" />
        </div>
      )}
      <ErrorBanner err={err} />

      {space && bounds && (
        <div className="grid gap-4 lg:grid-cols-[1fr_18rem]">
          <div className="card !p-3">
            <svg viewBox={`0 0 ${W} ${H}`} className="w-full h-auto" role="img" aria-label={`2-D map of ${pack.docs.length} documents coloured by ${pack.colorField}`}>
              <rect x={0} y={0} width={W} height={H} className="fill-transparent" />
              {query &&
                query.nn.map((n) => (
                  <line key={n.i} x1={sx(query.pt[0])} y1={sy(query.pt[1])} x2={sx(space.pts[n.i][0])} y2={sy(space.pts[n.i][1])} stroke="#4C8DFF" strokeOpacity={0.5} strokeWidth={1} />
                ))}
              {space.pts.map((p, i) => {
                const g = String(pack.docs[i].metadata[pack.colorField]);
                if (hidden.has(g)) return null;
                const isNn = query?.nn.some((n) => n.i === i);
                return (
                  <circle
                    key={pack.docs[i].id}
                    cx={sx(p[0])}
                    cy={sy(p[1])}
                    r={hover === i ? 7 : isNn ? 6 : 4.5}
                    fill={colour(g)}
                    fillOpacity={query && !isNn ? 0.45 : 0.9}
                    stroke={isNn ? "#4C8DFF" : "none"}
                    strokeWidth={2}
                    onMouseEnter={() => setHover(i)}
                    onMouseLeave={() => setHover(null)}
                    onFocus={() => setHover(i)}
                    tabIndex={0}
                  >
                    <title>{`${pack.docs[i].id} · ${g}\n${pack.docs[i].text}`}</title>
                  </circle>
                );
              })}
              {query && (
                <g>
                  <circle cx={sx(query.pt[0])} cy={sy(query.pt[1])} r={8} fill="none" stroke="currentColor" strokeWidth={2} className="text-black dark:text-ink" />
                  <circle cx={sx(query.pt[0])} cy={sy(query.pt[1])} r={2.5} className="fill-black dark:fill-ink" />
                </g>
              )}
            </svg>
            <div className="mt-2 flex flex-wrap gap-2">
              {groups.map((g) => (
                <button
                  key={g}
                  onClick={() => setHidden((h) => {
                    const n = new Set(h);
                    if (n.has(g)) n.delete(g);
                    else n.add(g);
                    return n;
                  })}
                  className={`inline-flex items-center gap-1.5 rounded-full border border-gray-200 dark:border-edge px-2 py-0.5 text-[11px] ${hidden.has(g) ? "opacity-40" : ""}`}
                  aria-pressed={!hidden.has(g)}
                >
                  <span className="h-2 w-2 rounded-full" style={{ background: colour(g) }} />
                  {g}
                </button>
              ))}
            </div>
          </div>

          <div className="space-y-3">
            <div className="card !p-4 space-y-2">
              <div className="eyebrow">{hover !== null ? "Hovered" : query ? "Nearest by true cosine" : "Hover a point"}</div>
              {hover !== null ? (
                <div className="text-xs space-y-1">
                  <div className="font-mono text-gray-500 dark:text-muted">
                    {pack.docs[hover].id} · {String(pack.docs[hover].metadata[pack.colorField])}
                  </div>
                  <p className="text-gray-700 dark:text-ink">{pack.docs[hover].text}</p>
                  {query && <p className="font-mono text-accent">cosine to question: {cosine(space.vecs[hover], query.vec).toFixed(3)}</p>}
                  {query?.nn.some((n) => n.i === hover) && <p className="text-[11px] text-ok">one of the 5 nearest</p>}
                </div>
              ) : query ? (
                <ol className="space-y-1.5 text-xs">
                  {query.nn.map((n, r) => (
                    <li key={n.i}>
                      <span className="font-mono text-accent">{n.s.toFixed(3)}</span> <span className="font-mono text-gray-500 dark:text-muted">{r + 1}. {pack.docs[n.i].id}</span>
                      <p className="text-gray-700 dark:text-ink line-clamp-2">{pack.docs[n.i].text}</p>
                    </li>
                  ))}
                </ol>
              ) : (
                <p className="text-xs text-gray-500 dark:text-muted">Each dot is one document. Colour = {pack.colorField}.</p>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
