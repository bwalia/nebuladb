/**
 * Talent Match — a two-sided job board built on NebulaDB.
 *
 * Hiring side: pick (or post) a job → candidates are matched in three
 * real steps:
 *   1. SQL hard filters + semantic retrieval in one query
 *      (`semantic_match(...) AND day_rate <= … AND array_contains(…)`),
 *      run alongside a filter-free query so near misses can be shown
 *      with the exact rule that excluded them.
 *   2. A second semantic facet: the job's team culture vs each
 *      candidate's working style, stored as its own vector in
 *      `talent_traits` — "character" is matched, not keyword-guessed.
 *   3. Transparent scoring in the browser (skills coverage, rate fit,
 *      availability) with user-tunable weights.
 * The recruiter chat is then grounded on exactly the ranked shortlist.
 *
 * Candidate side runs the same idea in reverse: a candidate's profile
 * against open jobs, with a career-coach chat.
 */
import { useCallback, useEffect, useMemo, useState } from "react";
import { api, ApiError, type SqlRow } from "../api";
import { ErrorBanner, Panel, Spinner, Stat } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ensureSeeded } from "../rag/seed";
import { field, lit, minMax, runSql, semanticText, similarity } from "../rag/sql";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { GroundedChat, type ContextDoc } from "../rag/GroundedChat";
import {
  CANDIDATE_DOCS,
  CANDIDATES,
  CANDIDATES_BUCKET,
  JOB_DOCS,
  JOBS_BUCKET,
  TALENT_SEED_VERSION,
  TRAIT_DOCS,
  TRAITS_BUCKET,
  candidateText,
  jobDoc,
  jobText,
  normSkill,
  parseSkills,
  type Candidate,
  type Engagement,
  type Job,
  type WorkMode,
} from "../rag/data/talent";

// ---------------------------------------------------------------------------
// Scoring

type Facet = "semantic" | "skills" | "culture" | "rate" | "availability";

const FACETS: Array<{ key: Facet; label: string; hint: string }> = [
  { key: "semantic", label: "Experience", hint: "Semantic similarity of the candidate's profile to the job description" },
  { key: "skills", label: "Skills", hint: "Coverage of must-have (75%) and nice-to-have (25%) skills" },
  { key: "culture", label: "Character", hint: "Semantic similarity of their working style to the team culture" },
  { key: "rate", label: "Rate", hint: "1 within budget, falling to 0 at the stretch limit" },
  { key: "availability", label: "Availability", hint: "1 if available by the start date, falling to 0 at 8 weeks late" },
];

const DEFAULT_WEIGHTS: Record<Facet, number> = {
  semantic: 30,
  skills: 30,
  culture: 15,
  rate: 15,
  availability: 10,
};

interface Scored {
  cand: Candidate;
  facets: Record<Facet, number>;
  matched: string[];
  missing: string[];
  niceMatched: string[];
  flags: string[];
  /** Raw cosine similarities, before per-result-set normalisation. */
  rawSemantic: number;
  rawCulture: number | null;
}

interface NearMiss {
  cand: Candidate;
  reasons: string[];
}

const DAY = 86_400_000;

function weeksBetween(fromIso: string, toIso: string): number {
  return (Date.parse(toIso) - Date.parse(fromIso)) / (7 * DAY);
}

function rateOf(c: Candidate, e: Engagement): number {
  return e === "contract" ? c.day_rate : c.salary;
}

function fmtRate(n: number, e: Engagement): string {
  return e === "contract" ? `£${n}/day` : `£${Math.round(n / 1000)}k`;
}

function rateFit(rate: number, budget: number, stretch: number): number {
  if (rate <= budget) return 1;
  if (stretch <= 0) return 0;
  const over = (rate - budget) / budget;
  return Math.max(0, 1 - over / stretch);
}

function availabilityFit(availableFrom: string, startBy: string): number {
  const late = weeksBetween(startBy, availableFrom);
  return late <= 0 ? 1 : Math.max(0, 1 - late / 8);
}

function skillCoverage(have: string[], must: string[], nice: string[]) {
  const set = new Set(have.map(normSkill));
  const matched = must.filter((s) => set.has(normSkill(s)));
  const missing = must.filter((s) => !set.has(normSkill(s)));
  const niceMatched = nice.filter((s) => set.has(normSkill(s)));
  const mustCov = must.length ? matched.length / must.length : 1;
  const niceCov = nice.length ? niceMatched.length / nice.length : 1;
  return { matched, missing, niceMatched, score: 0.75 * mustCov + 0.25 * niceCov };
}

function composite(s: Scored, w: Record<Facet, number>): number {
  const total = FACETS.reduce((n, f) => n + w[f.key], 0) || 1;
  return FACETS.reduce((n, f) => n + s.facets[f.key] * w[f.key], 0) / total;
}

function locationOk(c: Candidate, j: Job): boolean {
  return j.mode === "remote" ? c.locations.includes("Remote") : c.locations.includes(j.location);
}

/** Why a candidate failed the hard filters — re-evaluated client-side
 *  with the same rules the SQL used, so the reasons are exact. */
function exclusionReasons(c: Candidate, j: Job, f: HardFilters): string[] {
  const out: string[] = [];
  const rate = rateOf(c, j.engagement);
  const cap = Math.round(j.budget_max * (1 + f.stretch));
  if (rate > cap) out.push(`rate ${fmtRate(rate, j.engagement)} > cap ${fmtRate(cap, j.engagement)}`);
  if (f.minYears && c.years < j.min_years) out.push(`${c.years} yrs < ${j.min_years} required`);
  if (f.location && !locationOk(c, j))
    out.push(j.mode === "remote" ? "not open to remote" : `won't work in ${j.location} (${c.locations.join("/")})`);
  if (f.rightToWork && !j.sponsorship && !c.right_to_work) out.push("needs visa sponsorship");
  return out;
}

interface HardFilters {
  stretch: number;
  minYears: boolean;
  location: boolean;
  rightToWork: boolean;
}

const DEFAULT_FILTERS: HardFilters = { stretch: 0.1, minYears: true, location: true, rightToWork: true };

// ---------------------------------------------------------------------------
// Row → domain object

const CANDIDATE_BY_ID = new Map(CANDIDATES.map((c) => [c.id, c]));

function candidateFromRow(r: SqlRow): Candidate {
  const meta = (field<Record<string, unknown>>(r, "metadata") ?? {}) as unknown as Candidate;
  return { ...(CANDIDATE_BY_ID.get(r.id) ?? ({} as Candidate)), ...meta, id: r.id };
}

function jobFromRow(r: SqlRow): Job {
  const meta = (field<Record<string, unknown>>(r, "metadata") ?? {}) as unknown as Job;
  return { ...meta, id: r.id };
}

function label(c: Candidate, blind: boolean) {
  return blind ? `Candidate ${c.id}` : c.name;
}

// ---------------------------------------------------------------------------

type SeedState = { phase: "seeding"; done: number; total: number } | { phase: "ready" } | { phase: "error"; err: string };

export function TalentMatchTab() {
  const [seed, setSeed] = useState<SeedState>({ phase: "seeding", done: 0, total: 0 });
  const [mode, setMode] = useState<"hiring" | "candidate">("hiring");
  const [blind, setBlind] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const total = JOB_DOCS.length + CANDIDATE_DOCS.length + TRAIT_DOCS.length;
    let base = 0;
    const progress = (d: number) => !cancelled && setSeed({ phase: "seeding", done: base + d, total });
    (async () => {
      for (const [bucket, docs] of [
        [JOBS_BUCKET, JOB_DOCS],
        [CANDIDATES_BUCKET, CANDIDATE_DOCS],
        [TRAITS_BUCKET, TRAIT_DOCS],
      ] as const) {
        await ensureSeeded(bucket, docs, TALENT_SEED_VERSION, progress);
        base += docs.length;
      }
      if (!cancelled) setSeed({ phase: "ready" });
    })().catch((e) => !cancelled && setSeed({ phase: "error", err: errText(e) }));
    return () => {
      cancelled = true;
    };
  }, []);

  if (seed.phase === "error") {
    return (
      <Panel title="Talent Match" subtitle="Couldn't load the demo corpus">
        <ErrorBanner err={seed.err} />
      </Panel>
    );
  }
  if (seed.phase === "seeding") {
    return (
      <Panel title="Talent Match" subtitle="Preparing the job board">
        <Spinner
          label={
            seed.total
              ? `indexing jobs, candidate profiles and working-style vectors… ${seed.done}/${seed.total}`
              : "checking corpus…"
          }
        />
      </Panel>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-3">
        <div className="inline-flex rounded-lg border border-gray-200 dark:border-edge p-0.5">
          {(["hiring", "candidate"] as const).map((m) => (
            <button
              key={m}
              onClick={() => setMode(m)}
              className={`px-3 py-1.5 text-sm rounded-md transition-colors ${
                mode === m
                  ? "bg-gray-900 text-white dark:bg-carbon-800 dark:text-ink"
                  : "text-gray-600 dark:text-muted hover:text-black dark:hover:text-ink"
              }`}
            >
              {m === "hiring" ? "Hiring manager" : "Candidate"}
            </button>
          ))}
        </div>
        <label className="inline-flex items-center gap-2 text-sm text-gray-600 dark:text-muted">
          <input type="checkbox" checked={blind} onChange={(e) => setBlind(e.target.checked)} />
          Blind screening
          <span className="text-xs text-gray-400 dark:text-faint">(hides names from the list and the LLM)</span>
        </label>
        <span className="ml-auto text-xs text-gray-400 dark:text-faint">
          No age, gender or other protected characteristics are stored, so retrieval and the LLM can't use them.
        </span>
      </div>
      {mode === "hiring" ? <HiringView blind={blind} /> : <CandidateView />}
      <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "/api/v1/bucket"]} />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Hiring manager

function HiringView({ blind }: { blind: boolean }) {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [jobQuery, setJobQuery] = useState("");
  const [jobsBusy, setJobsBusy] = useState(false);
  const [job, setJob] = useState<Job | null>(null);
  const [filters, setFilters] = useState<HardFilters>(DEFAULT_FILTERS);
  const [weights, setWeights] = useState(DEFAULT_WEIGHTS);
  const [scored, setScored] = useState<Scored[]>([]);
  const [nearMisses, setNearMisses] = useState<NearMiss[]>([]);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [showPost, setShowPost] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [runId, setRunId] = useState(0);

  const loadJobs = useCallback(async (q: string) => {
    setJobsBusy(true);
    setErr(null);
    try {
      const r = await runSql(
        `SELECT * FROM ${JOBS_BUCKET} WHERE semantic_match(description, ${lit(semanticText(q || "technology job"))}) LIMIT 50`
      );
      const list = r.rows.map(jobFromRow).filter((j) => j.title);
      // With no search text, show the board newest-first instead of by similarity.
      if (!q) list.sort((a, b) => (b.posted_at ?? "").localeCompare(a.posted_at ?? ""));
      setJobs(list);
    } catch (e) {
      setErr(errText(e));
    } finally {
      setJobsBusy(false);
    }
  }, []);

  useEffect(() => {
    void loadJobs("");
  }, [loadJobs]);

  const runMatch = useCallback(
    async (j: Job, f: HardFilters) => {
      setBusy(true);
      setErr(null);
      setExpanded(null);
      const rateField = j.engagement === "contract" ? "day_rate" : "salary";
      const cap = Math.round(j.budget_max * (1 + f.stretch));
      const sem = `semantic_match(profile, ${lit(semanticText(jobText(j)))})`;
      const base = [sem, `array_contains(open_to, ${lit(j.engagement)})`];
      const strict = [...base, `${rateField} <= ${cap}`];
      if (f.minYears) strict.push(`years >= ${j.min_years}`);
      if (f.location) strict.push(`array_contains(locations, ${lit(j.mode === "remote" ? "Remote" : j.location)})`);
      if (f.rightToWork && !j.sponsorship) strict.push("right_to_work = TRUE");
      const strictSql = `SELECT * FROM ${CANDIDATES_BUCKET}\nWHERE ${strict.join("\n  AND ")}\nLIMIT 40`;
      const broadSql = `SELECT * FROM ${CANDIDATES_BUCKET} WHERE ${base.join(" AND ")} LIMIT 40`;
      const traitSql = `SELECT * FROM ${TRAITS_BUCKET}\nWHERE semantic_match(traits, ${lit(semanticText(j.culture))})\nLIMIT 40`;
      setSteps([
        { label: "SQL: hard filters + semantic retrieval", detail: strictSql, status: "running" },
        { label: "SQL: character facet (team culture ↔ working style)", detail: traitSql, status: "running" },
      ]);
      try {
        const [strictRun, broadRun, traitRun] = await Promise.all([runSql(strictSql), runSql(broadSql), runSql(traitSql)]);
        const scoreStart = performance.now();
        const survivors = strictRun.rows.map((r) => ({ cand: candidateFromRow(r), sim: similarity(r.score) }));
        const traitSim = new Map(traitRun.rows.map((r) => [r.id, similarity(r.score)]));
        const normSem = minMax(survivors.map((s) => s.sim));
        const traitVals = survivors.map((s) => traitSim.get(s.cand.id)).filter((v): v is number => v !== undefined);
        const normCulture = minMax(traitVals);

        const out: Scored[] = survivors.map(({ cand, sim }) => {
          const sk = skillCoverage(cand.skills, j.must_have, j.nice_to_have);
          const rate = rateOf(cand, j.engagement);
          const rawCulture = traitSim.get(cand.id) ?? null;
          const flags: string[] = [];
          if (rate > j.budget_max)
            flags.push(`${Math.round(((rate - j.budget_max) / j.budget_max) * 100)}% over budget`);
          const late = weeksBetween(j.start_by, cand.available_from);
          if (late > 0) flags.push(`available ${Math.ceil(late)}w after start`);
          if (cand.notice_weeks >= 8) flags.push(`${cand.notice_weeks}w notice`);
          if (!cand.right_to_work) flags.push("needs sponsorship");
          if (!f.minYears && cand.years < j.min_years) flags.push(`${cand.years} yrs (< ${j.min_years})`);
          return {
            cand,
            facets: {
              semantic: normSem(sim),
              skills: sk.score,
              culture: rawCulture === null ? 0 : normCulture(rawCulture),
              rate: rateFit(rate, j.budget_max, f.stretch),
              availability: availabilityFit(cand.available_from, j.start_by),
            },
            matched: sk.matched,
            missing: sk.missing,
            niceMatched: sk.niceMatched,
            flags,
            rawSemantic: sim,
            rawCulture,
          };
        });
        const strictIds = new Set(survivors.map((s) => s.cand.id));
        const misses: NearMiss[] = broadRun.rows
          .filter((r) => !strictIds.has(r.id))
          .map((r) => {
            const cand = candidateFromRow(r);
            return { cand, reasons: exclusionReasons(cand, j, f) };
          })
          .filter((m) => m.reasons.length > 0)
          .slice(0, 6);
        const scoreMs = Math.round(performance.now() - scoreStart);

        setScored(out);
        setNearMisses(misses);
        setRunId((n) => n + 1);
        setSteps([
          {
            label: "SQL: hard filters + semantic retrieval",
            detail: strictSql,
            rowsIn: broadRun.rows.length,
            rowsOut: strictRun.rows.length,
            ms: strictRun.tookMs,
          },
          {
            label: "SQL: character facet (team culture ↔ working style)",
            detail: traitSql,
            rowsOut: traitRun.rows.length,
            ms: traitRun.tookMs,
          },
          {
            label: "Score: weighted facets (in browser)",
            detail:
              "score = Σ wᵢ·facetᵢ / Σ wᵢ — experience & character min-max scaled per result set; skills = 0.75·must + 0.25·nice; rate & availability deterministic",
            rowsOut: out.length,
            ms: scoreMs,
          },
        ]);
      } catch (e) {
        setErr(errText(e));
        setSteps((s) => s.map((x) => ({ ...x, status: "error" })));
      } finally {
        setBusy(false);
      }
    },
    []
  );

  const ranked = useMemo(
    () =>
      scored
        .map((s) => ({ ...s, total: composite(s, weights) }))
        .sort((a, b) => b.total - a.total),
    [scored, weights]
  );

  const select = (j: Job) => {
    setJob(j);
    void runMatch(j, filters);
  };

  const updateFilters = (patch: Partial<HardFilters>) => {
    const next = { ...filters, ...patch };
    setFilters(next);
    if (job) void runMatch(job, next);
  };

  const getContext = useCallback(
    async (): Promise<ContextDoc[]> => {
      if (!job) return [];
      const brief: ContextDoc = {
        id: job.id,
        bucket: JOBS_BUCKET,
        label: "Job brief",
        text: [
          `JOB: ${job.title} at ${job.company}. ${job.engagement}, ${job.mode}, ${job.location}.`,
          `Budget up to ${fmtRate(job.budget_max, job.engagement)}. Minimum ${job.min_years} years. Start by ${job.start_by}. Visa sponsorship: ${job.sponsorship ? "yes" : "no"}.`,
          `Must have: ${job.must_have.join(", ")}. Nice to have: ${job.nice_to_have.join(", ")}.`,
          job.description,
          `Team culture: ${job.culture}`,
        ].join("\n"),
      };
      const shortlist = ranked.slice(0, 8).map((s, i): ContextDoc => {
        const c = s.cand;
        return {
          id: c.id,
          bucket: CANDIDATES_BUCKET,
          label: label(c, blind),
          text: [
            `CANDIDATE rank ${i + 1}: ${label(c, blind)} (${c.id}) — ${c.headline}.`,
            `Match score ${pct(s.total)} (experience ${pct(s.facets.semantic)}, skills ${pct(s.facets.skills)}, character ${pct(s.facets.culture)}, rate ${pct(s.facets.rate)}, availability ${pct(s.facets.availability)}).`,
            `Rate ${fmtRate(rateOf(c, job.engagement), job.engagement)}; ${c.years} years; based in ${c.location}, will work in ${c.locations.join("/")}; available ${c.available_from} (${c.notice_weeks} weeks notice); right to work in UK: ${c.right_to_work ? "yes" : "no"}.`,
            `Must-haves matched: ${s.matched.join(", ") || "none"}; missing: ${s.missing.join(", ") || "none"}. Nice-to-haves: ${s.niceMatched.join(", ") || "none"}.`,
            s.flags.length ? `Flags: ${s.flags.join("; ")}.` : "",
            c.summary,
            c.experience,
            `Working style: ${c.traits}`,
          ]
            .filter(Boolean)
            .join("\n"),
        };
      });
      return [brief, ...shortlist];
    },
    [job, ranked, blind]
  );

  const totalWeight = FACETS.reduce((n, f) => n + weights[f.key], 0) || 1;

  return (
    <>
      <div className="grid gap-4 lg:grid-cols-[minmax(0,19rem)_minmax(0,1fr)]">
        {/* Job board */}
        <Panel
          title="Job board"
          subtitle={`${jobs.length} open roles · semantic search`}
          action={
            <button className="btn-secondary !py-1 !px-2 !text-xs" onClick={() => setShowPost((v) => !v)}>
              {showPost ? "Close" : "+ Post a job"}
            </button>
          }
        >
          <form
            className="flex gap-2"
            onSubmit={(e) => {
              e.preventDefault();
              void loadJobs(jobQuery.trim());
            }}
          >
            <input
              className="input flex-1"
              value={jobQuery}
              onChange={(e) => setJobQuery(e.target.value)}
              placeholder="e.g. healthcare AI, remote infra…"
            />
            <button className="btn" disabled={jobsBusy}>
              {jobsBusy ? "…" : "Search"}
            </button>
          </form>
          <ul className="space-y-1.5 max-h-[34rem] overflow-y-auto pr-1">
            {jobs.map((j) => (
              <li key={j.id}>
                <button
                  onClick={() => select(j)}
                  className={`w-full text-left rounded-md border px-3 py-2 transition-colors ${
                    job?.id === j.id
                      ? "border-accent bg-accent/5"
                      : "border-gray-200 dark:border-edge hover:bg-gray-50 dark:hover:bg-carbon-900"
                  }`}
                >
                  <div className="text-sm font-medium text-gray-900 dark:text-ink">{j.title}</div>
                  <div className="text-xs text-gray-500 dark:text-muted">
                    {j.company} · {j.location} · {j.mode}
                  </div>
                  <div className="mt-1 flex flex-wrap gap-1.5">
                    <span className="metachip">{j.engagement}</span>
                    <span className="metachip">≤ {fmtRate(j.budget_max, j.engagement)}</span>
                    <span className="metachip">{j.min_years}+ yrs</span>
                  </div>
                </button>
              </li>
            ))}
            {!jobsBusy && jobs.length === 0 && (
              <li className="text-sm text-gray-500 dark:text-muted">No roles match that search.</li>
            )}
          </ul>
        </Panel>

        <div className="space-y-4 min-w-0">
          {showPost && (
            <PostJobForm
              onPosted={(j) => {
                setShowPost(false);
                setJobs((prev) => [j, ...prev.filter((x) => x.id !== j.id)]);
                select(j);
              }}
            />
          )}

          {err && <ErrorBanner err={err} />}

          {!job && !showPost && (
            <Panel title="Pick a role" subtitle="Or post your own — it's indexed instantly and matchable straight away.">
              <p className="text-sm text-gray-600 dark:text-muted">
                Matching runs a SQL query that combines <code>semantic_match</code> with hard filters on rate,
                experience, location and right to work, then a second vector search over candidates'
                working style for culture fit. Every candidate shows why they ranked where they did.
              </p>
            </Panel>
          )}

          {job && (
            <Panel
              title={job.title}
              subtitle={`${job.company} · ${job.location} (${job.mode}) · ${job.engagement} · start by ${job.start_by}`}
            >
              <p className="text-sm text-gray-700 dark:text-muted">{job.description}</p>
              <div className="flex flex-wrap gap-1.5">
                {job.must_have.map((s) => (
                  <span key={s} className="rounded-full bg-gray-900 text-white dark:bg-ink dark:text-black px-2 py-0.5 text-xs">
                    {s}
                  </span>
                ))}
                {job.nice_to_have.map((s) => (
                  <span key={s} className="rounded-full border border-gray-300 dark:border-edge px-2 py-0.5 text-xs text-gray-600 dark:text-muted">
                    {s}
                  </span>
                ))}
              </div>
              <p className="text-xs text-gray-500 dark:text-muted">
                <span className="eyebrow mr-1.5">Culture</span>
                {job.culture}
              </p>

              {/* Hard filters */}
              <div className="grid gap-3 sm:grid-cols-2 rounded-md border border-gray-200 dark:border-edge p-3">
                <div className="space-y-2">
                  <div className="eyebrow">Hard filters (SQL WHERE)</div>
                  <label className="flex items-center gap-2 text-sm">
                    <span className="w-28 text-gray-600 dark:text-muted">Budget stretch</span>
                    <select
                      className="input !w-auto !py-1"
                      value={filters.stretch}
                      onChange={(e) => updateFilters({ stretch: Number(e.target.value) })}
                    >
                      {[0, 0.05, 0.1, 0.15, 0.25].map((v) => (
                        <option key={v} value={v}>
                          {v === 0 ? "none" : `+${v * 100}%`}
                        </option>
                      ))}
                    </select>
                  </label>
                  {(
                    [
                      ["minYears", `≥ ${job.min_years} years`],
                      ["location", job.mode === "remote" ? "Open to remote" : `Will work in ${job.location}`],
                      ["rightToWork", job.sponsorship ? "Right to work (role sponsors — off)" : "Right to work in UK"],
                    ] as const
                  ).map(([k, text]) => (
                    <label key={k} className="flex items-center gap-2 text-sm text-gray-700 dark:text-muted">
                      <input
                        type="checkbox"
                        checked={filters[k]}
                        disabled={k === "rightToWork" && job.sponsorship}
                        onChange={(e) => updateFilters({ [k]: e.target.checked })}
                      />
                      {text}
                    </label>
                  ))}
                </div>
                <div className="space-y-1.5">
                  <div className="eyebrow">Ranking weights</div>
                  {FACETS.map((f) => (
                    <label key={f.key} className="flex items-center gap-2 text-xs" title={f.hint}>
                      <span className="w-20 text-gray-600 dark:text-muted">{f.label}</span>
                      <input
                        type="range"
                        min={0}
                        max={50}
                        value={weights[f.key]}
                        onChange={(e) => setWeights((w) => ({ ...w, [f.key]: Number(e.target.value) }))}
                        className="flex-1 accent-[#4C8DFF]"
                      />
                      <span className="w-9 text-right font-mono tabular-nums">
                        {Math.round((weights[f.key] / totalWeight) * 100)}%
                      </span>
                    </label>
                  ))}
                  <button className="text-xs text-accent hover:underline" onClick={() => setWeights(DEFAULT_WEIGHTS)}>
                    reset weights
                  </button>
                </div>
              </div>

              <PipelineTrace steps={steps} />
            </Panel>
          )}

          {job && (
            <Panel
              title={busy ? "Matching…" : `Shortlist · ${ranked.length} candidate${ranked.length === 1 ? "" : "s"} pass the hard filters`}
              subtitle="Ranked by weighted facets — drag the weights to re-rank instantly"
            >
              {busy && <Spinner label="running SQL + facet search…" />}
              {!busy && ranked.length === 0 && (
                <p className="text-sm text-gray-500 dark:text-muted">
                  Nobody passes every hard filter. Try a larger budget stretch or relax a filter — near misses are listed below.
                </p>
              )}
              <ol className="space-y-2">
                {ranked.map((s, i) => (
                  <CandidateRow
                    key={s.cand.id}
                    rank={i + 1}
                    s={s}
                    total={s.total}
                    job={job}
                    blind={blind}
                    open={expanded === s.cand.id}
                    onToggle={() => setExpanded((x) => (x === s.cand.id ? null : s.cand.id))}
                  />
                ))}
              </ol>
              {nearMisses.length > 0 && (
                <div className="pt-2">
                  <div className="eyebrow mb-1.5">Near misses — semantically relevant, excluded by a hard filter</div>
                  <ul className="space-y-1">
                    {nearMisses.map((m) => (
                      <li key={m.cand.id} className="text-xs text-gray-600 dark:text-muted">
                        <span className="font-medium text-gray-800 dark:text-ink">{label(m.cand, blind)}</span>{" "}
                        <span className="text-gray-400 dark:text-faint">· {m.cand.headline}</span>{" "}
                        — <span className="text-warn">{m.reasons.join("; ")}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </Panel>
          )}
        </div>
      </div>

      <GroundedChat
        title="Recruiter copilot"
        subtitle={
          job
            ? `Grounded on the job brief + top ${Math.min(8, ranked.length)} ranked candidates${blind ? " (blind)" : ""}`
            : "Select a job to start"
        }
        system={RECRUITER_SYSTEM}
        getContext={getContext}
        resetKey={`${job?.id ?? "none"}:${runId}:${blind}`}
        disabledReason={!job ? "Select a job first" : ranked.length === 0 && !busy ? "No candidates to discuss — relax the filters" : undefined}
        onCite={(d) => setExpanded(d.id)}
        suggestions={[
          "Compare the top 3 candidates for this role",
          "Who can start before the start date and is within budget?",
          "What are the biggest risks with the #1 candidate?",
          "Write 5 interview questions to probe #2's gaps",
          "Draft a short outreach message to the top candidate",
        ]}
      />
    </>
  );
}

const RECRUITER_SYSTEM = `You are a recruitment assistant helping a hiring manager shortlist for the role in [0]. Candidates [1], [2], ... are already ranked by a match score.
Rules:
- Use only facts from the documents and cite them like [1].
- Judge only on skills, experience, rate, availability, location, right to work and stated working style. Never mention or guess age, gender, ethnicity, religion, disability, or any other protected characteristic.
- Be honest about gaps, budget overruns and late availability.
- Prefer short bullet lists. Keep answers under 200 words unless asked to draft something.`;

function CandidateRow({
  rank,
  s,
  total,
  job,
  blind,
  open,
  onToggle,
}: {
  rank: number;
  s: Scored;
  total: number;
  job: Job;
  blind: boolean;
  open: boolean;
  onToggle: () => void;
}) {
  const c = s.cand;
  return (
    <li
      className={`rounded-md border transition-colors ${
        open ? "border-accent/60" : "border-gray-200 dark:border-edge"
      }`}
    >
      <button onClick={onToggle} className="w-full text-left px-3 py-2.5 flex flex-wrap items-start gap-3">
        <span className="font-mono text-xs text-gray-400 dark:text-faint w-5 pt-0.5">#{rank}</span>
        <div className="min-w-[12rem] flex-1">
          <div className="text-sm font-medium text-gray-900 dark:text-ink">
            {label(c, blind)} <span className="font-mono text-[10px] text-gray-400 dark:text-faint">{c.id}</span>
          </div>
          <div className="text-xs text-gray-500 dark:text-muted">{c.headline}</div>
          <div className="mt-1 flex flex-wrap gap-1">
            {s.matched.map((x) => (
              <span key={x} className="rounded bg-ok/15 text-ok px-1.5 py-0.5 text-[10px]">✓ {x}</span>
            ))}
            {s.missing.map((x) => (
              <span key={x} className="rounded bg-bad/10 text-bad px-1.5 py-0.5 text-[10px]">✗ {x}</span>
            ))}
            {s.niceMatched.map((x) => (
              <span key={x} className="rounded border border-gray-200 dark:border-edge text-gray-500 dark:text-muted px-1.5 py-0.5 text-[10px]">+ {x}</span>
            ))}
          </div>
        </div>
        <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs">
          <Stat label="rate" value={fmtRate(rateOf(c, job.engagement), job.engagement)} />
          <Stat label="yrs" value={c.years} />
          <Stat label="from" value={c.available_from.slice(5)} />
        </div>
        <div className="w-44 shrink-0 space-y-0.5">
          {FACETS.map((f) => (
            <div key={f.key} className="flex items-center gap-1.5" title={f.hint}>
              <span className="w-16 text-[10px] text-gray-500 dark:text-muted">{f.label}</span>
              <div className="flex-1 h-1.5 rounded-full bg-gray-100 dark:bg-carbon-800 overflow-hidden">
                <div className="h-full bg-accent" style={{ width: `${Math.round(s.facets[f.key] * 100)}%` }} />
              </div>
            </div>
          ))}
        </div>
        <div className="w-14 text-right">
          <div className="text-lg font-semibold tabular-nums text-gray-900 dark:text-ink">{pct(total)}</div>
          <div className="text-[10px] text-gray-400 dark:text-faint">match</div>
        </div>
      </button>
      {s.flags.length > 0 && (
        <div className="px-3 pb-2 -mt-1 flex flex-wrap gap-1.5 pl-11">
          {s.flags.map((f) => (
            <span key={f} className="rounded border border-warn/40 text-warn px-1.5 py-0.5 text-[10px]">⚠ {f}</span>
          ))}
        </div>
      )}
      {open && (
        <div className="border-t border-gray-200 dark:border-edge px-3 py-2.5 pl-11 space-y-1.5 text-xs text-gray-600 dark:text-muted">
          <p>{c.summary}</p>
          <p>{c.experience}</p>
          <p>
            <span className="eyebrow mr-1.5">Working style</span>
            {c.traits}
          </p>
          <p className="font-mono text-[10px] text-gray-400 dark:text-faint">
            raw cosine: profile {s.rawSemantic.toFixed(3)} · character {s.rawCulture?.toFixed(3) ?? "—"} · {c.location} → {c.locations.join("/")} · notice {c.notice_weeks}w · right to work {c.right_to_work ? "yes" : "no"}
          </p>
        </div>
      )}
    </li>
  );
}

// ---------------------------------------------------------------------------
// Post a job

function PostJobForm({ onPosted }: { onPosted: (j: Job) => void }) {
  const [f, setF] = useState({
    title: "Senior Go Engineer — Open Banking APIs",
    company: "Your company",
    location: "London",
    mode: "hybrid" as WorkMode,
    engagement: "contract" as Engagement,
    budget_max: 650,
    min_years: 5,
    must_have: "go, postgresql, kafka",
    nice_to_have: "kubernetes, payments, rust",
    description:
      "Build and operate open-banking payment initiation APIs: consent flows, idempotent payment submission, webhooks to merchants and reconciliation with partner banks.",
    culture: "Friendly, pragmatic team that pairs often, writes things down and keeps on-call calm.",
    start_by: new Date(Date.now() + 21 * DAY).toISOString().slice(0, 10),
    sponsorship: false,
  });
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const set = <K extends keyof typeof f>(k: K, v: (typeof f)[K]) => setF((x) => ({ ...x, [k]: v }));

  const submit = async () => {
    setErr(null);
    if (!f.title.trim() || !f.description.trim()) return setErr("Title and description are required.");
    const must = parseSkills(f.must_have);
    if (must.length === 0) return setErr("Add at least one must-have skill.");
    const slug = f.title.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 32);
    const job: Job = {
      id: `job-${slug}-${Math.random().toString(36).slice(2, 7)}`,
      title: f.title.trim(),
      company: f.company.trim() || "Unnamed company",
      location: f.mode === "remote" ? "Remote" : f.location.trim(),
      mode: f.mode,
      engagement: f.engagement,
      budget_max: Number(f.budget_max),
      min_years: Number(f.min_years),
      must_have: must,
      nice_to_have: parseSkills(f.nice_to_have),
      description: f.description.trim(),
      culture: f.culture.trim() || "No culture notes provided.",
      start_by: f.start_by,
      sponsorship: f.sponsorship,
      posted_at: new Date().toISOString().slice(0, 10),
    };
    setBusy(true);
    try {
      const d = jobDoc(job);
      await api.upsertDoc(JOBS_BUCKET, d.id, d.text, d.metadata);
      onPosted(job);
    } catch (e) {
      setErr(errText(e));
    } finally {
      setBusy(false);
    }
  };

  const text = (k: keyof typeof f, lbl: string, cls = "") => (
    <label className={`block ${cls}`}>
      <span className="block text-xs font-medium mb-1">{lbl}</span>
      <input className="input" value={String(f[k])} onChange={(e) => set(k, e.target.value as never)} />
    </label>
  );

  return (
    <Panel title="Post a job" subtitle="Indexed into talent_jobs on submit, then matched immediately">
      <div className="grid gap-3 sm:grid-cols-2">
        {text("title", "Title", "sm:col-span-2")}
        {text("company", "Company")}
        {text("location", "Location (city)")}
        <label className="block">
          <span className="block text-xs font-medium mb-1">Work mode</span>
          <select className="input" value={f.mode} onChange={(e) => set("mode", e.target.value as WorkMode)}>
            <option value="onsite">onsite</option>
            <option value="hybrid">hybrid</option>
            <option value="remote">remote</option>
          </select>
        </label>
        <label className="block">
          <span className="block text-xs font-medium mb-1">Engagement</span>
          <select className="input" value={f.engagement} onChange={(e) => set("engagement", e.target.value as Engagement)}>
            <option value="contract">contract (£/day)</option>
            <option value="permanent">permanent (£/year)</option>
          </select>
        </label>
        <label className="block">
          <span className="block text-xs font-medium mb-1">
            Max budget ({f.engagement === "contract" ? "£/day" : "£/year"})
          </span>
          <input className="input" type="number" min={0} value={f.budget_max} onChange={(e) => set("budget_max", Number(e.target.value))} />
        </label>
        <label className="block">
          <span className="block text-xs font-medium mb-1">Minimum years</span>
          <input className="input" type="number" min={0} value={f.min_years} onChange={(e) => set("min_years", Number(e.target.value))} />
        </label>
        {text("must_have", "Must-have skills (comma separated)")}
        {text("nice_to_have", "Nice-to-have skills")}
        <label className="block sm:col-span-2">
          <span className="block text-xs font-medium mb-1">Description</span>
          <textarea className="input min-h-[5rem]" value={f.description} onChange={(e) => set("description", e.target.value)} />
        </label>
        <label className="block sm:col-span-2">
          <span className="block text-xs font-medium mb-1">Team culture (matched against candidates' working style)</span>
          <textarea className="input min-h-[3rem]" value={f.culture} onChange={(e) => set("culture", e.target.value)} />
        </label>
        <label className="block">
          <span className="block text-xs font-medium mb-1">Start by</span>
          <input className="input" type="date" value={f.start_by} onChange={(e) => set("start_by", e.target.value)} />
        </label>
        <label className="flex items-center gap-2 text-sm pt-5">
          <input type="checkbox" checked={f.sponsorship} onChange={(e) => set("sponsorship", e.target.checked)} />
          We can sponsor a visa
        </label>
      </div>
      {err && <ErrorBanner err={err} />}
      <div className="flex justify-end">
        <button className="btn" disabled={busy} onClick={() => void submit()}>
          {busy ? <Spinner label="indexing…" /> : "Post & match"}
        </button>
      </div>
    </Panel>
  );
}

// ---------------------------------------------------------------------------
// Candidate side: roles for a person

interface JobFit {
  job: Job;
  total: number;
  sim: number;
  skills: number;
  matched: string[];
  missing: string[];
  issues: string[];
}

function CandidateView() {
  const [candId, setCandId] = useState(CANDIDATES[2].id);
  const [fits, setFits] = useState<JobFit[]>([]);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const cand = CANDIDATE_BY_ID.get(candId)!;

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setBusy(true);
      setErr(null);
      const sql = `SELECT * FROM ${JOBS_BUCKET}\nWHERE semantic_match(description, ${lit(semanticText(candidateText(cand)))})\n  AND engagement IN (${cand.open_to.map(lit).join(", ")})\nLIMIT 20`;
      setSteps([{ label: "SQL: jobs this person is open to, by semantic fit", detail: sql, status: "running" }]);
      try {
        const r = await runSql(sql);
        const t0 = performance.now();
        const sims = r.rows.map((row) => similarity(row.score));
        const norm = minMax(sims);
        const out = r.rows.map((row, i): JobFit => {
          const job = jobFromRow(row);
          const sk = skillCoverage(cand.skills, job.must_have ?? [], job.nice_to_have ?? []);
          const issues: string[] = [];
          const rate = rateOf(cand, job.engagement);
          if (rate > job.budget_max) issues.push(`you ask ${fmtRate(rate, job.engagement)}, budget ${fmtRate(job.budget_max, job.engagement)}`);
          if (cand.years < job.min_years) issues.push(`${job.min_years}+ years asked`);
          if (!locationOk(cand, job)) issues.push(job.mode === "remote" ? "role is remote" : `role is in ${job.location}`);
          if (!cand.right_to_work && !job.sponsorship) issues.push("no visa sponsorship");
          const late = weeksBetween(job.start_by, cand.available_from);
          if (late > 0) issues.push(`starts ${Math.ceil(late)}w before you're free`);
          const total = 0.45 * norm(sims[i]) + 0.4 * sk.score + 0.15 * (issues.length === 0 ? 1 : Math.max(0, 1 - issues.length * 0.35));
          return { job, total, sim: sims[i], skills: sk.score, matched: sk.matched, missing: sk.missing, issues };
        });
        out.sort((a, b) => b.total - a.total);
        if (cancelled) return;
        setFits(out);
        setSteps([
          { label: "SQL: jobs this person is open to, by semantic fit", detail: sql, rowsOut: r.rows.length, ms: r.tookMs },
          {
            label: "Score: 45% semantic fit + 40% skills + 15% practical fit",
            detail: "practical fit drops 35% per blocker (budget, years, location, visa, start date)",
            rowsOut: out.length,
            ms: Math.round(performance.now() - t0),
          },
        ]);
      } catch (e) {
        if (!cancelled) setErr(errText(e));
      } finally {
        if (!cancelled) setBusy(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [cand]);

  const getContext = useCallback(
    async (): Promise<ContextDoc[]> => [
      {
        id: cand.id,
        bucket: CANDIDATES_BUCKET,
        label: "Your profile",
        text: `CANDIDATE PROFILE: ${cand.headline}. ${cand.summary} ${cand.experience} Skills: ${cand.skills.join(", ")}. ${cand.years} years. Open to ${cand.open_to.join(" or ")}; day rate £${cand.day_rate || "n/a"}, salary £${cand.salary || "n/a"}. Will work in ${cand.locations.join("/")}. Available ${cand.available_from}. Right to work: ${cand.right_to_work ? "yes" : "no"}. Working style: ${cand.traits}`,
      },
      ...fits.slice(0, 6).map(
        (f, i): ContextDoc => ({
          id: f.job.id,
          bucket: JOBS_BUCKET,
          label: f.job.title,
          text: `JOB rank ${i + 1} (fit ${pct(f.total)}): ${f.job.title} at ${f.job.company}, ${f.job.engagement}, ${f.job.mode}, ${f.job.location}, budget ${fmtRate(f.job.budget_max, f.job.engagement)}, start by ${f.job.start_by}. ${f.job.description} Must have: ${f.job.must_have.join(", ")}. You match: ${f.matched.join(", ") || "none"}; missing: ${f.missing.join(", ") || "none"}. Blockers: ${f.issues.join("; ") || "none"}. Team culture: ${f.job.culture}`,
        })
      ),
    ],
    [cand, fits]
  );

  return (
    <>
      <div className="grid gap-4 lg:grid-cols-[minmax(0,19rem)_minmax(0,1fr)]">
        <Panel title="Candidate" subtitle="Pick a profile to see roles from their side">
          <select className="input" value={candId} onChange={(e) => setCandId(e.target.value)}>
            {CANDIDATES.map((c) => (
              <option key={c.id} value={c.id}>
                {c.name} — {c.headline}
              </option>
            ))}
          </select>
          <div className="text-xs space-y-1.5 text-gray-600 dark:text-muted">
            <p>{cand.summary}</p>
            <div className="flex flex-wrap gap-1">
              {cand.skills.map((s) => (
                <span key={s} className="rounded border border-gray-200 dark:border-edge px-1.5 py-0.5 text-[10px]">{s}</span>
              ))}
            </div>
            <p className="font-mono text-[10px] text-gray-400 dark:text-faint">
              {cand.open_to.join("/")} · {cand.day_rate ? `£${cand.day_rate}/day` : ""} {cand.salary ? `£${Math.round(cand.salary / 1000)}k` : ""} · {cand.locations.join("/")} · from {cand.available_from}
            </p>
          </div>
        </Panel>
        <Panel title="Roles that fit" subtitle="Semantic fit to the job text, then skills and practical blockers">
          {err && <ErrorBanner err={err} />}
          <PipelineTrace steps={steps} />
          {busy && <Spinner />}
          <ol className="space-y-2">
            {fits.map((f) => (
              <li key={f.job.id} className="rounded-md border border-gray-200 dark:border-edge px-3 py-2.5">
                <div className="flex items-start gap-3">
                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-medium text-gray-900 dark:text-ink">{f.job.title}</div>
                    <div className="text-xs text-gray-500 dark:text-muted">
                      {f.job.company} · {f.job.location} · {f.job.engagement} · ≤ {fmtRate(f.job.budget_max, f.job.engagement)}
                    </div>
                    <div className="mt-1 flex flex-wrap gap-1">
                      {f.matched.map((x) => (
                        <span key={x} className="rounded bg-ok/15 text-ok px-1.5 py-0.5 text-[10px]">✓ {x}</span>
                      ))}
                      {f.missing.map((x) => (
                        <span key={x} className="rounded bg-bad/10 text-bad px-1.5 py-0.5 text-[10px]">✗ {x}</span>
                      ))}
                      {f.issues.map((x) => (
                        <span key={x} className="rounded border border-warn/40 text-warn px-1.5 py-0.5 text-[10px]">⚠ {x}</span>
                      ))}
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="text-lg font-semibold tabular-nums text-gray-900 dark:text-ink">{pct(f.total)}</div>
                    <div className="text-[10px] text-gray-400 dark:text-faint">fit</div>
                  </div>
                </div>
              </li>
            ))}
          </ol>
        </Panel>
      </div>
      <GroundedChat
        title="Career coach"
        subtitle="Grounded on this profile + the top-ranked roles"
        system={COACH_SYSTEM}
        getContext={getContext}
        resetKey={`${cand.id}:${fits.length}`}
        disabledReason={busy ? "Matching roles…" : fits.length === 0 ? "No roles to discuss yet" : undefined}
        suggestions={[
          "Which role suits me best, and why?",
          "What should I highlight when applying for the top role?",
          "Which skill gap would open up the most roles?",
          "Draft a short cover note for the top role",
        ]}
      />
    </>
  );
}

const COACH_SYSTEM = `You are a career coach. [0] is the candidate's profile; [1], [2], ... are open roles ranked by fit.
Use only facts from the documents and cite them like [1]. Be encouraging but honest about blockers such as budget, location or visa sponsorship. Prefer short bullet lists.`;

// ---------------------------------------------------------------------------

function pct(x: number) {
  return `${Math.round(x * 100)}%`;
}

function errText(e: unknown): string {
  if (e instanceof ApiError) return `${e.code}: ${e.body.slice(0, 300)}`;
  return (e as Error).message;
}
