/**
 * NebulaDB Labs — shared types.
 *
 * A *lab* is a reusable experiment (meaning vs words, model shoot-out, …).
 * A *pack* is one business's demo data plus the labelled questions, safety
 * rules and scripted reasoning plans the labs need. Every lab works with
 * every pack.
 *
 * Pack files use `import type` only so Node can load them directly
 * (`node --test`) for the consistency checks.
 */

export type LabId =
  | "semantic"
  | "models"
  | "filters"
  | "rag"
  | "reason"
  | "refuse"
  | "scale"
  | "map";

export type PackId = "campus" | "carehome" | "chat" | "cms" | "joyai" | "kisaan" | "source";

/** Metadata values a pack doc may carry. Arrays are queried with array_contains. */
export type MetaValue = string | number | boolean | string[];

export interface PackDoc {
  /** Unique within the pack, e.g. "campus-pol-07". */
  id: string;
  /** 1–4 sentences of realistic text. This is what gets embedded. */
  text: string;
  /** Flat metadata only; every key used in a filter must be present on every doc. */
  metadata: Record<string, MetaValue>;
}

/** A question with the doc ids a good retriever should return (graded relevance not needed). */
export interface LabelledQuestion {
  id: string;
  q: string;
  /** Ids of the docs that answer it (1–3). The first is the best answer. */
  expect: string[];
  /**
   * Why this question is interesting: "paraphrase" (no shared key words),
   * "keyword" (exact code or name), "multilingual", "typo", "filtered" …
   */
  kind: "paraphrase" | "keyword" | "multilingual" | "typo" | "filtered" | "plain";
  /** Language / script of the question, e.g. "en", "hi-Deva", "hi-Latn", "pa-Guru". */
  lang?: string;
  /** For kind "filtered": the metadata filter that makes it answerable. */
  filters?: Record<string, string>;
}

/** How the app must handle a question it should NOT answer from the model. */
export type RefuseKind = "safety" | "out_of_scope" | "acl";

export interface RefuseQuestion {
  id: string;
  q: string;
  expect: RefuseKind;
  /** For "safety": the SafetyRule id that must fire. */
  rule?: string;
  /** For "acl": the persona asking (the docs they must not see are those outside persona.filter). */
  persona?: string;
}

/** A fixed, non-model response triggered by words in the question. Lives in code, never in a prompt. */
export interface SafetyRule {
  id: string;
  label: string;
  /** Lower-case phrases; a match on any (substring, case-insensitive) fires the rule. */
  triggers: string[];
  /** The exact card text shown instead of any model output. */
  card: string;
}

/** A metadata filter the Filters lab can toggle. */
export interface FilterField {
  field: string;
  label: string;
  /** "eq": field = 'v'; "contains": array_contains(field, 'v'); "max": field <= n; "bool": field = TRUE. */
  op: "eq" | "contains" | "max" | "bool";
  /** Options for eq / contains. */
  values?: string[];
}

/** Who is asking — used by the ACL refusals and the "persona" filter in RAG. */
export interface Persona {
  id: string;
  label: string;
  /** Metadata equality the persona is restricted to, e.g. { home: "Elm Lodge" }. */
  filter: Record<string, string>;
}

/** One step of a scripted reasoning plan (used when no real LLM is configured, and as the reference plan). */
export type ReasonStep =
  | { kind: "sql"; why: string; sql: string }
  | { kind: "search"; why: string; query: string; k: number };

export interface ReasonTask {
  id: string;
  q: string;
  /** Reference plan. Every SQL must be valid NebulaDB SQL against the pack bucket. */
  plan: ReasonStep[];
}

export interface Pack {
  id: PackId;
  name: string;
  /** e.g. "Schools & universities". */
  industry: string;
  /** One line shown on the pack picker. */
  tagline: string;
  /** NebulaDB bucket the pack seeds into. Always `lab_<id>`. */
  bucket: string;
  /** Bump whenever docs change so ensureSeeded rewrites them. */
  version: string;
  /** The default question for every lab. */
  sample: string;
  /** Domain rules appended to the grounded RAG system prompt. */
  system: string;
  /** Metadata field the Map lab colours points by. */
  colorField: string;
  docs: PackDoc[];
  questions: LabelledQuestion[];
  refuse: RefuseQuestion[];
  safety: SafetyRule[];
  filters: FilterField[];
  personas: Persona[];
  reason: ReasonTask[];
  /** One concrete challenge per lab, phrased for this pack. */
  challenges: Record<LabId, string>;
}
