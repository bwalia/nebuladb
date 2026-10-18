/**
 * Talent Match demo corpus — fictional UK tech roles and candidates.
 *
 * Every person and company here is invented. Candidates deliberately
 * cover the awkward real-world cases a matcher must handle: the ideal
 * fit who is 8% over budget, the cheap junior, the strong engineer
 * with a 12-week notice period, the one who needs visa sponsorship.
 *
 * Protected characteristics (age, gender, ethnicity, …) are not in
 * the data at all, so neither retrieval nor the LLM can use them.
 */
import type { SeedDoc } from "../seed";

export const TALENT_SEED_VERSION = "talent-v1";
export const JOBS_BUCKET = "talent_jobs";
export const CANDIDATES_BUCKET = "talent_candidates";
export const TRAITS_BUCKET = "talent_traits";

export type Engagement = "contract" | "permanent";
export type WorkMode = "remote" | "hybrid" | "onsite";

export interface Job {
  id: string;
  title: string;
  company: string;
  location: string;
  mode: WorkMode;
  engagement: Engagement;
  /** £/day for contract roles, £/year for permanent. */
  budget_max: number;
  min_years: number;
  must_have: string[];
  nice_to_have: string[];
  description: string;
  /** What the team is like — matched against candidates' working style. */
  culture: string;
  start_by: string;
  sponsorship: boolean;
  posted_at: string;
}

export interface Candidate {
  id: string;
  name: string;
  headline: string;
  location: string;
  /** Places they'll work; "Remote" means fully-remote roles are fine. */
  locations: string[];
  years: number;
  skills: string[];
  open_to: Engagement[];
  day_rate: number;
  salary: number;
  notice_weeks: number;
  available_from: string;
  right_to_work: boolean;
  summary: string;
  experience: string;
  /** Character / working style, embedded separately as its own facet. */
  traits: string;
}

/** Canonical skill names; job posts typed by users are normalised through this. */
const ALIASES: Record<string, string> = {
  k8s: "kubernetes",
  postgres: "postgresql",
  psql: "postgresql",
  ts: "typescript",
  js: "javascript",
  "react.js": "react",
  reactjs: "react",
  golang: "go",
  gcp: "google cloud",
  "amazon web services": "aws",
  tf: "terraform",
  "ci/cd": "ci-cd",
  cicd: "ci-cd",
  ml: "machine learning",
  llms: "llm",
  "large language models": "llm",
  "next.js": "nextjs",
  "node.js": "node",
  nodejs: "node",
  "iso 27001": "iso27001",
  "soc 2": "soc2",
};

export function normSkill(s: string): string {
  const k = s.trim().toLowerCase().replace(/\s+/g, " ");
  return ALIASES[k] ?? k;
}

export function parseSkills(s: string): string[] {
  return [...new Set(s.split(/[,;\n]/).map(normSkill).filter(Boolean))];
}

export const JOBS: Job[] = [
  {
    id: "job-rust-payments",
    title: "Senior Rust Engineer — Payments Ledger",
    company: "Fenwick Pay",
    location: "London",
    mode: "hybrid",
    engagement: "contract",
    budget_max: 700,
    min_years: 6,
    must_have: ["rust", "postgresql", "distributed systems"],
    nice_to_have: ["kafka", "kubernetes", "payments", "event sourcing"],
    description:
      "Outside-IR35 6-month contract to rebuild our double-entry payments ledger in Rust. You'll own the posting engine (idempotent writes, exactly-once settlement against Faster Payments and card schemes), design the Postgres schema and partitioning, and harden reconciliation. Expect async Rust (tokio), property-based testing and on-call for the ledger.",
    culture:
      "Small senior team that values written design docs, calm on-call, ownership end-to-end and pragmatic trade-offs over perfection. Pairing is common; ego is not.",
    start_by: "2026-11-09",
    sponsorship: false,
    posted_at: "2026-10-12",
  },
  {
    id: "job-platform-sre",
    title: "Platform / SRE Engineer",
    company: "Harbourline Logistics",
    location: "Manchester",
    mode: "remote",
    engagement: "contract",
    budget_max: 600,
    min_years: 5,
    must_have: ["kubernetes", "terraform", "aws"],
    nice_to_have: ["argo cd", "prometheus", "go", "incident management"],
    description:
      "Inside-IR35 contract, fully remote (UK hours). Migrate 40 services from ECS to EKS with Terraform and Argo CD, build golden-path templates, and define SLOs with Prometheus/Grafana alerting. You'll run blameless postmortems and reduce pager noise for product teams.",
    culture:
      "Remote-first, async by default, documentation-heavy. We want someone who teaches rather than gatekeeps and stays steady during incidents.",
    start_by: "2026-11-02",
    sponsorship: false,
    posted_at: "2026-10-10",
  },
  {
    id: "job-ml-rag",
    title: "Machine Learning Engineer — LLM & Retrieval",
    company: "Calder Health",
    location: "Edinburgh",
    mode: "hybrid",
    engagement: "permanent",
    budget_max: 105000,
    min_years: 4,
    must_have: ["python", "llm", "retrieval"],
    nice_to_have: ["pytorch", "evaluation", "vector databases", "nhs data"],
    description:
      "Build clinical-document question answering for NHS trusts: retrieval pipelines over discharge letters and guidelines, offline evaluation harnesses (faithfulness, citation accuracy), guardrails for PHI, and fine-tuning small models for on-prem deployment. Python, PyTorch, a vector database and a lot of careful measurement.",
    culture:
      "Mission-driven, evidence-first team; we publish internal write-ups of every experiment. Curiosity, humility about model limits, and patience with clinicians' feedback matter as much as skill.",
    start_by: "2027-01-11",
    sponsorship: true,
    posted_at: "2026-10-08",
  },
  {
    id: "job-frontend-react",
    title: "Senior Frontend Engineer (React/TypeScript)",
    company: "Brightwater Energy",
    location: "Bristol",
    mode: "hybrid",
    engagement: "permanent",
    budget_max: 85000,
    min_years: 5,
    must_have: ["react", "typescript", "accessibility"],
    nice_to_have: ["nextjs", "design systems", "testing library", "data visualisation"],
    description:
      "Own the customer energy dashboard used by 1.2M households: smart-meter charts, tariff switching flows and a shared design system. WCAG 2.2 AA is a hard requirement. You'll mentor two mid-level engineers and work closely with design and research.",
    culture:
      "Collaborative and user-obsessed; we sit in on research sessions. We value clear communication, kindness in code review and steady delivery over heroics.",
    start_by: "2026-12-07",
    sponsorship: false,
    posted_at: "2026-10-05",
  },
  {
    id: "job-data-eng",
    title: "Data Engineer — Analytics Platform",
    company: "Larkspur Retail",
    location: "Leeds",
    mode: "hybrid",
    engagement: "contract",
    budget_max: 550,
    min_years: 4,
    must_have: ["sql", "dbt", "python"],
    nice_to_have: ["snowflake", "airflow", "spark", "data modelling"],
    description:
      "3-month contract (likely extension) to move 200 legacy SQL reports into dbt on Snowflake, orchestrate with Airflow, add data tests and lineage, and model loyalty and basket data for the pricing team.",
    culture:
      "Busy commercial environment; stakeholders change their minds. We need someone organised, good at saying no politely, and comfortable presenting to non-technical managers.",
    start_by: "2026-10-26",
    sponsorship: false,
    posted_at: "2026-10-14",
  },
  {
    id: "job-security",
    title: "Cloud Security Engineer",
    company: "Oakmere Insurance",
    location: "London",
    mode: "onsite",
    engagement: "permanent",
    budget_max: 110000,
    min_years: 6,
    must_have: ["aws", "cloud security", "iso27001"],
    nice_to_have: ["soc2", "terraform", "threat modelling", "siem"],
    description:
      "Lead cloud security for a regulated insurer: AWS guardrails (SCPs, GuardDuty, Security Hub), threat modelling with product teams, ISO 27001 surveillance audits and FCA operational-resilience evidence. Onsite in the City four days a week due to client data handling.",
    culture:
      "Formal, regulated and risk-aware. We need someone meticulous, discreet and able to influence senior stakeholders without drama.",
    start_by: "2027-01-04",
    sponsorship: false,
    posted_at: "2026-10-01",
  },
  {
    id: "job-ios",
    title: "iOS Engineer (Swift)",
    company: "Wayfarer Travel",
    location: "London",
    mode: "hybrid",
    engagement: "contract",
    budget_max: 575,
    min_years: 4,
    must_have: ["swift", "swiftui", "ios"],
    nice_to_have: ["combine", "offline sync", "app store releases", "kotlin"],
    description:
      "Rebuild the booking app in SwiftUI with offline-first trip itineraries, push notifications and Apple Pay. Ship fortnightly to the App Store with feature flags.",
    culture: "Product-minded mobile squad; we care about polish and fast feedback from users. Playful, informal, high-trust.",
    start_by: "2026-11-16",
    sponsorship: false,
    posted_at: "2026-10-11",
  },
  {
    id: "job-eng-manager",
    title: "Engineering Manager — Core Platform",
    company: "Fenwick Pay",
    location: "London",
    mode: "hybrid",
    engagement: "permanent",
    budget_max: 130000,
    min_years: 8,
    must_have: ["people management", "distributed systems", "hiring"],
    nice_to_have: ["payments", "rust", "incident management", "okrs"],
    description:
      "Manage two teams (11 engineers) owning the ledger, settlement and reconciliation services. Grow the team from 11 to 16, run incident reviews, and partner with product on a 12-month roadmap to multi-currency.",
    culture:
      "We promote managers who coach, give direct but kind feedback, and protect focus time. Psychological safety is measured in our engagement survey.",
    start_by: "2027-02-01",
    sponsorship: false,
    posted_at: "2026-09-28",
  },
  {
    id: "job-analytics-eng",
    title: "Analytics Engineer (Looker)",
    company: "Calder Health",
    location: "Remote",
    mode: "remote",
    engagement: "permanent",
    budget_max: 70000,
    min_years: 3,
    must_have: ["sql", "looker", "data modelling"],
    nice_to_have: ["dbt", "bigquery", "python", "nhs data"],
    description:
      "Own the semantic layer (LookML) for operational dashboards used by hospital operations teams: waiting lists, bed occupancy, discharge delays. Build dbt models on BigQuery with tests.",
    culture: "Remote team spread across the UK; we value reliability, clear written updates and empathy for frontline NHS staff.",
    start_by: "2026-12-01",
    sponsorship: true,
    posted_at: "2026-10-09",
  },
  {
    id: "job-product-designer",
    title: "Senior Product Designer",
    company: "Brightwater Energy",
    location: "Bristol",
    mode: "hybrid",
    engagement: "contract",
    budget_max: 525,
    min_years: 5,
    must_have: ["figma", "design systems", "user research"],
    nice_to_have: ["accessibility", "prototyping", "service design"],
    description:
      "4-month contract to lead design for tariff switching and smart-meter onboarding: research with vulnerable customers, Figma prototypes, and extending the design system with accessible components.",
    culture: "Research-led, collaborative, and kind. We want a designer who brings evidence to debates and includes engineers early.",
    start_by: "2026-11-23",
    sponsorship: false,
    posted_at: "2026-10-13",
  },
];

const c = (x: Candidate) => x;

export const CANDIDATES: Candidate[] = [
  // --- Rust / backend ---------------------------------------------------
  c({
    id: "C-01", name: "Amara Okafor", headline: "Staff backend engineer — Rust, payments ledgers",
    location: "London", locations: ["London", "Remote"], years: 11,
    skills: ["rust", "postgresql", "distributed systems", "kafka", "event sourcing", "payments", "tokio"],
    open_to: ["contract"], day_rate: 760, salary: 0, notice_weeks: 2, available_from: "2026-11-02", right_to_work: true,
    summary: "Built the card-settlement ledger at a UK challenger bank in Rust; led migration from a Java monolith with zero reconciliation breaks.",
    experience: "2022–2026 Staff Engineer, challenger bank: Rust ledger (40k TPS), Postgres partitioning, Kafka outbox. 2017–2022 Senior Engineer, payments processor: Faster Payments gateway, ISO 20022.",
    traits: "Writes thorough design docs before coding, calm and methodical on-call, prefers small senior teams, mentors through pairing, direct but kind in review.",
  }),
  c({
    id: "C-02", name: "Tomasz Wiśniewski", headline: "Senior Rust engineer — trading systems",
    location: "London", locations: ["London"], years: 8,
    skills: ["rust", "c++", "distributed systems", "postgresql", "low latency", "kubernetes"],
    open_to: ["contract", "permanent"], day_rate: 690, salary: 115000, notice_weeks: 4, available_from: "2026-11-16", right_to_work: true,
    summary: "Low-latency order routing in Rust and C++; strong on correctness and benchmarking.",
    experience: "2021–2026 Senior Engineer, FX trading venue: Rust matching engine, Postgres trade store. 2018–2021 C++ developer, market data vendor.",
    traits: "Perfectionist about performance, very independent, prefers deep focus over meetings, can be blunt, excellent at incident root-cause analysis.",
  }),
  c({
    id: "C-03", name: "Priya Raman", headline: "Backend engineer — Rust & Go, fintech",
    location: "Reading", locations: ["London", "Reading", "Remote"], years: 6,
    skills: ["rust", "go", "postgresql", "kafka", "payments", "distributed systems"],
    open_to: ["contract"], day_rate: 620, salary: 0, notice_weeks: 1, available_from: "2026-10-26", right_to_work: true,
    summary: "Payment-orchestration services in Go, last 2 years moving hot paths to Rust; strong testing discipline.",
    experience: "2023–2026 Contractor, open-banking provider: Rust idempotency service, Kafka consumers. 2020–2023 Backend Engineer, lending platform (Go, Postgres).",
    traits: "Pragmatic, collaborative, enjoys pairing, communicates early when things slip, steady under pressure, asks good questions.",
  }),
  c({
    id: "C-04", name: "Daniel Mensah", headline: "Mid-level Rust developer",
    location: "Croydon", locations: ["London", "Remote"], years: 3,
    skills: ["rust", "postgresql", "docker", "tokio"],
    open_to: ["contract", "permanent"], day_rate: 420, salary: 62000, notice_weeks: 2, available_from: "2026-11-02", right_to_work: true,
    summary: "Three years of async Rust on internal APIs; eager to work on financial systems.",
    experience: "2023–2026 Software Engineer, insurtech: Rust claims API, Postgres. 2022 graduate scheme.",
    traits: "Enthusiastic, fast learner, seeks feedback, sometimes over-commits, great team energy.",
  }),
  c({
    id: "C-05", name: "Elena Petrova", headline: "Principal engineer — distributed databases (Rust)",
    location: "Cambridge", locations: ["Remote"], years: 14,
    skills: ["rust", "distributed systems", "postgresql", "raft", "kubernetes", "event sourcing"],
    open_to: ["contract"], day_rate: 850, salary: 0, notice_weeks: 12, available_from: "2027-01-18", right_to_work: true,
    summary: "Designed a Raft-replicated storage engine in Rust; deep Postgres internals knowledge.",
    experience: "2019–2026 Principal Engineer, database start-up: storage engine, replication. 2012–2019 Postgres consultancy.",
    traits: "Deep thinker, prefers remote async work, excellent writer, low patience for process overhead, generous mentor.",
  }),
  // --- Platform / SRE ---------------------------------------------------
  c({
    id: "C-06", name: "Liam O'Connor", headline: "Senior SRE — Kubernetes, Terraform, AWS",
    location: "Manchester", locations: ["Manchester", "Remote"], years: 9,
    skills: ["kubernetes", "terraform", "aws", "argo cd", "prometheus", "go", "incident management"],
    open_to: ["contract"], day_rate: 580, salary: 0, notice_weeks: 1, available_from: "2026-10-26", right_to_work: true,
    summary: "Led ECS→EKS migration for 60 services; built golden paths with Argo CD and Backstage.",
    experience: "2022–2026 Lead SRE contractor, e-commerce: EKS, Terraform modules, SLOs. 2017–2022 DevOps, telecoms.",
    traits: "Teacher at heart, writes runbooks people actually read, calm incident commander, remote-first and async-friendly.",
  }),
  c({
    id: "C-07", name: "Fatima Zahra", headline: "Platform engineer — GCP & Kubernetes",
    location: "Birmingham", locations: ["Birmingham", "Remote"], years: 6,
    skills: ["kubernetes", "terraform", "google cloud", "prometheus", "python"],
    open_to: ["contract", "permanent"], day_rate: 540, salary: 78000, notice_weeks: 4, available_from: "2026-11-16", right_to_work: true,
    summary: "GKE platform for a media company; strong Terraform, limited AWS exposure.",
    experience: "2021–2026 Platform Engineer, broadcaster: GKE, Terraform, Prometheus. 2019–2021 Systems engineer.",
    traits: "Organised, documentation-first, collaborative, prefers clear priorities, quietly reliable.",
  }),
  c({
    id: "C-08", name: "Marcus Bell", headline: "DevOps lead — AWS, Kubernetes, cost optimisation",
    location: "Leeds", locations: ["Leeds", "Manchester", "Remote"], years: 12,
    skills: ["aws", "kubernetes", "terraform", "ci-cd", "finops", "incident management"],
    open_to: ["contract"], day_rate: 650, salary: 0, notice_weeks: 2, available_from: "2026-11-02", right_to_work: true,
    summary: "Cut AWS spend 38% at a scale-up; heavy Terraform and EKS.",
    experience: "2020–2026 DevOps Lead contractor: EKS, Karpenter, cost programmes. 2014–2020 Infrastructure engineer.",
    traits: "Commercially minded, decisive, likes ownership, can be impatient with slow decision-making, strong stakeholder skills.",
  }),
  c({
    id: "C-09", name: "Sofia Alvarez", headline: "SRE — observability & incident response",
    location: "Lisbon", locations: ["Remote"], years: 7,
    skills: ["kubernetes", "prometheus", "aws", "go", "incident management", "terraform"],
    open_to: ["contract"], day_rate: 520, salary: 0, notice_weeks: 2, available_from: "2026-11-02", right_to_work: false,
    summary: "Built SLO programme and on-call rotations for a 200-engineer org; strong Go tooling.",
    experience: "2021–2026 SRE, SaaS company: Prometheus, Thanos, PagerDuty. 2018–2021 Backend engineer.",
    traits: "Empathetic, data-driven about reliability, runs excellent blameless postmortems, async communicator.",
  }),
  // --- ML / LLM ---------------------------------------------------------
  c({
    id: "C-10", name: "Dr. Hannah Clarke", headline: "ML engineer — clinical NLP & retrieval",
    location: "Edinburgh", locations: ["Edinburgh", "Glasgow", "Remote"], years: 7,
    skills: ["python", "pytorch", "llm", "retrieval", "evaluation", "nhs data", "vector databases"],
    open_to: ["permanent"], day_rate: 0, salary: 98000, notice_weeks: 8, available_from: "2026-12-14", right_to_work: true,
    summary: "PhD in clinical NLP; built discharge-summary retrieval and evaluation for two NHS boards.",
    experience: "2021–2026 Senior ML Engineer, health-tech: RAG over clinical letters, faithfulness evals. 2017–2021 PhD, clinical text mining.",
    traits: "Evidence-first, careful about model limitations, patient with clinicians, publishes experiment write-ups, quietly persistent.",
  }),
  c({
    id: "C-11", name: "Arjun Mehta", headline: "LLM engineer — RAG systems, Python",
    location: "London", locations: ["London", "Remote"], years: 5,
    skills: ["python", "llm", "retrieval", "vector databases", "fastapi", "evaluation"],
    open_to: ["permanent", "contract"], day_rate: 650, salary: 95000, notice_weeks: 4, available_from: "2026-11-30", right_to_work: false,
    summary: "Shipped legal-document RAG used by 3,000 lawyers; strong evals and prompt engineering.",
    experience: "2023–2026 ML Engineer, legal-tech: hybrid retrieval, reranking, citation checks. 2021–2023 Data scientist.",
    traits: "Curious, fast-moving, experiment-driven, enjoys demos, sometimes over-optimistic on timelines.",
  }),
  c({
    id: "C-12", name: "Grace Liu", headline: "Machine learning researcher → engineer",
    location: "Edinburgh", locations: ["Edinburgh"], years: 4,
    skills: ["python", "pytorch", "machine learning", "evaluation", "llm"],
    open_to: ["permanent"], day_rate: 0, salary: 82000, notice_weeks: 4, available_from: "2026-12-07", right_to_work: true,
    summary: "Fine-tuned small language models for on-device use; moving from research to production.",
    experience: "2022–2026 Research Engineer, university AI lab: distillation, quantisation. 2020–2022 MSc.",
    traits: "Rigorous, humble, thorough experimenter, learning production practices, collaborative.",
  }),
  c({
    id: "C-13", name: "Kwame Asante", headline: "Data scientist — NLP, healthcare",
    location: "Newcastle", locations: ["Newcastle", "Remote"], years: 6,
    skills: ["python", "machine learning", "nhs data", "sql", "retrieval"],
    open_to: ["permanent"], day_rate: 0, salary: 75000, notice_weeks: 4, available_from: "2026-12-01", right_to_work: true,
    summary: "NHS trust data scientist; built triage text classifiers and an early retrieval prototype.",
    experience: "2020–2026 Data Scientist, NHS trust: referrals classification, information governance. 2018–2020 Analyst.",
    traits: "Deeply mission-driven, empathetic with clinicians, careful with patient data, steady and reliable.",
  }),
  // --- Frontend ---------------------------------------------------------
  c({
    id: "C-14", name: "Chloe Bennett", headline: "Senior frontend engineer — React, accessibility",
    location: "Bristol", locations: ["Bristol", "Bath", "Remote"], years: 8,
    skills: ["react", "typescript", "accessibility", "design systems", "testing library", "nextjs"],
    open_to: ["permanent"], day_rate: 0, salary: 82000, notice_weeks: 4, available_from: "2026-11-30", right_to_work: true,
    summary: "Led WCAG 2.2 AA remediation of a utility's customer portal; maintains a 60-component design system.",
    experience: "2021–2026 Senior Frontend, energy supplier: React/TS, Storybook. 2018–2021 Frontend dev, agency.",
    traits: "User-obsessed, kind and thorough in code review, mentors juniors, steady delivery, attends research sessions.",
  }),
  c({
    id: "C-15", name: "Oliver Grant", headline: "Frontend engineer — React, data visualisation",
    location: "Cardiff", locations: ["Cardiff", "Bristol", "Remote"], years: 5,
    skills: ["react", "typescript", "data visualisation", "d3", "nextjs"],
    open_to: ["permanent", "contract"], day_rate: 500, salary: 72000, notice_weeks: 4, available_from: "2026-12-01", right_to_work: true,
    summary: "Built interactive energy-usage charts with D3 and React; less depth on accessibility.",
    experience: "2022–2026 Frontend Engineer, climate-tech: dashboards, D3. 2021 bootcamp + agency.",
    traits: "Creative, detail-oriented about visuals, collaborative, prefers clear specs.",
  }),
  c({
    id: "C-16", name: "Isabel Moreno", headline: "Staff frontend engineer — design systems",
    location: "London", locations: ["London"], years: 10,
    skills: ["react", "typescript", "design systems", "accessibility", "testing library"],
    open_to: ["permanent"], day_rate: 0, salary: 98000, notice_weeks: 12, available_from: "2027-01-25", right_to_work: true,
    summary: "Owned a bank's design system used by 40 teams; accessibility champion.",
    experience: "2019–2026 Staff Engineer, high-street bank: design system, a11y audits. 2015–2019 Frontend lead.",
    traits: "Strategic, influential across teams, high standards, prefers London office culture, strong communicator.",
  }),
  c({
    id: "C-17", name: "Ben Hughes", headline: "Full-stack developer — TypeScript, Node",
    location: "Swindon", locations: ["Bristol", "Swindon", "Remote"], years: 4,
    skills: ["typescript", "react", "node", "postgresql"],
    open_to: ["permanent", "contract"], day_rate: 450, salary: 65000, notice_weeks: 4, available_from: "2026-11-23", right_to_work: true,
    summary: "Generalist building internal tools; wants to specialise in frontend.",
    experience: "2022–2026 Developer, logistics SME: React admin tools, Node APIs.",
    traits: "Friendly, adaptable, reliable, quiet in large groups, keen to learn accessibility.",
  }),
  // --- Data -------------------------------------------------------------
  c({
    id: "C-18", name: "Rachel Adeyemi", headline: "Senior data engineer — dbt, Snowflake",
    location: "Leeds", locations: ["Leeds", "Manchester", "Remote"], years: 7,
    skills: ["sql", "dbt", "python", "snowflake", "airflow", "data modelling"],
    open_to: ["contract"], day_rate: 560, salary: 0, notice_weeks: 1, available_from: "2026-10-26", right_to_work: true,
    summary: "Migrated 300 legacy reports to dbt on Snowflake for a supermarket group.",
    experience: "2022–2026 Data Engineering contractor, retail: dbt, Snowflake, Airflow. 2018–2022 BI developer.",
    traits: "Organised, great with non-technical stakeholders, pushes back politely on scope creep, clear presenter.",
  }),
  c({
    id: "C-19", name: "Sam Patel", headline: "Data engineer — Spark & Python",
    location: "Sheffield", locations: ["Sheffield", "Leeds", "Remote"], years: 5,
    skills: ["python", "spark", "sql", "airflow", "aws"],
    open_to: ["contract", "permanent"], day_rate: 480, salary: 68000, notice_weeks: 2, available_from: "2026-11-02", right_to_work: true,
    summary: "Batch and streaming pipelines in Spark; started using dbt last year.",
    experience: "2021–2026 Data Engineer, ad-tech: Spark on EMR, Airflow. 2019–2021 Analyst.",
    traits: "Heads-down builder, prefers technical work over stakeholder meetings, reliable, dry humour.",
  }),
  c({
    id: "C-20", name: "Nadia Hussain", headline: "Analytics engineer — Looker, BigQuery",
    location: "Glasgow", locations: ["Glasgow", "Remote"], years: 5,
    skills: ["sql", "looker", "data modelling", "dbt", "bigquery"],
    open_to: ["permanent"], day_rate: 0, salary: 66000, notice_weeks: 4, available_from: "2026-11-30", right_to_work: true,
    summary: "Built LookML semantic layer for a care-home operator's occupancy and staffing dashboards.",
    experience: "2021–2026 Analytics Engineer, social-care provider: LookML, dbt, BigQuery. 2019–2021 Data analyst.",
    traits: "Empathetic towards frontline users, writes clear updates, dependable, enjoys remote work.",
  }),
  c({
    id: "C-21", name: "George Whitfield", headline: "BI developer — SQL, Power BI",
    location: "Leeds", locations: ["Leeds"], years: 9,
    skills: ["sql", "power bi", "data modelling", "ssis"],
    open_to: ["contract"], day_rate: 450, salary: 0, notice_weeks: 1, available_from: "2026-10-26", right_to_work: true,
    summary: "Long-time BI developer on Microsoft stack; no dbt yet.",
    experience: "2015–2026 BI Developer, retail and finance: SQL Server, SSIS, Power BI.",
    traits: "Patient, methodical, prefers stable requirements, excellent at explaining numbers to managers.",
  }),
  // --- Security ---------------------------------------------------------
  c({
    id: "C-22", name: "Victoria Shaw", headline: "Cloud security lead — AWS, ISO 27001",
    location: "London", locations: ["London"], years: 10,
    skills: ["aws", "cloud security", "iso27001", "soc2", "threat modelling", "terraform"],
    open_to: ["permanent"], day_rate: 0, salary: 112000, notice_weeks: 12, available_from: "2027-01-18", right_to_work: true,
    summary: "Ran ISO 27001 and SOC 2 programmes at an insurtech; built AWS SCP guardrails.",
    experience: "2020–2026 Head of Cloud Security, insurtech: GuardDuty, Security Hub, audits. 2016–2020 Security engineer, Big Four.",
    traits: "Meticulous, discreet, influences executives calmly, comfortable in formal regulated settings.",
  }),
  c({
    id: "C-23", name: "Ahmed Karim", headline: "Security engineer — detection & response",
    location: "London", locations: ["London", "Remote"], years: 6,
    skills: ["siem", "aws", "cloud security", "python", "threat modelling"],
    open_to: ["permanent", "contract"], day_rate: 600, salary: 92000, notice_weeks: 4, available_from: "2026-11-30", right_to_work: true,
    summary: "Built SIEM detections and AWS incident response playbooks; no ISO audit ownership yet.",
    experience: "2021–2026 Security Engineer, fintech: Splunk, AWS IR. 2019–2021 SOC analyst.",
    traits: "Curious, hands-on, enjoys red-team exercises, prefers hybrid working.",
  }),
  // --- Mobile -----------------------------------------------------------
  c({
    id: "C-24", name: "Jessica Tan", headline: "Senior iOS engineer — SwiftUI",
    location: "London", locations: ["London", "Remote"], years: 7,
    skills: ["swift", "swiftui", "ios", "combine", "offline sync", "app store releases"],
    open_to: ["contract"], day_rate: 600, salary: 0, notice_weeks: 2, available_from: "2026-11-09", right_to_work: true,
    summary: "Rebuilt a rail-ticketing app in SwiftUI with offline tickets; 4.8★ App Store rating.",
    experience: "2022–2026 iOS contractor, transport: SwiftUI, offline sync. 2018–2022 iOS developer, agency.",
    traits: "Polish-obsessed, product-minded, playful, enjoys fast feedback loops.",
  }),
  c({
    id: "C-25", name: "Ryan Murphy", headline: "Mobile developer — iOS & Android",
    location: "Brighton", locations: ["London", "Brighton", "Remote"], years: 5,
    skills: ["swift", "ios", "kotlin", "swiftui", "app store releases"],
    open_to: ["contract", "permanent"], day_rate: 520, salary: 70000, notice_weeks: 1, available_from: "2026-10-26", right_to_work: true,
    summary: "Cross-platform native developer; shipped travel and fitness apps.",
    experience: "2021–2026 Mobile developer, agency: 9 apps on both stores. 2020 bootcamp.",
    traits: "Easy-going, versatile, informal, good with designers, sometimes light on tests.",
  }),
  // --- Leadership ------------------------------------------------------
  c({
    id: "C-26", name: "Catherine Doyle", headline: "Engineering manager — payments platforms",
    location: "London", locations: ["London"], years: 15,
    skills: ["people management", "hiring", "distributed systems", "payments", "incident management", "okrs"],
    open_to: ["permanent"], day_rate: 0, salary: 128000, notice_weeks: 12, available_from: "2027-01-25", right_to_work: true,
    summary: "Managed 3 teams (18 engineers) at a card issuer; grew org from 8 to 18.",
    experience: "2019–2026 Engineering Manager, card issuer: settlement, disputes. 2011–2019 Engineer → tech lead.",
    traits: "Coaching-focused, direct but kind feedback, champions psychological safety, protects focus time.",
  }),
  c({
    id: "C-27", name: "Mohammed Rahman", headline: "Tech lead → first-time manager",
    location: "London", locations: ["London", "Remote"], years: 9,
    skills: ["distributed systems", "rust", "go", "hiring", "payments"],
    open_to: ["permanent"], day_rate: 0, salary: 115000, notice_weeks: 4, available_from: "2026-12-07", right_to_work: true,
    summary: "Tech lead for a 6-person payments team; has run hiring loops but no direct reports yet.",
    experience: "2021–2026 Tech Lead, payments start-up: Rust/Go services. 2017–2021 Senior engineer.",
    traits: "Hands-on, trusted by engineers, learning to delegate, warm and approachable.",
  }),
  // --- Design -----------------------------------------------------------
  c({
    id: "C-28", name: "Lucy Fairbairn", headline: "Senior product designer — energy & utilities",
    location: "Bristol", locations: ["Bristol", "Remote"], years: 8,
    skills: ["figma", "design systems", "user research", "accessibility", "prototyping"],
    open_to: ["contract"], day_rate: 510, salary: 0, notice_weeks: 1, available_from: "2026-11-16", right_to_work: true,
    summary: "Designed smart-meter onboarding for a big-six supplier; ran research with vulnerable customers.",
    experience: "2022–2026 Design contractor, utilities: research, Figma, DS contributions. 2017–2022 UX designer, GDS-style public services.",
    traits: "Research-led, kind, brings evidence to debates, involves engineers early.",
  }),
  c({
    id: "C-29", name: "Theo Laurent", headline: "Product designer — visual & interaction",
    location: "London", locations: ["London", "Bristol"], years: 6,
    skills: ["figma", "prototyping", "design systems", "motion design"],
    open_to: ["contract"], day_rate: 560, salary: 0, notice_weeks: 2, available_from: "2026-11-23", right_to_work: true,
    summary: "Strong visual and interaction designer; limited formal research experience.",
    experience: "2021–2026 Product designer, fintech and media. 2019–2021 Agency designer.",
    traits: "Opinionated, high craft, fast, prefers autonomy over consensus.",
  }),
  // --- Extra depth for the busiest roles -------------------------------
  c({
    id: "C-30", name: "Joanna Kowalczyk", headline: "Backend contractor — Rust, Kafka, banking",
    location: "Milton Keynes", locations: ["London", "Remote"], years: 7,
    skills: ["rust", "kafka", "postgresql", "distributed systems", "kubernetes"],
    open_to: ["contract"], day_rate: 680, salary: 0, notice_weeks: 4, available_from: "2026-11-16", right_to_work: true,
    summary: "Real-time fraud scoring service in Rust consuming Kafka; Postgres and Kubernetes in production.",
    experience: "2022–2026 Contractor, retail bank: Rust fraud engine. 2019–2022 Scala engineer, insurer.",
    traits: "Steady, collaborative, writes ADRs, prefers hybrid with two office days, dislikes heroics.",
  }),
  c({
    id: "C-31", name: "Ethan Brooks", headline: "SRE — Kubernetes & Terraform (Azure)",
    location: "Liverpool", locations: ["Liverpool", "Manchester", "Remote"], years: 5,
    skills: ["kubernetes", "terraform", "azure", "prometheus", "ci-cd"],
    open_to: ["contract"], day_rate: 500, salary: 0, notice_weeks: 1, available_from: "2026-10-26", right_to_work: true,
    summary: "AKS platform engineer; Terraform heavy; little AWS.",
    experience: "2021–2026 Platform engineer, housing association and gov contracts: AKS, Terraform.",
    traits: "Reliable, helpful, async-friendly, still building incident leadership experience.",
  }),
  c({
    id: "C-32", name: "Aisha Bello", headline: "Data engineer — dbt, Airflow, Python",
    location: "Manchester", locations: ["Manchester", "Leeds", "Remote"], years: 4,
    skills: ["sql", "dbt", "python", "airflow", "snowflake", "data modelling"],
    open_to: ["contract", "permanent"], day_rate: 470, salary: 64000, notice_weeks: 2, available_from: "2026-11-02", right_to_work: true,
    summary: "dbt + Snowflake for an online retailer's basket and loyalty analytics.",
    experience: "2022–2026 Data Engineer, e-commerce: dbt models, Airflow DAGs. 2021–2022 Graduate analyst.",
    traits: "Structured, communicates clearly, enjoys presenting insights, a little junior with difficult stakeholders.",
  }),
  c({
    id: "C-33", name: "Harriet Cole", headline: "Analytics engineer — NHS performance data",
    location: "Nottingham", locations: ["Remote"], years: 4,
    skills: ["sql", "looker", "dbt", "nhs data", "python", "data modelling"],
    open_to: ["permanent"], day_rate: 0, salary: 62000, notice_weeks: 4, available_from: "2026-11-30", right_to_work: true,
    summary: "Built waiting-list and A&E dashboards in Looker for an NHS ICS.",
    experience: "2022–2026 Analytics Engineer, NHS ICS: LookML, dbt, BigQuery. 2020–2022 Information analyst.",
    traits: "Calm, empathetic to frontline staff, reliable, writes excellent documentation.",
  }),
  c({
    id: "C-34", name: "Luca Romano", headline: "ML engineer — retrieval & search",
    location: "Edinburgh", locations: ["Edinburgh", "Remote"], years: 6,
    skills: ["python", "retrieval", "llm", "vector databases", "evaluation", "pytorch"],
    open_to: ["permanent"], day_rate: 0, salary: 112000, notice_weeks: 4, available_from: "2026-12-14", right_to_work: true,
    summary: "Search relevance and RAG at a travel marketplace; built offline eval harness.",
    experience: "2021–2026 Senior ML Engineer, travel: hybrid search, LLM reranking. 2019–2021 Search engineer.",
    traits: "Analytical, measures everything, collaborative, slightly impatient with slow feedback cycles.",
  }),
  c({
    id: "C-35", name: "Megan Price", headline: "Frontend engineer — accessibility specialist",
    location: "Bath", locations: ["Bristol", "Bath"], years: 6,
    skills: ["react", "typescript", "accessibility", "testing library", "design systems"],
    open_to: ["permanent"], day_rate: 0, salary: 76000, notice_weeks: 4, available_from: "2026-12-07", right_to_work: true,
    summary: "IAAP-certified accessibility specialist; audits and fixes React apps to WCAG 2.2 AA.",
    experience: "2021–2026 Frontend Engineer, public-sector supplier: a11y audits, React. 2019–2021 QA engineer.",
    traits: "Principled, patient teacher, kind reviewer, advocates for disabled users, prefers office collaboration.",
  }),
  c({
    id: "C-36", name: "Ian Fraser", headline: "Infrastructure engineer — AWS, Terraform",
    location: "Aberdeen", locations: ["Remote"], years: 8,
    skills: ["aws", "terraform", "kubernetes", "python", "ci-cd"],
    open_to: ["contract"], day_rate: 560, salary: 0, notice_weeks: 4, available_from: "2026-11-16", right_to_work: true,
    summary: "Energy-sector AWS landing zones and EKS clusters; Terraform module author.",
    experience: "2020–2026 Infrastructure contractor, energy: AWS Control Tower, EKS. 2016–2020 Sysadmin.",
    traits: "Low-ego, methodical, prefers remote async work, solid but quiet in incidents.",
  }),
];

/** The text we embed for a job — what candidates are semantically matched against. */
export function jobText(j: Job): string {
  return [
    `${j.title} at ${j.company} (${j.engagement}, ${j.mode}, ${j.location}).`,
    j.description,
    `Must have: ${j.must_have.join(", ")}. Nice to have: ${j.nice_to_have.join(", ")}.`,
    `Minimum ${j.min_years} years.`,
  ].join(" ");
}

/** Skills + experience text: the "can they do the job" facet. */
export function candidateText(c: Candidate): string {
  return [
    `${c.headline}.`,
    c.summary,
    `Skills: ${c.skills.join(", ")}.`,
    c.experience,
    `${c.years} years experience.`,
  ].join(" ");
}

export function jobDoc(j: Job): SeedDoc {
  return {
    id: j.id,
    text: jobText(j),
    metadata: { ...j, must_have: j.must_have.map(normSkill), nice_to_have: j.nice_to_have.map(normSkill) },
  };
}

export const JOB_DOCS: SeedDoc[] = JOBS.map(jobDoc);

export const CANDIDATE_DOCS: SeedDoc[] = CANDIDATES.map((c) => ({
  id: c.id,
  text: candidateText(c),
  metadata: { ...c, skills: c.skills.map(normSkill) },
}));

/** The character facet lives in its own bucket so it gets its own vector. */
export const TRAIT_DOCS: SeedDoc[] = CANDIDATES.map((c) => ({
  id: c.id,
  text: `Working style and character: ${c.traits}`,
  metadata: { candidate_id: c.id },
}));
