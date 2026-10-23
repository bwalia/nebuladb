/**
 * RFP Responder corpus — past proposals, capability sheets and win notes
 * for fictional vendor "NebulaStack" selling an AI-native database.
 */
import type { SeedDoc } from "../seed";

export const SEED_VERSION = "rfp-v1";
export const CAPABILITY_BUCKET = "rfp_capabilities";
export const PROPOSAL_BUCKET = "rfp_proposals";

export const SEGMENTS = ["security", "ha", "rag", "ops", "pricing", "compliance"] as const;
export type Segment = (typeof SEGMENTS)[number];

export interface RfpQuestion {
  id: string;
  segment: Segment;
  title: string;
  text: string;
}

/** Sample questions from a fictional bank RFP. */
export const RFP_QUESTIONS: RfpQuestion[] = [
  {
    id: "Q1",
    segment: "security",
    title: "Encryption & keys",
    text: "Describe encryption at rest and in transit. Can customer-managed keys (CMK) be used in our AWS account? How are key rotations handled?",
  },
  {
    id: "Q2",
    segment: "ha",
    title: "Multi-region active-active",
    text: "We require active-active across two UK regions with RPO under 60 seconds for document and vector indexes. Describe your replication model and failure modes.",
  },
  {
    id: "Q3",
    segment: "rag",
    title: "Grounded generation",
    text: "How do you prevent the LLM from answering outside retrieved context? Do you support citation IDs back to source chunks and audit of prompts?",
  },
  {
    id: "Q4",
    segment: "ops",
    title: "Observability",
    text: "List Prometheus metrics, SLO dashboards and alerting you ship. Can we scrape from our central Prometheus in eu-west-2?",
  },
  {
    id: "Q5",
    segment: "compliance",
    title: "Data residency",
    text: "Confirm UK/EU data residency options. Is support staff access to production data logged and MFA-gated?",
  },
  {
    id: "Q6",
    segment: "pricing",
    title: "Licensing",
    text: "Provide list vs. discount pricing for a 50M vector corpus, 3-node HA, annual commit, including support tiers.",
  },
];

function cap(id: string, segment: Segment, title: string, body: string): SeedDoc {
  return {
    id,
    text: `${id} · ${title}\n${body}`,
    metadata: { doc_id: id, kind: "capability", segment, title },
  };
}

function prop(
  id: string,
  segment: Segment,
  customer: string,
  outcome: "won" | "lost" | "pilot",
  title: string,
  body: string
): SeedDoc {
  return {
    id,
    text: `${id} · ${customer} · ${outcome} · ${title}\n${body}`,
    metadata: { doc_id: id, kind: "proposal", segment, customer, outcome, title },
  };
}

export const CAPABILITIES: SeedDoc[] = [
  cap(
    "CAP-SEC-1",
    "security",
    "Encryption and CMK",
    "TLS 1.3 on all client and peer links. Data volumes use LUKS/AES-256 at rest on customer disks. Optional AWS KMS CMK via envelope encryption for snapshot exports; rotation follows the CMK policy (typically annual). Application-level field encryption is roadmap. Secrets inject via External Secrets / Vault — never baked into images."
  ),
  cap(
    "CAP-HA-1",
    "ha",
    "Home-region active-active + XDCR",
    "Each bucket is pinned to one home region for writes; XDCR streams WAL both ways so every region serves local reads. Same bucket is never dual-written. Within-region: leader + WAL follower with promote. Cross-region lag exposed on /admin/replication (lag_bytes, behind). Cold peers bootstrap from .nsnap snapshots — XDCR does not backfill compacted history."
  ),
  cap(
    "CAP-RAG-1",
    "rag",
    "Grounded RAG API",
    "POST /api/v1/ai/rag accepts explicit context[] plus system instructions. The model is instructed to cite [n] indexes into that context only; empty context yields a refusal template. SSE streams context chunks before tokens. Prompt/response pairs can be mirrored to an audit bucket when NEBULA_RAG_AUDIT=1."
  ),
  cap(
    "CAP-OPS-1",
    "ops",
    "Prometheus & Grafana",
    "Native /metrics with query latency, embed cache, WAL lag, backup age, RAG counters. Dashboards-as-code deploy to Grafana. Scrape from any Prometheus that can reach the Service; exemplars optional. Alerts ship for catch-up lag and crash loops."
  ),
  cap(
    "CAP-CMP-1",
    "compliance",
    "Residency and access",
    "Single-region and dual-region UK/EU topologies. Support break-glass is MFA + ticketed, session recorded. No offshore copy of customer corpora in standard contracts. DPA and SOC2 Type II pack available under NDA."
  ),
  cap(
    "CAP-PRC-1",
    "pricing",
    "Packaging sketch",
    "List: platform fee + per-million vectors stored + HA adder. Enterprise includes 24×7 follow-the-sun, named CSM, and quarterly architecture reviews. Spot and committed-use discounts via AWS Marketplace private offer."
  ),
];

export const PROPOSALS: SeedDoc[] = [
  prop(
    "PROP-24-HSBC-PILOT",
    "ha",
    "Contoso Bank UK",
    "pilot",
    "Dual-AZ pilot response (HA excerpt)",
    "We proposed us-east-1 style naming mapped to eu-west-2 / eu-west-1 with home_region pinning for ledger buckets and local RAG reads. RPO target: steady-state lag ≤ 5s p99 measured on /admin/replication. Failover runbook included snapshot-bootstrap of a cold peer. Pilot scope: 5M vectors, no CMK in phase 1."
  ),
  prop(
    "PROP-25-RETAIL-WIN",
    "rag",
    "Shopwright PLC",
    "won",
    "Catalogue + support RAG",
    "Won on grounded citations into the help centre and SQL hard filters for stock. Differentiator vs pure vector DBs: one binary for docs + HNSW + streaming RAG. Security questionnaire answered with CAP-SEC-1 and CAP-CMP-1. Discount: 18% on 3-year commit."
  ),
  prop(
    "PROP-23-INSURE-LOST",
    "security",
    "MutualCover",
    "lost",
    "Lost on CMK timeline",
    "MutualCover required in-account CMK for primary volumes in Q3; we only offered snapshot-export CMK at the time. Loss note: accelerate volume-level KMS. Otherwise scored high on RAG auditability."
  ),
  prop(
    "PROP-25-TELCO-OPS",
    "ops",
    "NorthGrid Mobile",
    "won",
    "Observability addendum",
    "Committed to shipping lag_bytes alerts into their existing Alertmanager. Provided Grafana JSON under Apache-2.0. Support: Enterprise gold, 15-minute Sev-1 response."
  ),
  prop(
    "PROP-24-PUBLIC-CMP",
    "compliance",
    "City of Riverside",
    "pilot",
    "UK residency RFP excerpt",
    "Confirmed dual-AZ London only; support staff in UK/EU. DPIA template attached. No training on customer data."
  ),
];
