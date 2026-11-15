# Prompt 02 — NebulaDB RAG for a Care Home Management System ("CareHub")

> Prepend `00-shared-benchmark-contract.md`. Demo slug: `carehome`. Bucket prefix: `ch_`.

## Goal

Build **CareHub**, a care home management system demo on NebulaDB. Residents' care plans,
daily care notes, medication records (MAR), incidents, staff rotas and regulatory policies all
live in one database. RAG helps carers, nurses and managers find the right information quickly
and safely. The demo must show strict access control, auditability and "the model drafts, a
nurse/manager decides".

Everything is fictional. Show a "Demo data, not clinical guidance" banner on every tab.

## Fictional operator (fixed demo calendar: today is 6 Nov 2026, 07:30 handover)

**Harbour Care Group**: 3 homes ("Harbour View", "Elm Lodge", "St Brigid's House" with an
apostrophe), about 140 residents, about 260 staff across day and night shifts, and units for
residential, nursing and dementia care.

## Buckets

| Bucket | One doc is… | Filtered on |
|---|---|---|
| `ch_residents` | resident profile: needs, preferences, likes, life story, DNACPR/ReSPECT flag (demo) | `resident_id`, `home`, `unit`, `room`, `care_type`, `risk_flags[]` |
| `ch_care_plans` | one care-plan section (mobility, nutrition, continence, skin, communication, end-of-life wishes) | `resident_id`, `section`, `review_due`, `version`, `author_role` |
| `ch_care_notes` | a daily care note written by a carer (free text with typos, as on a real shift) | `resident_id`, `home`, `shift`, `staff_id`, `written_at`, `tags[]` |
| `ch_mar` | a medication administration record entry (fictional drugs/doses) | `resident_id`, `med_code`, `round`, `due_at`, `status` (given/refused/missed/withheld) |
| `ch_obs` | observations: weight, MUST score, fluids, NEWS2-style score (demo), falls risk | `resident_id`, `kind`, `value`, `recorded_at` |
| `ch_incidents` | falls, skin damage, behaviour, safeguarding, medication errors, with outcomes | `home`, `resident_id`, `kind`, `severity`, `occurred_at`, `status` |
| `ch_policies` | chunks of home policies + summarised regulator-style standards (written fresh; no copied CQC text) | `policy`, `section`, `home`, `version`, `audience[]` |
| `ch_rota` | a shift assignment | `home`, `unit`, `staff_id`, `role`, `date`, `shift` |
| `ch_family` | family contacts and messages (demo) | `resident_id`, `relation`, `consent_to_share` |
| `ch_drafts` | handovers, incident reports, family updates, plan-review suggestions | `kind`, `resident_id`, `status = 'draft'`, `created_by` |

`bench` scale: care notes + MAR + obs make up about 85% of docs, written continuously around
the clock with peaks at the 08:00, 12:00, 17:00 and 21:00 medication rounds.

## The apps (sidebar group "CareHub")

### A. Shift Handover Generator
"Handover for Elm Lodge dementia unit, night → day". It pulls the last 12 h of `ch_care_notes`,
`ch_mar` exceptions (refused/missed), `ch_obs` changes and open `ch_incidents` with SQL filters,
then RAG summarises them per resident with citations to each note. The result goes to
`ch_drafts`, and the nurse in charge signs it off.

### B. Ask the Care Plan
A carer asks "How does Mrs D like to be supported with eating?" or "Is Mr K allowed thickened
fluids?". RAG covers that resident's `ch_care_plans` + `ch_residents` only, and the answer
quotes the plan section and its review date. If the plan is overdue for review, a warning
banner shows.

### C. Resident Timeline & Pattern Finder
For one resident: weight trend, falls, fluid intake and behaviour notes on one timeline. A
semantic search over notes ("more confused in the evenings", "not finishing meals") finds
early-warning patterns. It suggests "consider care-plan review: nutrition" with cited evidence.
It never diagnoses.

### D. Incident Reporting Assistant
A carer types or dictates what happened. The app structures it (when, where, who, injury,
actions taken), finds similar past incidents and the relevant policy section (falls
post-incident checks, safeguarding referral thresholds), and drafts the report and the family
notification for a manager.

### E. Policy & Compliance Desk
RAG over `ch_policies` with audience filtering. An inspection-readiness view uses SQL to list
overdue care-plan reviews, gaps in MAR, untrained staff on the rota (demo training field) and
open incidents older than 7 days. Each item links to its evidence.

### F. Family Update Composer
It drafts a warm, plain-English weekly update from notes and activities, but only fields with
`consent_to_share = true`, and never medication or clinical details unless consented. A person
approves before sending (nothing is actually sent).

## Safety rules (pure TS module + tests)

- **Emergency words** (not breathing, unresponsive, chest pain, choking, bleeding heavily,
  head injury after fall) → fixed card: "Call 999 now. Follow your home's emergency procedure
  and inform the nurse in charge." No model text.
- **Safeguarding words** (bruising unexplained, financial abuse, rough handling) → fixed card
  pointing to the safeguarding policy section and the manager on shift (from `ch_rota`). A
  draft referral is created and nothing is sent.
- **Medication**: the assistant never recommends giving, changing or withholding a dose. For
  medication questions it quotes the MAR + care plan and says "check with the nurse /
  prescriber".
- **Access**: carers see only residents in their home/unit on today's rota. Family personas see
  only consented fields. Enforce this in SQL filters, with tests proving cross-home retrieval is
  impossible.
- **Audit**: every retrieval writes an audit doc (who, resident, question, doc ids returned).
  Add a test for it.

## Benchmark workloads (`carehome.*`)

| Workload | Kind | Mix | Notes |
|---|---|---|---|
| `carehome.note_write` | write | 0.25 | continuous 24/7 note ingest |
| `carehome.mar_round_write` | write | 0.15 | bursty at round times |
| `carehome.resident_point_read` | point_read | 0.15 | profile + plan by resident_id |
| `carehome.handover_build` | mixed | 0.10 | 4 filtered scans + RAG summary |
| `carehome.care_plan_rag` | rag | 0.15 | resident-scoped |
| `carehome.pattern_search` | vector | 0.10 | semantic over one resident's notes (filtered recall matters) |
| `carehome.compliance_scan` | filtered_scan | 0.10 | overdue reviews / MAR gaps |

Specific to this demo: report **freshness**, meaning time from note write to its appearance in
handover RAG results (read-your-writes lag), at p50/p95, while round-time write bursts run.
Also report filtered recall for `pattern_search`, because a single resident is a tiny fraction
of the bucket and that is the hard case for post-filtering.

## Acceptance

All six apps work with the mock embedder. The emergency, safeguarding, medication and access
rules have tests. The audit trail has a test. `make bench-carehome` writes results.
`docs/carehome-demo.md` exists.
