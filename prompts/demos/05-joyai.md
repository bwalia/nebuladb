# Prompt 05 — JoyAI on NebulaDB ("JoyAI Demo")

> Prepend `00-shared-benchmark-contract.md`. Demo slug: `joyai`. Bucket prefix: `joy_`.
>
> **Ground this in the real JoyAI stack before you design anything.** JoyAI runs on opsapi
> (Lapis/OpenResty; chat agent and tax agent; `llm.lua` multi-provider config, default
> provider Ollama) and is deployed as `joyai-opsapi` in the `joyai-acc` namespace with secrets
> from Vault `joyai/acc/config`. Read the opsapi repo's chat-agent and tax-agent code and
> `llm.lua` first. Then list which of the journeys below already exist, which ones need
> NebulaDB wired in, and which ones are new. Update this prompt if reality differs.

## Goal

Show JoyAI's assistants (general chat agent + tax agent + any other agents in opsapi) using
NebulaDB as their **retrieval and memory backend**: knowledge RAG, per-user conversation memory
and structured customer/business data, all in one store. Also measure what NebulaDB adds to
JoyAI's end-to-end response time and answer quality.

## Fictional tenants

Three small UK businesses using JoyAI: a bakery chain ("Joy's Bakes", 4 shops), an
accountancy practice ("Patel & O'Neill", with an apostrophe), and a sole-trader electrician.
Each tenant has its own documents, customers, invoices and chat history. Today is 6 Nov 2026,
two months before the 31 Jan self-assessment deadline.

## Buckets

| Bucket | One doc is… | Filtered on |
|---|---|---|
| `joy_kb` | chunk of a tenant's own docs (price lists, procedures, FAQs, contracts) | `tenant_id`, `source`, `doc_id`, `updated_at` |
| `joy_tax_kb` | chunk of **plain-English, freshly written** UK tax guidance notes for the demo (self-assessment deadlines, allowable expenses, VAT thresholds, MTD), each with a `tax_year` and "verify with HMRC" note | `topic`, `tax_year`, `audience` |
| `joy_conversations` | one chat turn | `tenant_id`, `user_id`, `agent` (chat/tax/...), `session_id`, `at` |
| `joy_memory` | durable user/tenant facts the agent learned | `tenant_id`, `user_id`, `kind`, `expires_at` |
| `joy_records` | structured business data: invoices, expenses, customers | `tenant_id`, `kind`, `date`, `amount_gbp`, `category`, `customer_id` |
| `joy_drafts` | emails, invoices, tax summaries drafted by agents | `tenant_id`, `kind`, `status = 'draft'` |

## Journeys

### A. JoyAI Chat with Business Knowledge
"What's our gluten-free policy for wedding cakes?" runs RAG over `joy_kb` filtered by
`tenant_id`, with citations. It shows the opsapi → NebulaDB call in "under the hood".

### B. Tax Agent
"Can I claim my van as an expense?" / "What do I owe for 2025-26 so far?" retrieves from
`joy_tax_kb` (by `tax_year`) and computes from `joy_records` with SQL aggregates (sum of income
and expenses by category). The answer separates **retrieved guidance** from **computed numbers**,
always ends with "This is not tax advice; check with HMRC or your accountant", and drafts a
summary to `joy_drafts`.

### C. Memory Across Sessions
The agent remembers "I'm VAT-registered since April" from a previous session (`joy_memory`),
shows which memories were used, and lets the user forget one.

### D. Provider Resilience Panel
Ties to the real incident where the chat showed "assistant is unavailable" because the AI
provider config was missing. The panel shows the provider resolution (`AI_PROVIDER`, model,
endpoint) and a health check. When the LLM is down, JoyAI still returns **retrieval-only
answers** from NebulaDB ("Here are the 3 most relevant passages") instead of an error.

### E. Tenant Isolation Proof
A side-by-side view: the same question asked as two tenants gets different sources and never
crosses. Add a test.

## Safety rules

- `tenant_id` filter on every query, enforced in the adapter and not in the prompt. Add tests.
- The tax agent never files, submits or pays. It gives guidance with citations, shows its
  figures, and adds a disclaimer.
- Secrets and bank details are never stored in memory (regex filter, with tests).

## Benchmark workloads (`joyai.*`)

| Workload | Kind | Mix | Notes |
|---|---|---|---|
| `joyai.kb_rag` | rag | 0.30 | tenant-filtered |
| `joyai.tax_rag_plus_sql` | mixed | 0.15 | retrieval + aggregates |
| `joyai.memory_recall` | vector | 0.15 | per-user |
| `joyai.turn_write` | write | 0.25 | conversation log |
| `joyai.records_aggregate` | filtered_scan | 0.10 | |
| `joyai.retrieval_only_fallback` | vector | 0.05 | LLM disabled |

Specific to this demo: break down end-to-end chat latency (**opsapi → NebulaDB retrieval → LLM
→ response**) per stage, so the report shows NebulaDB's share of total time. Also report how
the number of tenants (10 → 10,000) affects filtered recall and p99, because many small tenants
is JoyAI's real shape.

## Acceptance

Chat and tax agent answer with citations through opsapi against NebulaDB. The fallback works
with the LLM off. Tenant isolation is tested. `make bench-joyai` writes results.
`docs/joyai-demo.md` exists.
