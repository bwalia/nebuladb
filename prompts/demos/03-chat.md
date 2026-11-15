# Prompt 03 — NebulaDB as the Memory & Search Layer for Chat ("NebulaChat")

> Prepend `00-shared-benchmark-contract.md`. Demo slug: `chat`. Bucket prefix: `chat_`.

## Goal

Build **NebulaChat**, a team chat app (channels, DMs, threads) plus an AI assistant, with
NebulaDB as the single store for messages, conversation memory and knowledge. Chat is the most
write-heavy, latency-sensitive workload of the six demos. The demo must prove NebulaDB can
ingest messages at chat speed and still serve semantic search and assistant memory over them.

## Fictional workspace

"Orbital Logistics": 1,200 users, 300 channels (#ops-london, #eng-backend, #customer-escalations,
#random...), 18 months of history, plus a customer-support chat widget whose conversations land
in the same database. Today is 6 Nov 2026.

## Buckets

| Bucket | One doc is… | Filtered on |
|---|---|---|
| `chat_messages` | one message (text, mentions, reactions, edited flag) | `workspace`, `channel_id`, `thread_id`, `author_id`, `sent_at`, `has_link`, `is_bot` |
| `chat_threads` | a rolling thread summary, updated every N messages | `channel_id`, `thread_id`, `updated_at`, `participants[]` |
| `chat_members` | channel membership + user profile | `user_id`, `channel_ids[]`, `team`, `tz` |
| `chat_memory` | long-term assistant memory facts per user ("prefers short answers", "on-call week 45") | `user_id`, `kind`, `created_at`, `expires_at`, `source_msg_id` |
| `chat_kb` | pinned docs / wiki pages / files shared in channels, chunked | `channel_id`, `title`, `uploaded_by`, `mime` |
| `chat_support` | customer support chat turns + resolution labels | `conversation_id`, `turn`, `role`, `intent`, `resolved`, `csat` |

`bench` scale: 10M+ messages with a realistic power-law (5% of channels hold 60% of traffic,
bursts during incidents), short texts (median 12 words), emoji, code blocks and many
near-duplicates ("+1", "thanks!", "lgtm").

## The apps (sidebar group "NebulaChat")

### A. Live Chat
A working multi-user chat UI over SSE/WebSocket. Every message is a NebulaDB upsert, and
history pagination is a filtered query on `channel_id` + `sent_at`. It shows the write → visible
latency on hover in "under the hood" mode.

### B. Semantic Search Across Chat
"What did we decide about the Rotterdam depot SLA?" runs a hybrid (BM25 + vector) search over
`chat_messages` and `chat_threads`, **filtered to channels the user is a member of**, and jumps
to the message in context. Filters: from:, in:, before:/after:.

### C. Catch Me Up
"Summarise #customer-escalations since Monday" uses RAG over thread summaries + top messages,
with citations linking to messages. It shows token budget and context assembly.

### D. Assistant with Memory
A chat assistant that writes durable facts to `chat_memory` (with source message id and
expiry) and recalls them by semantic search on later turns. It shows which memories were
recalled for each answer and lets the user delete a memory (and proves it is no longer
retrieved).

### E. Support Bot over Past Conversations
For the customer widget: retrieve similar resolved `chat_support` conversations + `chat_kb`,
draft a reply, and hand off to a human when confidence is low or intent = complaint/refund.

## Safety rules

- Channel ACL is enforced in SQL filters. Add a test that a non-member never retrieves a
  private channel message, including through thread summaries and memory.
- Memory has PII hygiene: never store passwords, card numbers or secrets. Use a regex
  pre-filter with tests.
- Self-harm / threat words in support chat → fixed escalation card and human handoff.

## Benchmark workloads (`chat.*`)

| Workload | Kind | Mix | Notes |
|---|---|---|---|
| `chat.message_write` | write | 0.40 | sustained + incident burst (10× for 5 min) |
| `chat.history_page` | filtered_scan | 0.25 | last 50 by channel, cursor paging |
| `chat.semantic_search` | hybrid | 0.10 | ACL-filtered |
| `chat.memory_recall` | vector | 0.10 | per-user, tiny partition → filtered recall |
| `chat.catch_up_rag` | rag | 0.05 | |
| `chat.support_rag` | rag | 0.10 | |

Specific to this demo: **write → searchable lag** (p50/p95/p99) under sustained ingest, the
maximum sustained messages/s before history p99 > 100 ms, and how recall changes with
`--dup-ratio` (short, repetitive chat texts).

## Acceptance

Two browser windows chat live. Search respects ACL (tested). Deleted memory is not recalled
(tested). `make bench-chat` writes results. `docs/chat-demo.md` exists.
