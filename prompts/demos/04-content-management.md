# Prompt 04 — NebulaDB as an AI-Native Content Management System ("NebulaCMS")

> Prepend `00-shared-benchmark-contract.md`. Demo slug: `cms`. Bucket prefix: `cms_`.

## Goal

Build **NebulaCMS**, a headless CMS where content, versions, media metadata, taxonomy and
embeddings all live in NebulaDB. It should show that one database serves the editor workflow
(drafts, versions, approvals, scheduling) **and** the public site (fast reads, faceted search,
related content, site-search RAG), with no separate search cluster.

## Fictional publisher

"Greenway Media": three sites (a travel magazine, a home & garden site, a B2B
sustainability newsletter), 40 editors, 25,000 articles, 120,000 media items, 6 locales
(en-GB, en-US, fr, de, es, hi). Today is 6 Nov 2026.

## Buckets

| Bucket | One doc is… | Filtered on |
|---|---|---|
| `cms_content` | the **published** version of an entry, chunked by section for retrieval | `site`, `content_type`, `slug`, `locale`, `tags[]`, `category`, `author_id`, `published_at`, `status` |
| `cms_versions` | every saved version (full JSON body + diff summary) | `entry_id`, `version`, `status` (draft/review/approved/scheduled/published/archived), `edited_by`, `edited_at` |
| `cms_media` | image/video/PDF metadata + AI alt text + caption embedding | `site`, `mime`, `width`, `rights`, `expires_at`, `used_in[]` |
| `cms_taxonomy` | tag/category nodes with synonyms | `site`, `parent`, `locale` |
| `cms_redirects` | slug → slug redirect | `site`, `from`, `to` |
| `cms_events` | page-view / search / click event (for "popular" and search analytics) | `site`, `kind`, `slug`, `at`, `query` |

`bench` scale: up to 5M chunks + 20M events. Reads outnumber writes about 100:1 on the public
side. Editors write in bursts before publishing deadlines.

## The apps (sidebar group "NebulaCMS")

### A. Editor Studio
Rich-text editor with structured fields (JSON schema per content type). Autosave writes a
`cms_versions` doc. A version history view shows diffs and restore. Workflow runs
draft → review → approved → scheduled → published. Publishing re-chunks and re-embeds only the
changed sections and shows how many chunks were re-embedded.

### B. AI Editorial Assistant
- "Is there already an article about composting in small flats?" runs duplicate/near-duplicate
  detection over `cms_content` before writing.
- "Suggest internal links" finds semantically related published pieces in the same site and
  locale.
- "Suggest tags" maps to existing `cms_taxonomy` nodes only. No new tags are invented.
- Alt-text drafts for media go to the editor for review.

### C. Public Site Search + "Ask the Site" RAG
A fast faceted search (site, category, locale, date) on the public reader UI, plus an answer
box that answers only from published content with citations linking to articles. Draft or
embargoed content must never appear. Enforce this with `status = 'published'` and
`published_at <= now`, and add a test that a scheduled article is not retrieved before its time.

### D. Related Content & Personalised Feed
"More like this" vector query by article embedding with a site/locale filter, blended with
popularity from `cms_events` (SQL aggregate).

### E. Content Governance Dashboard
SQL: stale articles (not updated in 18 months with high traffic), media whose rights expire in
30 days and are still used, broken internal links, untranslated high-traffic pages, and search
queries with zero results (content gaps from `cms_events`).

## Safety rules

- Never surface draft, review, scheduled or archived content to public personas. Add tests.
- Media with expired rights is excluded from public retrieval. Add tests.
- The assistant never auto-publishes. Publish is a human action.

## Benchmark workloads (`cms.*`)

| Workload | Kind | Mix | Notes |
|---|---|---|---|
| `cms.page_read` | point_read | 0.40 | by site + slug + locale |
| `cms.faceted_search` | hybrid | 0.20 | |
| `cms.related_content` | vector | 0.15 | |
| `cms.site_rag` | rag | 0.05 | |
| `cms.event_write` | write | 0.15 | page-view stream |
| `cms.publish` | write | 0.03 | version write + re-chunk + re-embed of changed sections |
| `cms.governance_scan` | filtered_scan | 0.02 | |

Specific to this demo: **publish → searchable** time (end-to-end, including re-embedding), the
read p99 during a "breaking news" spike (50× page reads on 10 slugs), and the cost of an edit
(chunks re-embedded per edit compared with a full re-embed).

## Acceptance

Edit → publish → it appears in search and RAG. A scheduled post stays hidden until its time
(tested). `make bench-cms` writes results. `docs/cms-demo.md` exists.
