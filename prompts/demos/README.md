# NebulaDB vertical demo + benchmark prompts

Run them one at a time. Each run is: `00-shared-benchmark-contract.md` + one demo prompt.

| # | Prompt | Slug | What it stresses |
|---|---|---|---|
| 00 | [Shared benchmark contract](00-shared-benchmark-contract.md) | — | common rules, result format, honesty rules |
| 01 | [School & University management](01-school-university.md) | `campus` | role-filtered policy RAG, MIS-style structured reads, registration write spikes |
| 02 | [Care home management](02-care-home.md) | `carehome` | 24/7 note ingest, write→RAG freshness, per-resident filtered recall, audit |
| 03 | [Chat](03-chat.md) | `chat` | highest write rate, short repetitive text, ACL search, assistant memory |
| 04 | [Content management](04-content-management.md) | `cms` | read-heavy public site, publish→searchable, incremental re-embed |
| 05 | [JoyAI](05-joyai.md) | `joyai` | many small tenants, RAG + SQL in one answer, LLM-down fallback |
| 06 | [Kisaan](06-kisaan.md) | `kisaan` | multilingual / Hinglish retrieval, geo + time filters, low bandwidth |
| 07 | [Labs](07-labs.md) | `labs` | 8 hands-on experiments × business packs: meaning vs words, model shoot-out, filters, RAG step by step, reasoning over SQL + retrieval, refusals, scale, embedding map |

Suggested order: 01 → 02 → 03 → 04 → 05 → 06. Build the shared bench runner (`bench/common`)
during 01 and reuse it for the rest.

Example:

```bash
cat prompts/demos/00-shared-benchmark-contract.md prompts/demos/01-school-university.md | claude
```
