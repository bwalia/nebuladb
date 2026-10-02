# Companies House showcase demo

Real-world RAG demo: search UK companies → select one → chat with live
Companies House context (profile, officers, PSC, filings).

## Set the API key

Never put the key in the frontend. Configure it on **nebula-server**:

```bash
export NEBULA_COMPANIES_HOUSE_API_KEY=your-key-here
# or
export COMPANIES_HOUSE_API_KEY=your-key-here
```

Get a key at <https://developer.company-information.service.gov.uk/>.

### Helm / k3s

Add the key to the tier Vault path (same secret as other NebulaDB secrets),
or to the optional `<release>-secrets` Secret consumed via `envFrom`:

```bash
kubectl -n int create secret generic nebuladb-secrets \
  --from-literal=NEBULA_COMPANIES_HOUSE_API_KEY=your-key-here \
  --dry-run=client -o yaml | kubectl apply -f -
```

Documented alongside `NEBULA_API_KEYS` / `NEBULA_OPENAI_API_KEY` in
`deploy/helm/nebuladb/values.yaml` (`externalSecret` comment block).

## Use the flow

1. Open the showcase **Companies House** tab.
2. Search by company name or number.
3. Click a result to load the profile (+ officers / PSC / filings).
4. Click **Load into RAG** — upserts register docs **plus**:
   - a researched public website (Companies House does **not** publish
     websites; NebulaDB looks one up, then liveness-checks it),
   - a **sources catalogue** telling the LLM what it may cite.
5. Chat in the same tab; retrieved source chunks appear above the answer.
   Answers use bucket `companies_house_<number>` via `/api/v1/ai/rag`.

### Website enrichment (Google preferred)

Companies House has no website field. On ingest NebulaDB tries:

1. **Google Custom Search JSON API** when both are set on nebula-server:
   - `NEBULA_GOOGLE_CSE_API_KEY` (or `GOOGLE_CSE_API_KEY` / `GOOGLE_API_KEY`)
   - `NEBULA_GOOGLE_CSE_ID` (or `GOOGLE_CSE_ID`) — Programmable Search Engine id  
   Create a CSE at <https://programmablesearchengine.google.com/> (search the
   whole web) and enable the [Custom Search JSON API](https://developers.google.com/custom-search/v1/overview)
   in Google Cloud (free tier ≈ 100 queries/day).
2. Else **DuckDuckGo HTML** scrape (unreliable / often empty in prod).

A confirmed hit is stored as source `google_cse` or `web_search` under
`{number}-website`. Status: `GET /api/v1/companies-house/status` →
`google_cse: true|false`.

Disable website enrichment with `NEBULA_CH_ENRICH_WEBSITE=0` or
`{"enrich_website": false}` on the ingest body.

## MCP

Nebula MCP tools (design 0012 — thin REST adapters):

- `companies_house_search` — register search
- `companies_house_ingest` — load + enrich into a per-company bucket
- `answer_question` — RAG over that bucket

For richer firmographics beyond website (LinkedIn, headcount, tech stack),
connect an external enrichment MCP such as
[CompanyEnrich](https://companyenrich.com/product/mcp-server) or
[Apollo](https://github.com/pauling-ai/apollo-io-mcp-server) alongside NebulaDB;
ingest still owns the Companies House ground truth.

## Without a key

The proxy returns a clear fixture payload so the UI still works. Status:
`GET /api/v1/companies-house/status` → `{ "configured": false, ... }`.
