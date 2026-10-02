# Companies House showcase demo

Real-world RAG demo: search UK companies → select one → chat with live
Companies House context (profile, officers, PSC, filings) plus researched
public website and contact email.

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
  --from-literal=NEBULA_FIRECRAWL_API_KEY=fc-your-key \
  --dry-run=client -o yaml | kubectl apply -f -
```

Documented alongside `NEBULA_API_KEYS` / `NEBULA_OPENAI_API_KEY` in
`deploy/helm/nebuladb/values.yaml` (`externalSecret` comment block).

## Use the flow

1. Open the showcase **Companies House** tab.
2. Search by company name or number.
3. Click a result to load the profile (+ officers / PSC / filings).
4. Click **Load into RAG** — upserts register docs **plus**:
   - a researched public website and optional contact email (Companies
     House does **not** publish these; Firecrawl search + scrape finds them),
   - a **sources catalogue** telling the LLM what it may cite.
5. Chat in the same tab; retrieved source chunks appear above the answer.
   Answers use bucket `companies_house_<number>` via `/api/v1/ai/rag`.

### Website + email enrichment (Firecrawl preferred)

Companies House has no website or email fields. On ingest NebulaDB tries:

1. **Firecrawl** when `NEBULA_FIRECRAWL_API_KEY` (or `FIRECRAWL_API_KEY`) is set:
   - Search the open web for the company official site
   - Scrape the homepage (and `/contact` when linked) for a business email
   - Signup / free tier: <https://www.firecrawl.dev/> (~1,000 credits/month)
   - Agent MCP (optional, alongside Nebula MCP):  
     `https://mcp.firecrawl.dev/v2/mcp` with `Authorization: Bearer <key>`
2. Else **Google Custom Search** when `NEBULA_GOOGLE_CSE_API_KEY` +
   `NEBULA_GOOGLE_CSE_ID` are set (whole-web CSE is closed to new engines).
3. Else **DuckDuckGo HTML** scrape (unreliable / often empty in prod).

Hits are stored as `{number}-website` and `{number}-contact` with source
`firecrawl` (or `google_cse` / `web_search`). Status:

`GET /api/v1/companies-house/status` → `firecrawl: true|false`, `google_cse: …`.

Disable enrichment with `NEBULA_CH_ENRICH_WEBSITE=0` or
`{"enrich_website": false}` on the ingest body.

## MCP

Nebula MCP tools (design 0012 — thin REST adapters):

- `companies_house_search` — register search
- `companies_house_ingest` — load + enrich into a per-company bucket
- `answer_question` — RAG over that bucket

Attach [Firecrawl MCP](https://docs.firecrawl.dev/mcp-server) for ad-hoc
crawl beyond ingest. For richer firmographics (LinkedIn, headcount), also
consider [CompanyEnrich](https://companyenrich.com/product/mcp-server) or
[Apollo](https://github.com/pauling-ai/apollo-io-mcp-server); ingest still
owns Companies House ground truth.

## Without a key

The proxy returns a clear fixture payload so the UI still works. Status:
`GET /api/v1/companies-house/status` → `{ "configured": false, ... }`.
