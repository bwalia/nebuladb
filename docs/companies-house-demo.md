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
     websites; NebulaDB looks one up via web search and liveness-check),
   - a **sources catalogue** telling the LLM what it may cite.
5. Chat in the same tab; retrieved source chunks appear above the answer.
   Answers use bucket `companies_house_<number>` via `/api/v1/ai/rag`.

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
