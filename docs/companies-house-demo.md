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
4. Click **Load into RAG** — docs are upserted into the `companies_house`
   bucket (or the bucket you set).
5. Chat in the same tab; answers use that bucket via `/api/v1/ai/rag`.

## Without a key

The proxy returns a clear fixture payload so the UI still works. Status:
`GET /api/v1/companies-house/status` → `{ "configured": false, ... }`.
