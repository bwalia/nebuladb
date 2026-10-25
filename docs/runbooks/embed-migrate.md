# Runbook: embed model / dimension migrate

**Audience:** platform / SRE  
**Replaces:** tribal “wipe PVC + CSV reload” for leaving `mock-384`  
**APIs:** `GET/POST /api/v1/admin/embed`, `GET/POST /api/v1/admin/embed/migrate`, `POST …/cancel`

## Why this exists

HNSW is single-dimension. Export/import **keeps** old vectors. Changing
`NEBULA_EMBED_DIM` or the embedder model without a controlled rebuild
refuses to boot (`snapshot dim != embedder dim`).

This job dumps texts → you flip env → restart → drain re-embeds.
Same path for same-dim model swaps and 384→768 cutovers.

## Target (int / prod)

| Env | Value |
|-----|--------|
| `NEBULA_OPENAI_BASE_URL` | `https://ollama.workstation.co.uk/v1` |
| `NEBULA_OPENAI_MODEL` | `nomic-embed-text` |
| `NEBULA_EMBED_DIM` | `768` |
| `NEBULA_OPENAI_AUTH` | `x-api-key` |
| API key | `NEBULA_LLM_OLLAMA_TOKEN` (Vault) when `OPENAI_API_KEY` unset |

## Cutover (leader)

Keep the **old** embedder env until the dump finishes.

```bash
# 1. Deploy image that includes /admin/embed/migrate, still on mock-384
#    (or current dim). Confirm:
curl -sS "$HOST/api/v1/admin/embed" | jq .

# 2. Dump texts (blocks until jsonl is written; OK for multi-million docs)
curl -sS -X POST "$HOST/api/v1/admin/embed/migrate" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"target_model":"nomic-embed-text","target_dim":768}' | jq .
# Expect state=awaiting_restart

# 3. Helm upgrade with NEBULA_EMBED_DIM=768 + OPENAI_* as above.
#    Boot archives old snapshot/WAL when pending.json matches.

# 4. Watch drain
watch -n 10 "curl -sS $HOST/api/v1/admin/embed/migrate | jq '{state,drained,dumped,total,error}'"

# 5. Done when state=done and /healthz shows model=nomic-embed-text dim=768
curl -sS "$HOST/healthz" | jq '{docs,dim,model}'
```

Cancel between batches: `POST /api/v1/admin/embed/migrate/cancel`.

## Peers (follower + region-b)

After the leader is `done` and has a fresh `.nsnap`:

```bash
KUBECONFIG=~/.kube/k3s1.yaml NS=prod \
  SOURCE=nebuladb TARGET=nebuladb-follower ./scripts/bootstrap_region_mirror.sh
KUBECONFIG=~/.kube/k3s1.yaml NS=prod \
  SOURCE=nebuladb TARGET=nebuladb-region-b ./scripts/bootstrap_region_mirror.sh
```

Do **not** expect XDCR/WAL to rebuild compacted history at the new dim.

## Prod timing

~8.3M docs through Ollama is multi-day. The drain cursor is durable
(`embed-migrate/pending.json`); a restart resumes. Raise rate limits only
if you route HTTP bulk loaders through the API — the in-process drain
does not hit nginx RPS.

## Verify gateway before cutover

```bash
# Mint/use the same HMAC secret as chat; expect 768 floats
curl -sS https://ollama.workstation.co.uk/v1/embeddings \
  -H "x-api-key: $JWT" -H "Content-Type: application/json" \
  -d '{"model":"nomic-embed-text","input":["hello nebula"]}' | jq '.data[0].embedding | length'
```
