# Design 0013: Azure AI Search Compatibility

- **Status**: Accepted — Phase 1 (native-compatible + external adapter)
- **Tracks**: Open AI Data Platform plan — unified enterprise search
- **Relates to**: `nebula-search`, `/api/v1/azure-search/*`, `docs/compat/registry.yaml`

## Goal

Provide an Azure AI Search–*shaped* REST surface on NebulaDB without claiming
wire-identical proprietary API compatibility. Every capability is classified:

| Status | Meaning |
|---|---|
| `native` | First-class NebulaDB behaviour |
| `compatible` | Same capability, NebulaDB-native implementation |
| `translated` | Azure request shape mapped onto NebulaDB primitives |
| `external` | Forwarded to a real Azure AI Search service |
| `partial` / `unsupported` | Documented limits / hard errors with registry links |

## Unified contract

All paths (legacy `/ai/search`, Azure-compat, external Azure) go through
`nebula_search::SearchRequest` / `SearchResponse` and a `SearchBackend`.

## Routes

| Route | Capability |
|---|---|
| `PUT/GET/DELETE /api/v1/azure-search/indexes/{name}` | Index definitions |
| `GET /api/v1/azure-search/indexes` | List |
| `POST .../docs/index` | Upload / delete batch |
| `POST .../docs/search` | Search (hybrid / semantic / filter lite) |
| `GET /api/v1/compat` | Full registry |
| `GET /api/v1/compat/{product}` | One product |

Set `NEBULA_AZURE_SEARCH_ALIAS_INDEXES=1` to also mount `/api/v1/indexes/*`.

## External Azure

```
NEBULA_AZURE_SEARCH_ENDPOINT=https://<service>.search.windows.net
NEBULA_AZURE_SEARCH_API_KEY=...
NEBULA_AZURE_SEARCH_API_VERSION=2024-07-01   # optional
```

Select backend with `X-Nebula-Search-Backend: azure|native` or index metadata
`backend: azure`.

## Migration

```
nebulactl search migrate --from native --to azure --index my-index
```

## Filter subset (translated)

Supported: `eq`, `ne`, `and`, `search.in(field, 'a,b')`.
Unsupported full OData → HTTP 400 citing `azure_search.filter_odata_full`.
