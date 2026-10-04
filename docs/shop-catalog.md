# Workstation AI Shop catalogue in NebulaDB

Searchable RAG mirror of [int-shop.workstation.co.uk](https://int-shop.workstation.co.uk/) products, options, rules, and stock levels.

## Buckets

| Bucket | Role |
|--------|------|
| `shop_catalog` | Categories, products, option groups, rules, sources catalogue |
| `shop_stock_levels` | Per-SKU / per-option qty + lead time (**stock SOT** for feasibility) |
| `shop_chats` | Sales-agent conversation turns for support analysis |

OpsAPI remains the transactional catalogue when live. NebulaDB is the agent-grounding + stock feasibility SOT mirror.

## API

- `GET /api/v1/shop/status` — seed stats + default buckets
- `POST /api/v1/shop/catalog/ingest` — upsert seed (or posted JSON) into catalog + stock buckets
- `GET /api/v1/shop/products/search?q=` — hybrid search over `shop_catalog`
- `POST /api/v1/shop/stock` — patch a stock SOT doc
- `POST /api/v1/shop/feasibility` — stock check for `product_slug` + selections
- `POST /api/v1/shop/chats/ingest` — archive a chat turn

Showcase: **Shop catalogue** tab. Env `NEBULA_SHOP_PUBLIC_URL` overrides product URL base (default `https://int-shop.workstation.co.uk`).

## Seed

Embedded from `crates/nebula-server/fixtures/shop_catalog.seed.json` (copied from `workstation-website/shop/data/catalog.seed.json`). Refresh the fixture when the shop catalogue changes.
