# nebuladb.net

The public website for NebulaDB, served by GitHub Pages from this folder
(`.github/workflows/pages.yml`). Plain HTML, CSS and JavaScript: no build
step, no dependencies beyond Google Fonts.

| Page | What it covers |
|---|---|
| `index.html` | Positioning, the problems with traditional stacks, why Rust, in-browser demos, capabilities, quick start, limits |
| `features.html` | Every capability with the crate, endpoint or setting that implements it |
| `architecture.html` | Middleware chain, write/read/RAG paths, WAL recovery, XDCR, swap rebalance, operator |
| `404.html` | Not-found page (GitHub Pages serves it automatically) |

## Design

- **Type**: same stacks and sizes as `../workstation-website` (Poppins body
  at 15px, Unbounded for h1/h2, JetBrains Mono for eyebrows and data).
- **Layout**: TenthMatrix patterns (mono eyebrow with an accent cell,
  numbered cards, hairline lists, night CTA band).
- **Colour**: the showcase's dark control-plane palette
  (`apps/showcase/tailwind.config.js`), with a light variant. The theme
  choice is stored in `localStorage["nebula-theme"]`, shared with the showcase.

All tokens are at the top of `assets/site.css`.

## Demos

`assets/demos.js` runs real algorithms on toy data, and each demo is
labelled as such on the page:

- **HNSW**: builds a layered graph over 2-D points and counts distance
  computations against brute force, with recall@5.
- **Hybrid search**: real BM25 (k1 = 1.2, b = 0.75) over eight sample docs,
  min-max fused with fixed sample vector scores, as `NEBULA_HYBRID_WEIGHTS` does.
- **Crash recovery**: CRC32-framed WAL records, a torn tail on crash,
  snapshot load and replay on restart.

## Keeping it honest

Facts on the site come from the repository. When they change, update the page:

- Stats strip in `index.html`: Rust line count (`crates/**/*.rs`), crate
  count, test count (`#[test]` / `#[tokio::test]`), MCP tool count
  (`crates/nebula-mcp/src/tools.rs`).
- The "Honest limits" section mirrors the README's unsupported-SQL list and
  `PROBLEMS_SOLVED.md`.

## Local preview

```bash
cd website && python3 -m http.server 8080
# open http://localhost:8080
```

## Domain

`CNAME` holds `nebuladb.net`. DNS records for the apex and `www` are listed
at the top of `.github/workflows/pages.yml`.
