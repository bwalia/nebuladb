# NebulaDB Enterprise AI Database Showcase

You are a Principal Product Engineer, AI platform architect, distributed database engineer, UX designer, and enterprise demo specialist.

Build a **world-class enterprise showcase/demo platform for NebulaDB**.

The showcase must demonstrate that NebulaDB is an **enterprise-grade distributed AI database / vector database designed specifically for AI agents, LLM applications, RAG, semantic search, structured SQL workloads, and document workloads**.

This is not a basic CRUD demo.

It must feel like the public enterprise showcase for a serious database company.

---

# 1. Product Positioning

The showcase should communicate this message immediately:

> **NebulaDB — The AI-Native Enterprise Database for Agents, LLMs and RAG**

Supporting message:

> SQL + JSON + Vector + RAG + AI Agents + Distributed HA in one database.

Demonstrate that NebulaDB combines:

- PostgreSQL-style SQL
- Document/JSON storage
- Buckets and collections
- Powerful indexing
- Distributed architecture
- High availability
- Multi-region replication
- XDCR-style replication
- Automatic failover
- Swap rebalance
- Zero-downtime upgrades
- WAL/CDC
- Backup and restore
- Point-in-time recovery
- Vector search
- HNSW
- Embeddings
- Intelligent chunking
- Hybrid search
- Reranking
- RAG
- MCP
- AI agents
- LLM integration
- Ollama/local AI
- Enterprise security
- Observability

The demo should make these capabilities understandable through interactive scenarios rather than simply listing them.

---

# 2. Build an Actual Showcase Application

Create a complete runnable application.

Recommended architecture:

```text
nebuladb-showcase/
│
├── frontend/
│   └── Next.js / React
│
├── backend/
│   └── Rust
│
├── demo-data/
│
├── demo-scenarios/
│
├── agents/
│
├── rag/
│
├── docker/
│
├── kubernetes/
│
└── docs/
```

Use:

- Next.js
- React
- TypeScript
- Rust/Axum backend
- NebulaDB APIs
- PostgreSQL wire protocol where appropriate
- MCP
- Ollama
- WebSockets/SSE
- OpenTelemetry
- Prometheus
- Grafana

The application must run using:

```bash
docker compose up
```

and also support Kubernetes deployment.

---

# 3. Executive Demo Mode

Create a dedicated:

## "Executive Demo"

screen.

This should tell the NebulaDB story visually in approximately 5 minutes.

Show:

```text
Enterprise Data
      ↓
NebulaDB
      ↓
SQL + JSON + Vector
      ↓
AI Retrieval
      ↓
RAG
      ↓
AI Agents
      ↓
Enterprise Applications
```

Display live statistics:

- Documents
- Vectors
- Embeddings
- Queries/sec
- Vector queries/sec
- P95 latency
- Active agents
- RAG queries
- Replication lag
- Cluster nodes
- Regions
- Storage
- Backup status

Use animated but realistic live data.

---

# 4. Showcase Landing Page

Build an impressive landing page.

Headline:

> **One Database for Enterprise AI**

Subheading:

> Store, search, understand and automate your enterprise data with SQL, JSON, vectors, RAG and AI agents.

Primary actions:

- Launch Demo
- Explore RAG
- Try AI Agent
- Explore SQL
- View Cluster
- Explore MCP

Include an architecture visualization.

---

# 5. Demo Navigation

Create sections:

```text
Overview
AI Search
RAG
Vector Database
SQL
Document Database
AI Agents
MCP
Cluster
Multi-Region
Replication
Rebalance
Backup & Restore
Observability
Security
Developer Experience
Benchmarks
```

Every section should contain:

- explanation
- interactive demo
- live metrics
- technical details
- "Try it" button
- underlying API/query where useful

---

# 6. Enterprise Knowledge Base Demo

Create realistic enterprise data.

Example datasets:

### Company Knowledge

- HR policies
- engineering documentation
- architecture documents
- security policies
- compliance documents
- product documentation
- incident reports
- runbooks
- customer support documents
- financial reports

Generate realistic sample data.

Do not use meaningless lorem ipsum.

---

# 7. Document Ingestion Demo

Create an interactive document ingestion screen.

Show:

```text
Upload Document
       ↓
Content Extraction
       ↓
Structure Detection
       ↓
Smart Chunking
       ↓
Metadata Extraction
       ↓
Embedding Generation
       ↓
Vector Index
       ↓
Ready for Search
```

Display live progress.

Show:

- document
- chunks
- tokens
- embedding dimensions
- embedding model
- processing time
- vector index
- metadata

---

# 8. Intelligent Chunking Demo

Allow users to inspect how NebulaDB chunks a document.

Show:

```text
Document
 ├── Section
 │    ├── Chunk
 │    ├── Chunk
 │    └── Chunk
 ├── Section
 │    ├── Chunk
 │    └── Chunk
```

Allow switching between:

- fixed-size chunking
- semantic chunking
- heading-aware chunking
- paragraph-aware chunking
- code-aware chunking

Show why the selected strategy was used.

---

# 9. Embedding Demo

Display:

- embedding model
- vector dimensions
- generation latency
- batch size
- throughput
- cache hit ratio

Allow the user to generate embeddings.

Support Ollama.

Make the model configurable.

---

# 10. Vector Search Playground

Build a beautiful interactive vector search interface.

User enters:

> How do I recover a failed Kubernetes node?

Show:

### Semantic results

Document | Chunk | Score | Similarity

Then show:

- vector distance
- metadata
- source document
- chunk
- embedding information

Allow changing:

- top K
- similarity threshold
- index
- filters

---

# 11. HNSW Visualization

Create an interactive HNSW demonstration.

Visualize:

```text
Vector
   ↓
HNSW
 ┌──────────────┐
 │ Layer 3      │
 │   ●──●──●    │
 │      │       │
 │ Layer 2      │
 │ ●──●──●──●   │
 │ Layer 1      │
 │ ●●●●●●●●●    │
 └──────────────┘
```

Show:

- number of vectors
- index size
- search latency
- ef_search
- M
- recall estimate

Allow users to change parameters and run searches.

---

# 12. Hybrid Search Demo

This is a key NebulaDB differentiator.

Show:

```text
User Query
    │
    ├── BM25
    │
    ├── Vector Search
    │
    ├── Metadata Filter
    │
    └── Semantic Expansion
            ↓
       Score Fusion
            ↓
        Reranking
            ↓
       Final Results
```

Display each stage.

Show:

- lexical score
- vector score
- metadata score
- reranker score
- final score

Allow users to change weighting.

---

# 13. RAG Playground

Create a complete RAG chat interface.

Example:

> What is our disaster recovery process for a multi-region Kubernetes cluster?

Pipeline visualization:

```text
Question
   ↓
Query Understanding
   ↓
Query Expansion
   ↓
Hybrid Retrieval
   ↓
Reranking
   ↓
Context Assembly
   ↓
LLM
   ↓
Answer
```

Display citations.

Example:

```text
Answer

...

Sources

1. Disaster Recovery Runbook
2. Kubernetes HA Architecture
3. Incident Response Policy
```

Clicking a citation must reveal the exact chunk used.

---

# 14. RAG Evaluation

Create a RAG evaluation dashboard.

Show:

- retrieval precision
- recall
- MRR
- NDCG
- groundedness
- citation accuracy
- answer relevance
- hallucination rate

Provide predefined test questions.

Allow "Run Evaluation".

---

# 15. AI Agent Showcase

Create multiple autonomous AI agents.

Examples:

### Database Agent

Can:

- inspect schema
- execute safe SQL
- explain queries
- recommend indexes

### RAG Agent

Can:

- search knowledge
- retrieve documents
- answer questions
- cite sources

### SRE Agent

Can:

- inspect cluster
- inspect metrics
- investigate failures
- diagnose replication issues

### Security Agent

Can:

- inspect permissions
- identify risky configuration
- review audit logs

### Backup Agent

Can:

- inspect backup health
- verify backups
- recommend retention

---

# 16. Multi-Agent Scenario

Create an impressive end-to-end demonstration.

User asks:

> "Why has our customer API become slower in Europe?"

Agent orchestration:

```text
Supervisor Agent
       │
 ┌─────┼─────────┐
 ↓     ↓         ↓
SRE   Database   Network
Agent Agent      Agent
 │      │         │
 └──────┼─────────┘
        ↓
 Investigation
        ↓
 Root Cause
        ↓
 Recommendation
```

Show the agent calls live.

The user should see exactly which NebulaDB MCP tools the agents invoke.

---

# 17. MCP Showcase

Create an MCP page.

Show:

```text
AI Agent
   ↓
MCP
   ↓
NebulaDB
```

Display available MCP tools.

Examples:

```text
execute_sql
semantic_search
hybrid_search
retrieve_context
ai_answer
cluster_health
list_nodes
rebalance_status
backup_status
query_metrics
```

Allow clicking a tool to inspect:

- description
- parameters
- permissions
- execution
- response

Provide an interactive MCP playground.

---

# 18. SQL Playground

Create a professional SQL editor.

Support:

```sql
SELECT *
FROM customers
WHERE region = 'EU';
```

Also demonstrate hybrid AI queries:

```sql
SELECT *
FROM documents
WHERE semantic_match(
    content,
    'Kubernetes disaster recovery'
)
LIMIT 10;
```

Show:

- execution plan
- latency
- rows
- indexes used
- storage reads
- vector operations

---

# 19. JSON / Document Playground

Demonstrate document storage.

Example:

```json
{
  "customer": {
    "id": "C1001",
    "profile": {
      "industry": "retail",
      "region": "EU"
    }
  }
}
```

Allow:

- insert
- update
- search
- nested field filtering
- JSON indexing
- document inspection

Demonstrate the JSON tree structure visually.

---

# 20. Cluster Visualization

Build the previously designed live cluster graph.

Show:

- nodes
- shards
- replicas
- regions
- CPU
- memory
- storage
- QPS
- health
- replication

Animate replication and shard movement.

---

# 21. Multi-Region Demo

Create:

```text
        Europe
       ┌───────┐
       │ Node 1│
       │ Node 2│
       │ Node 3│
       └───┬───┘
           │
          XDCR
           │
       ┌───▼───┐
       │ Node 4│
       │ Node 5│
       │ Node 6│
       └───────┘
        America
```

Display:

- replication throughput
- replication lag
- documents replicated
- conflicts
- region health

---

# 22. Failover Demonstration

Provide a safe simulated failure.

Button:

> Simulate Node Failure

Show:

```text
Healthy
  ↓
Node Failure
  ↓
Failure Detection
  ↓
Replica Promotion
  ↓
Routing Update
  ↓
Recovery
```

Show live metrics.

Clearly label this as a simulation unless it is operating against a real cluster.

---

# 23. Swap Rebalance Demonstration

Create a live demonstration of:

```text
Node v1
   ↓
Snapshot
   ↓
CDC
   ↓
Catch-up
   ↓
Consistency Verification
   ↓
Cutover
   ↓
Node v2
```

Display:

- shard progress
- bytes transferred
- CDC lag
- throughput
- estimated completion
- state machine phase

Allow:

- pause
- resume
- retry
- rollback

in a safe demo environment.

---

# 24. Backup & Restore Showcase

Demonstrate:

- full backup
- incremental backup
- continuous WAL backup
- point-in-time recovery
- cross-region restore

Show:

```text
Cluster
   ↓
Snapshot
   +
WAL
   ↓
Object Storage
   ↓
Restore
```

Provide a timeline where the user can select:

> Restore to 14:32:10

Show exactly what data would be restored.

---

# 25. Observability

Build an enterprise observability screen.

Show:

- QPS
- read latency
- write latency
- vector latency
- RAG latency
- WAL latency
- CDC lag
- replication lag
- CPU
- memory
- disk
- cache
- HNSW statistics

Include:

- logs
- traces
- metrics

---

# 26. Security Showcase

Demonstrate:

- authentication
- RBAC
- tenant isolation
- document permissions
- row-level access
- API keys
- TLS
- audit logging

Create sample users:

```text
Admin
Developer
Analyst
AI Agent
Read Only
```

Show how permissions affect AI retrieval.

---

# 27. Developer Experience

Create a "Build with NebulaDB" page.

Show examples in:

- Rust
- Python
- TypeScript
- Go
- Java

Provide copyable examples for:

- SQL
- JSON
- vector search
- RAG
- MCP
- agent integration

---

# 28. Performance Benchmark

Create a benchmark page.

Demonstrate:

- reads/sec
- writes/sec
- SQL latency
- vector search latency
- hybrid search latency
- ingestion throughput
- embedding throughput
- RAG throughput

Do not fabricate benchmark results.

If running synthetic/demo workloads, clearly label them:

> Synthetic benchmark

Allow users to run the benchmark themselves.

---

# 29. AI-Native Database Architecture View

Create a visually impressive architecture diagram.

Show:

```text
                    AI AGENTS
                        │
                 ┌──────▼──────┐
                 │     MCP     │
                 └──────┬──────┘
                        │
              ┌─────────▼─────────┐
              │     NebulaDB      │
              │                   │
              │ SQL   JSON VECTOR │
              │                   │
              │ RAG   AI  SEARCH  │
              └─────────┬─────────┘
                        │
              ┌─────────▼─────────┐
              │ Distributed Core  │
              └─────────┬─────────┘
                        │
           ┌────────────┼────────────┐
           ↓            ↓            ↓
          WAL         HNSW        Storage
           │
           ↓
     Replication / CDC
```

Make every component clickable.

---

# 30. Guided Demo Mode

Create guided tours:

### Tour 1 — AI Search

Document → Chunk → Embed → Search

### Tour 2 — RAG

Question → Retrieval → Context → LLM

### Tour 3 — AI Agent

Agent → MCP → NebulaDB

### Tour 4 — Enterprise HA

Node failure → failover → recovery

### Tour 5 — Multi-region

Region A → replication → Region B

### Tour 6 — Zero Downtime Upgrade

Old node → swap rebalance → new node

### Tour 7 — Backup

Snapshot → WAL → restore

Each tour should take approximately 2–5 minutes.

---

# 31. Demo Reset

Provide:

> Reset Demo

This must restore the demo environment to a known state.

Allow:

- reset data
- reset cluster
- reset agents
- reset metrics
- reset scenarios

---

# 32. Demo Seed System

Build a deterministic seed system.

Command:

```bash
make demo-seed
```

Generate:

- customers
- products
- documents
- support tickets
- incidents
- engineering documents
- vectors
- embeddings
- AI metadata

The demo must work offline using local models where possible.

---

# 33. Local AI

Use Ollama as the default local LLM/embedding runtime.

The showcase must not require external AI APIs.

Provide configurable models through environment variables.

Example:

```text
LLM_MODEL=
EMBEDDING_MODEL=
RERANK_MODEL=
```

---

# 34. Production Architecture

Do not build fake frontend-only demonstrations.

The showcase must communicate with actual NebulaDB APIs.

Where a capability is simulated, clearly isolate it behind a Demo Simulation layer.

Never present simulated metrics as real production metrics.

---

# 35. API Architecture

Build a backend-for-frontend API.

Example:

```text
/api/demo
/api/rag
/api/search
/api/vector
/api/sql
/api/agents
/api/mcp
/api/cluster
/api/rebalance
/api/backup
/api/metrics
```

Use WebSockets/SSE for live operations.

---

# 36. UX Requirements

The UI should feel like an enterprise database control plane.

Design characteristics:

- modern
- premium
- technical
- clean
- responsive
- dark/light mode
- excellent typography
- subtle animations
- real-time updates

Avoid:

- gimmicky animations
- excessive gradients
- fake AI terminology
- meaningless charts

Every visualization must communicate useful technical information.

---

# 37. "Under the Hood" Mode

Every major demo should have:

> Show me what's happening

When clicked, expose:

- SQL
- API request
- MCP tool call
- execution plan
- retrieval pipeline
- vector query
- database response
- timing

This is critical for technical audiences.

---

# 38. Enterprise Personas

Add demo modes for:

### CTO

Focus on:

- architecture
- reliability
- cost
- scalability

### Enterprise Architect

Focus on:

- integration
- APIs
- MCP
- security
- multi-region

### Developer

Focus on:

- SQL
- APIs
- SDKs
- RAG

### Data Engineer

Focus on:

- CDC
- ingestion
- replication
- indexing

### SRE

Focus on:

- HA
- failover
- rebalance
- observability
- backups

### AI Engineer

Focus on:

- embeddings
- vectors
- RAG
- agents
- MCP

---

# 39. Technical Documentation

Generate documentation explaining every showcase.

Include:

```text
docs/
├── architecture.md
├── ai-search.md
├── rag.md
├── vectors.md
├── sql.md
├── agents.md
├── mcp.md
├── ha.md
├── multi-region.md
├── rebalance.md
├── backup.md
├── security.md
└── observability.md
```

---

# 40. Docker Compose

Provide:

```text
NebulaDB
Ollama
Showcase Backend
Showcase Frontend
Prometheus
Grafana
```

Single command:

```bash
docker compose up
```

Then:

```text
http://localhost:3000
```

should launch the complete showcase.

---

# 41. Kubernetes

Provide Helm deployment for:

- NebulaDB
- Showcase
- Ollama
- Prometheus
- Grafana

Use production-style configuration.

---

# 42. Testing

Create comprehensive tests.

Test:

- document ingestion
- chunking
- embeddings
- vector search
- hybrid search
- RAG
- SQL
- JSON
- MCP
- agents
- failover simulation
- rebalance simulation
- backup simulation
- observability

Include:

- unit tests
- integration tests
- E2E tests
- Playwright tests

---

# 43. Final Acceptance Criteria

The showcase is complete only when a new user can:

1. Start the entire platform with Docker Compose.
2. Open the dashboard.
3. Load the enterprise demo dataset.
4. Upload a document.
5. Watch it become chunks and embeddings.
6. Perform semantic search.
7. Perform hybrid search.
8. Ask a RAG question.
9. Inspect citations.
10. Ask an AI agent a database question.
11. Watch MCP calls.
12. Execute SQL.
13. Inspect JSON documents.
14. View HNSW/vector statistics.
15. View cluster topology.
16. simulate node failure.
17. Demonstrate failover.
18. Demonstrate swap rebalance.
19. Demonstrate replication.
20. Demonstrate backup/restore.
21. Inspect metrics and logs.
22. Run benchmarks.
23. Explore the underlying APIs.
24. Reset the entire demo.

---

# 44. Most Important Requirement

Do not build this as a collection of disconnected demo screens.

Build a **single coherent enterprise story**.

The user should experience:

```text
Enterprise Data
      ↓
NebulaDB
      ↓
Store
      ↓
Index
      ↓
Understand
      ↓
Search
      ↓
Retrieve
      ↓
RAG
      ↓
AI Agent
      ↓
MCP
      ↓
Automate
```

Then demonstrate that all of this operates on the same highly available distributed database.

The final result should make it immediately obvious why an enterprise would choose NebulaDB for:

> **AI agents + LLM applications + enterprise RAG + vector search + structured and document data.**

---

# 45. Final Deliverables

Produce a complete runnable repository containing:

- Next.js showcase
- Rust backend
- NebulaDB integration
- AI/RAG pipeline
- Ollama integration
- MCP integration
- Agent framework
- Demo dataset
- Seed scripts
- SQL examples
- Vector examples
- HA simulations
- Rebalance simulations
- Backup simulations
- Observability
- Grafana dashboards
- Docker Compose
- Kubernetes manifests
- Helm chart
- automated tests
- Playwright E2E tests
- documentation
- architecture diagrams
- developer examples

Do not stop at architecture or pseudocode.

Implement the actual application wherever the underlying NebulaDB APIs already exist.

For functionality that does not yet exist in NebulaDB, create clean adapters/interfaces and provide a deterministic demo implementation without hiding the fact that it is simulated.

Prioritize **technical credibility, enterprise UX, reproducibility, and an impressive end-to-end AI database experience**.