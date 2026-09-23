# NebulaDB agents

The showcase's agent team, run by Claude against a live NebulaDB.

The showcase **Agents** tab (`apps/showcase/src/tabs/AgentsTab.tsx`) demonstrates five agents and
a supervisor, but their plans are scripted: every run makes the same tool calls in the same order.
This example runs the same team with Claude choosing the tool calls. The tools are served by
[`nebula-mcp`](../../crates/nebula-mcp), NebulaDB's Model Context Protocol server, which forwards each call to
nebula-server's REST API.

```
Claude ──tool_use──▶ run.py ──MCP (Streamable HTTP)──▶ nebula-mcp ──REST + bearer──▶ nebula-server
```

| Agent | Key | NebulaDB tools it may use |
|---|---|---|
| Database Agent | `db` | `list_buckets`, `explain_query`, `execute_sql`, `semantic_search`, `get_document` |
| RAG Agent | `rag` | `semantic_search`, `answer_question`, `get_document` |
| SRE Agent | `sre` | `server_stats`, `reliability_status`, `cluster_health`, `replication_status`, `slow_queries`, `explain_query` |
| Security Agent | `sec` | `audit_log`, `server_stats` |
| Backup Agent | `backup` | `list_backups`, `durability_status`, `cluster_health` |
| Assistant | `assistant` | `recall`, `remember`, `semantic_search`, `answer_question` |
| Supervisor | `supervisor` | `delegate` only: hands tasks to the five specialists and combines their reports |

Each agent gets only its own tools. None of them can write to the database except the
assistant, which writes only to the `agent_memory` bucket.

## Run it

You need Rust, Python 3.10+, and an Anthropic API key.

```bash
# 1. NebulaDB (terminal 1)
NEBULA_DATA_DIR=./data cargo run --release -p nebula-server          # :8080

# 2. The MCP server (terminal 2)
cargo run --release -p nebula-mcp                                    # :8090/mcp

# 3. The agents (terminal 3)
cd examples/nebula-agents
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export ANTHROPIC_API_KEY=sk-ant-...
python seed.py                    # 5 runbooks into the `runbooks` bucket, via MCP
```

Then ask the team:

```bash
python run.py supervisor          # "Why has our customer API become slower?"
python run.py sre
python run.py rag "What is our disaster recovery process?"
python run.py backup
python run.py sec
python run.py db "Which buckets exist, and what's in the runbooks bucket about latency?"
```

Tool calls stream to stderr as the agent makes them, and the final report goes to stdout:

```
  [Supervisor] → delegate(agent='sre', task='...')
  [Supervisor] ⇢ SRE Agent: ...
  [SRE Agent] → reliability_status()
  [SRE Agent] → slow_queries()
  ...
```

Each agent is told to cite the tool output behind every claim and to finish with findings,
evidence, and next actions.

### Long-term memory across sessions

```bash
python run.py assistant --user u-42 "I own the billing service and I'm moving it to NebulaDB."
# ...a later session, a new process:
python run.py assistant --user u-42 "What should I read before cutover?"
```

The first run stores the memory with `remember`. The second run calls `recall` first, gets back the
billing migration, and answers from the DR and backup runbooks. Memories go to the `agent_memory`
bucket with `user_id` metadata, persist in the WAL, and `recall` returns only that user's entries.

## Configuration

| Variable | Default | |
|---|---|---|
| `NEBULA_MCP_URL` | `http://127.0.0.1:8090/mcp` | nebula-mcp endpoint |
| `NEBULA_TOKEN` | *(unset)* | NebulaDB bearer token. Sent to nebula-mcp, which forwards it to nebula-server. Required when `NEBULA_API_KEYS` is set on the server. |
| `NEBULA_AGENT_MODEL` | `claude-opus-5` | Claude model for every agent |
| `ANTHROPIC_API_KEY` | | Anthropic credentials |

Every request uses adaptive thinking and server-side refusal fallbacks (`fallbacks: "default"`).
To point the agents at a deployed cluster, run nebula-mcp next to it with
`NEBULA_MCP_UPSTREAM_URL=https://...` and set `NEBULA_MCP_URL` here.

## Use the same tools from Claude Code or any MCP client

No code needed. With nebula-mcp running:

```bash
claude mcp add --transport http nebuladb http://127.0.0.1:8090/mcp
```

Claude Code then has all 20 NebulaDB tools, the four `nebula://` resources, and the
`diagnose_cluster_health` / `explain_slow_queries` prompts.

## How it works

`nebula_agents.py` is under 300 lines:

- `nebula_mcp()` opens one MCP session over Streamable HTTP.
- `Team._tools_for()` converts the agent's allowed MCP tools with the Anthropic SDK's
  `async_mcp_tool` helper.
- `Team._run()` hands them to `client.beta.messages.tool_runner`, which runs the loop: each
  `tool_use` Claude emits is executed as an MCP `tools/call`, and the result goes back as a
  `tool_result`.
- The supervisor's only tool is `delegate`, a `@beta_async_tool` that runs a specialist to completion
  and returns its report. When the supervisor delegates several tasks in one turn, the SDK's tool
  runner executes them one after another, not concurrently.

NebulaDB errors reach the agent as readable tool errors (for example `sql_invalid: WHERE must include
semantic_match(...)`), so the agent can correct its query and retry.

## Limits

- `recall` fetches the best 100 matches in the memory bucket, then keeps the user's own. NebulaDB
  applies bucket and metadata filters after the vector search, so if a user's memories rank below
  the bucket's top 100 for a query, `recall` misses them.
- The server's default `mock-384` embedder produces vectors with little real meaning, so search
  relevance is poor. Configure a real embedding model (`NEBULA_OPENAI_API_KEY` or Ollama) to get
  meaningful search and RAG results.
- `answer_question` uses whichever LLM nebula-server is configured with. The default mock echoes
  the retrieved context rather than answering.
