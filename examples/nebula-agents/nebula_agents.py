"""NebulaDB agents: the showcase's agent team, driven by Claude over MCP.

The showcase's Agents tab (apps/showcase/src/tabs/AgentsTab.tsx) runs
five *scripted* agents: each follows a fixed list of tool calls. This
module runs the same team for real. Claude decides which NebulaDB tools
to call and in what order. Every tool is served by `nebula-mcp`, which
forwards each call to nebula-server's REST API.

    Claude  --tool_use-->  this process  --MCP-->  nebula-mcp  --REST-->  nebula-server

Each agent sees only its own tool subset, so the RAG agent can't run
SQL and the Security agent can't write documents. The supervisor has no
NebulaDB tools of its own; it delegates to the specialists through a
`delegate` tool and combines their reports.
"""

from __future__ import annotations

import logging
import os
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from anthropic import AsyncAnthropic, beta_async_tool
from anthropic.lib.tools.mcp import async_mcp_tool
from mcp import ClientSession
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client

MCP_URL = os.environ.get("NEBULA_MCP_URL", "http://127.0.0.1:8090/mcp")
MODEL = os.environ.get("NEBULA_AGENT_MODEL", "claude-opus-5")
# On a policy decline, the API retries the request on a fallback model
# inside the same call, chosen by refusal category.
FALLBACK_BETA = "server-side-fallback-2026-07-01"

# rmcp answers the session-close DELETE with 202; the Python client logs
# anything but 200/204 as a failed termination. The session is closed.
logging.getLogger("mcp.client.streamable_http").setLevel(logging.ERROR)

SQL_DIALECT = (
    "NebulaDB SQL rules: every SELECT's WHERE must include semantic_match(content, '<text>') "
    "or vector_distance(...). Metadata fields are columns (e.g. AND region = 'eu'). "
    "Supported: GROUP BY, ORDER BY, LIMIT, inner JOIN. Not supported: OR, OUTER JOIN, "
    "subqueries, CTEs, HAVING. Filters run after retrieval over LIMIT x 4 candidates "
    "(at least 32), so aggregates cover the retrieved rows, not the whole bucket. Say so "
    "when you report counts."
)

GROUNDING = (
    "Base every claim on tool output from this session and cite the tool that produced it. "
    "If a tool returns an error, read NebulaDB's error body and adapt (fix the SQL, narrow "
    "the query) rather than giving up. If the data can't answer the question, say what is "
    "missing. Finish with a short report: findings first, then evidence, then recommended "
    "next actions."
)


@dataclass(frozen=True)
class Agent:
    key: str
    name: str
    role: str
    tools: tuple[str, ...]
    brief: str

    def system_prompt(self, user_id: str | None = None) -> str:
        parts = [
            f"You are the {self.name} on a NebulaDB operations team ({self.role}).",
            self.brief,
            GROUNDING,
        ]
        if "execute_sql" in self.tools or "explain_query" in self.tools:
            parts.append(SQL_DIALECT)
        if user_id:
            parts.append(
                f"You are assisting user_id '{user_id}'. Call recall at the start to "
                "personalise your answer. When the user states a durable preference, fact, "
                "or ongoing task, store it with remember (one self-contained sentence per "
                "memory). Don't store secrets or one-off chatter."
            )
        return "\n\n".join(parts)


# The showcase's five agents (AgentsTab.tsx AGENTS), plus a memory-backed
# assistant. The tool lists replace the scripted `plan` arrays: they say
# what an agent *may* do, and Claude decides what it *does*.
AGENTS: dict[str, Agent] = {
    a.key: a
    for a in [
        Agent(
            key="db",
            name="Database Agent",
            role="schema and query specialist",
            tools=("list_buckets", "explain_query", "execute_sql", "semantic_search", "get_document"),
            brief=(
                "Inspect what data exists before querying it. Check a statement's plan with "
                "explain_query before running it with execute_sql. Keep queries read-only."
            ),
        ),
        Agent(
            key="rag",
            name="RAG Agent",
            role="knowledge retrieval specialist",
            tools=("semantic_search", "answer_question", "get_document"),
            brief=(
                "Answer from the knowledge base. Search first to see which documents are "
                "relevant, read the best ones in full with get_document, and quote the doc "
                "ids you relied on. Use hybrid search for exact names or error codes."
            ),
        ),
        Agent(
            key="sre",
            name="SRE Agent",
            role="reliability and performance",
            tools=(
                "server_stats",
                "reliability_status",
                "cluster_health",
                "replication_status",
                "slow_queries",
                "explain_query",
            ),
            brief=(
                "Explain what the cluster is doing: start from a counter and reliability "
                "baseline, then drill into topology, replication lag, and slow statements. "
                "Explain the plan of any slow query you blame."
            ),
        ),
        Agent(
            key="sec",
            name="Security Agent",
            role="access and audit review",
            tools=("audit_log", "server_stats"),
            brief=(
                "Review the audit trail and auth/rate-limit counters for anomalies: bursts "
                "of 401/403/429, unfamiliar principals, writes to admin routes. Distinguish "
                "confirmed findings from things that merely look unusual."
            ),
        ),
        Agent(
            key="backup",
            name="Backup Agent",
            role="recovery readiness",
            tools=("list_backups", "durability_status", "cluster_health"),
            brief=(
                "Establish whether this deployment could recover from losing its data "
                "directory today: do backups exist and are they recent, is the WAL on, and "
                "what range could be replayed. Don't start a backup; recommend one if needed."
            ),
        ),
        Agent(
            key="assistant",
            name="Assistant",
            role="personal assistant with long-term memory",
            tools=("recall", "remember", "semantic_search", "answer_question"),
            brief=(
                "Help the user with questions about the company knowledge base, and "
                "remember what they tell you about themselves across sessions."
            ),
        ),
    ]
}

SUPERVISOR_PROMPT = (
    "You are the supervisor of a NebulaDB operations team. You have no database tools; "
    "you work by delegating focused tasks to specialists with the delegate tool:\n"
    + "\n".join(f"- {a.key}: {a.name} ({a.role})" for a in AGENTS.values() if a.key != "assistant")
    + "\n\nBreak the question into specialist tasks. Delegate independent tasks in parallel "
    "(several delegate calls in one turn). Give each specialist the context it needs. "
    "Their reports are your only evidence: cross-check them, delegate follow-ups where "
    "they disagree or leave gaps, and don't claim anything no report supports. Finish with "
    "a root-cause summary, the evidence behind it, and ranked next actions."
)


class Tracer:
    """Prints each tool call as it happens, so you can see the plan emerge."""

    def __init__(self, stream=sys.stderr):
        self.stream = stream

    def tool_call(self, agent: str, name: str, args: dict) -> None:
        shown = ", ".join(f"{k}={v!r}" for k, v in args.items())
        if len(shown) > 160:
            shown = shown[:157] + "..."
        print(f"  [{agent}] → {name}({shown})", file=self.stream, flush=True)

    def note(self, text: str) -> None:
        print(text, file=self.stream, flush=True)


@asynccontextmanager
async def nebula_mcp(url: str = MCP_URL, token: str | None = None) -> AsyncIterator[ClientSession]:
    """Open an MCP session to nebula-mcp. `token` is forwarded to nebula-server."""
    headers = {"Authorization": f"Bearer {token}"} if token else None
    async with create_mcp_http_client(headers=headers) as http:
        async with streamable_http_client(url, http_client=http) as (read, write, *_):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session


class Team:
    """Runs agents against one MCP session."""

    def __init__(self, client: AsyncAnthropic, session: ClientSession, tracer: Tracer | None = None):
        self.client = client
        self.session = session
        self.tracer = tracer or Tracer()
        self._mcp_tools: dict | None = None

    async def _tools_for(self, agent: Agent) -> list:
        if self._mcp_tools is None:
            listed = await self.session.list_tools()
            self._mcp_tools = {t.name: t for t in listed.tools}
        missing = [n for n in agent.tools if n not in self._mcp_tools]
        if missing:
            raise RuntimeError(
                f"{agent.name} needs tools nebula-mcp doesn't expose: {missing}. "
                "Is nebula-mcp older than this example?"
            )
        return [async_mcp_tool(self._mcp_tools[n], self.session) for n in agent.tools]

    async def _run(self, label: str, system: str, tools: list, task: str) -> str:
        runner = self.client.beta.messages.tool_runner(
            model=MODEL,
            max_tokens=16000,
            system=system,
            tools=tools,
            thinking={"type": "adaptive"},
            betas=[FALLBACK_BETA],
            fallbacks="default",
            messages=[{"role": "user", "content": task}],
        )
        final = None
        async for message in runner:
            for block in message.content:
                if block.type == "tool_use":
                    self.tracer.tool_call(label, block.name, block.input)
            final = message
        if final is None:
            return "(no response)"
        if final.stop_reason == "refusal":
            return "(the model declined this request)"
        text = "".join(b.text for b in final.content if b.type == "text").strip()
        if final.stop_reason == "max_tokens":
            text += "\n\n(report truncated: hit max_tokens)"
        return text or "(no text in final response)"

    async def run(self, agent_key: str, task: str, user_id: str | None = None) -> str:
        agent = AGENTS[agent_key]
        tools = await self._tools_for(agent)
        return await self._run(agent.name, agent.system_prompt(user_id), tools, task)

    async def supervise(self, question: str) -> str:
        team = self

        @beta_async_tool
        async def delegate(agent: str, task: str) -> str:
            """Hand a focused task to one specialist and return its report.

            Args:
                agent: Specialist key: db, rag, sre, sec, or backup.
                task: Self-contained instructions, including any context from earlier reports.
            """
            if agent not in AGENTS or agent == "assistant":
                return f"Unknown specialist {agent!r}. Choose one of: db, rag, sre, sec, backup."
            team.tracer.note(f"  [Supervisor] ⇢ {AGENTS[agent].name}: {task}")
            report = await team.run(agent, task)
            return f"Report from {AGENTS[agent].name}:\n{report}"

        return await self._run("Supervisor", SUPERVISOR_PROMPT, [delegate], question)
