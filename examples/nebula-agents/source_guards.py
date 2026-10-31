"""Code-enforced tool limits for the Source Supplies agent team.

Prompts ask agents to stay in their lane; this module makes them. Every
MCP call an agent makes goes through `GuardedSession.call_tool`, which
runs `check()` first. A rejected call never reaches nebula-mcp: the
agent gets a readable tool error instead.

Rules:

- Every agent may call only the tools in its policy.
- SQL must be one read-only SELECT over `ss_*` buckets.
- Searches and document reads must name a bucket from the agent's list
  (the Safety Agent: `ss_sds` and `ss_guides` only).
- The Order Agent may insert only into `ss_order_drafts`, only with
  `metadata.status == "draft"` and an `account_id` and `site`.
- The Account Assistant reads and writes memories only in
  `agent_memory`, only for the user the session was started for.

Standard library only, so the tests run with `python -m unittest`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

DRAFTS_BUCKET = "ss_order_drafts"
MEMORY_BUCKET = "agent_memory"
SS_READ = "ss_*"  # any bucket whose name starts with ss_

READ_BUCKETS_ALL = frozenset({SS_READ})
SAFETY_BUCKETS = frozenset({"ss_sds", "ss_guides"})


class GuardError(ValueError):
    """A tool call that breaks the agent's policy."""


@dataclass(frozen=True)
class Policy:
    tools: tuple[str, ...]
    read_buckets: frozenset[str] = READ_BUCKETS_ALL
    may_write_drafts: bool = False
    may_use_memory: bool = False


POLICIES: dict[str, Policy] = {
    "product": Policy(tools=("execute_sql", "semantic_search", "get_document")),
    "safety": Policy(tools=("semantic_search", "get_document"), read_buckets=SAFETY_BUCKETS),
    "order": Policy(tools=("execute_sql", "semantic_search", "insert_document"), may_write_drafts=True),
    "washroom": Policy(tools=("execute_sql", "semantic_search", "get_document")),
    "sustainability": Policy(tools=("execute_sql", "get_document")),
    "assistant": Policy(tools=("recall", "remember", "semantic_search", "answer_question"), may_use_memory=True),
}

WRITE_TOOLS = frozenset({"insert_document", "remember", "delete_document", "create_snapshot", "companies_house_ingest"})

_STRING = re.compile(r"'(?:[^']|'')*'")
_LINE_COMMENT = re.compile(r"--[^\n]*")
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.S)
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|create|alter|truncate|grant|revoke|merge|copy|attach|replace|upsert|call|exec(ute)?)\b",
    re.I,
)
_TABLES = re.compile(r"\b(?:from|join)\s+(\"?)([A-Za-z_][\w.\-]*)\1", re.I)


def _bucket_allowed(bucket: str, allowed: frozenset[str]) -> bool:
    if bucket in allowed:
        return True
    return SS_READ in allowed and bucket.startswith("ss_")


def check_sql(sql: str) -> None:
    """One read-only SELECT whose FROM / JOIN targets are all ss_* buckets."""
    if not isinstance(sql, str) or not sql.strip():
        raise GuardError("execute_sql needs a non-empty sql string")
    # Drop string literals first, so a search phrase like 'how to delete' is not a keyword.
    bare = _STRING.sub("''", sql)
    bare = _BLOCK_COMMENT.sub(" ", _LINE_COMMENT.sub(" ", bare)).strip()
    bare = bare.rstrip(";").strip()
    if ";" in bare:
        raise GuardError("only one SQL statement per call")
    if not re.match(r"(?is)^select\b", bare):
        raise GuardError("only SELECT is allowed")
    bad = _FORBIDDEN.search(bare)
    if bad:
        raise GuardError(f"read-only SQL only: '{bad.group(0)}' is not allowed")
    tables = [m.group(2) for m in _TABLES.finditer(bare)]
    if not tables:
        raise GuardError("SELECT must read FROM an ss_* bucket")
    outside = [t for t in tables if not t.startswith("ss_")]
    if outside:
        raise GuardError(f"SQL may only read ss_* buckets, not {', '.join(outside)}")


def _require_bucket(tool: str, args: dict, allowed: frozenset[str]) -> None:
    bucket = args.get("bucket")
    if not bucket:
        names = ", ".join(sorted(b for b in allowed if b != SS_READ)) or "an ss_* bucket"
        raise GuardError(f"{tool} must name a bucket ({names}); searching every bucket is not allowed")
    if not _bucket_allowed(str(bucket), allowed):
        raise GuardError(f"{tool} may not use bucket {bucket!r} for this agent")


def check_insert(args: dict) -> dict:
    """insert_document: only drafts, only status=draft. Returns the args with created_by filled in."""
    if args.get("bucket") != DRAFTS_BUCKET:
        raise GuardError(f"insert_document may only write to {DRAFTS_BUCKET}, not {args.get('bucket')!r}")
    meta = args.get("metadata")
    if not isinstance(meta, dict):
        raise GuardError("insert_document needs a metadata object with status, account_id and site")
    if meta.get("status") != "draft":
        raise GuardError("drafts only: metadata.status must be 'draft' (agents never place orders)")
    for key in ("account_id", "site"):
        if not meta.get(key):
            raise GuardError(f"metadata.{key} is required on a draft")
    if not str(args.get("doc_id", "")).startswith("draft-"):
        raise GuardError("draft doc_id must start with 'draft-'")
    return {**args, "metadata": {**meta, "created_by": meta.get("created_by") or "order-agent"}}


def check_memory(tool: str, args: dict, user_id: str | None) -> dict:
    """recall / remember: only agent_memory, only the session's user."""
    bucket = args.get("bucket") or MEMORY_BUCKET
    if bucket != MEMORY_BUCKET:
        raise GuardError(f"{tool} may only use {MEMORY_BUCKET}, not {bucket!r}")
    if not user_id:
        raise GuardError(f"{tool} needs a session user (run with --user)")
    if args.get("user_id") not in (None, "", user_id):
        raise GuardError(f"{tool} may only use memories for user {user_id!r}")
    return {**args, "user_id": user_id, "bucket": MEMORY_BUCKET}


def check(agent: str, tool: str, args: dict | None, user_id: str | None = None) -> dict:
    """Validate one tool call. Returns the (possibly normalised) arguments, or raises GuardError."""
    policy = POLICIES.get(agent)
    if policy is None:
        raise GuardError(f"unknown agent {agent!r}")
    if tool not in policy.tools:
        raise GuardError(f"{agent} may not call {tool}; allowed: {', '.join(policy.tools)}")
    args = dict(args or {})

    if tool == "execute_sql":
        check_sql(args.get("sql", ""))
    elif tool in ("semantic_search", "answer_question", "get_document"):
        _require_bucket(tool, args, policy.read_buckets)
    elif tool == "insert_document":
        if not policy.may_write_drafts:
            raise GuardError(f"{agent} may not write documents")
        args = check_insert(args)
    elif tool in ("recall", "remember"):
        if not policy.may_use_memory:
            raise GuardError(f"{agent} may not use memory")
        args = check_memory(tool, args, user_id)
    else:
        raise GuardError(f"no guard defined for {tool}")
    return args


class GuardedSession:
    """Wraps an MCP ClientSession so every call_tool is checked first.

    `make_error(text)` builds the tool result returned for a rejected call
    (in the real team: an MCP CallToolResult with isError=True).
    """

    def __init__(
        self,
        session: Any,
        agent: str,
        make_error: Callable[[str], Any],
        user_id: str | None = None,
        on_block: Callable[[str, str, str], None] | None = None,
    ):
        self._session = session
        self.agent = agent
        self.user_id = user_id
        self._make_error = make_error
        self._on_block = on_block

    async def call_tool(self, name: str, arguments: dict | None = None, **kw: Any) -> Any:
        try:
            safe = check(self.agent, name, arguments, self.user_id)
        except GuardError as e:
            if self._on_block:
                self._on_block(self.agent, name, str(e))
            return self._make_error(f"blocked by Source Supplies guard: {e}")
        call: Callable[..., Awaitable[Any]] = self._session.call_tool
        return await call(name=name, arguments=safe, **kw)

    def __getattr__(self, item: str) -> Any:
        return getattr(self._session, item)
