"""HTTP front-door for the NebulaDB agents example (k8s + local).

Exposes the same agents as `run.py` over a small JSON API so the
showcase (and operators) can hit them on the cluster ingress path
`/examples/*` without needing a shell on the pod.

Routes are registered both bare and under `/examples` so Traefik can
forward the prefix without a StripPrefix middleware.
"""

from __future__ import annotations

import asyncio
import os
import traceback
from typing import Any

import anthropic
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from nebula_agents import AGENTS, MCP_URL, MODEL, Team, nebula_mcp

DEFAULT_TASKS = {
    "db": "What data is in this database? Find anything about disaster recovery with SQL.",
    "rag": "What is our disaster recovery process?",
    "sre": "Give me a health and performance read-out of this NebulaDB deployment.",
    "sec": "Review recent API access for anything suspicious.",
    "backup": "Could we recover if we lost the data directory right now?",
    "assistant": "What do you remember about me, and what should I read first?",
    "supervisor": "Why has our customer API become slower?",
}

app = FastAPI(title="NebulaDB examples — agents", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_run_lock = asyncio.Lock()


class RunRequest(BaseModel):
    task: str | None = None
    user_id: str | None = Field(default=None, description="Required for assistant")


class RunResponse(BaseModel):
    agent: str
    model: str
    task: str
    report: str


def _health_payload() -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "nebula-examples-agents",
        "mcp_url": MCP_URL,
        "model": MODEL,
        "anthropic_configured": bool(os.environ.get("ANTHROPIC_API_KEY")),
        "agents": sorted([*AGENTS, "supervisor"]),
    }


def _agents_payload() -> dict[str, Any]:
    return {
        "model": MODEL,
        "mcp_url": MCP_URL,
        "agents": [
            {
                "id": key,
                "name": a.name,
                "role": a.role,
                "tools": list(a.tools),
                "default_task": DEFAULT_TASKS.get(key),
            }
            for key, a in AGENTS.items()
        ]
        + [
            {
                "id": "supervisor",
                "name": "Supervisor",
                "role": "Orchestrates the specialist agents",
                "tools": ["delegate"],
                "default_task": DEFAULT_TASKS["supervisor"],
            }
        ],
    }


@app.get("/healthz")
@app.get("/examples/healthz")
async def healthz() -> dict[str, Any]:
    return _health_payload()


@app.get("/")
@app.get("/examples")
@app.get("/examples/")
async def root() -> dict[str, Any]:
    return {
        **_health_payload(),
        "endpoints": {
            "healthz": "/examples/healthz",
            "list_agents": "/examples/agents",
            "run": "POST /examples/agents/{name}/run",
            "seed": "POST /examples/seed",
        },
    }


@app.get("/agents")
@app.get("/examples/agents")
async def list_agents() -> dict[str, Any]:
    return _agents_payload()


async def _do_run(name: str, body: RunRequest) -> RunResponse:
    if name not in AGENTS and name != "supervisor":
        raise HTTPException(404, f"unknown agent {name!r}")
    if name == "assistant" and not body.user_id:
        raise HTTPException(400, "assistant requires user_id")
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise HTTPException(
            503,
            "ANTHROPIC_API_KEY is not set on this deployment; "
            "add it to the nebuladb-secrets Secret",
        )

    task = body.task or DEFAULT_TASKS[name]
    token = os.environ.get("NEBULA_TOKEN") or os.environ.get("NEBULA_API_KEYS", "").split(",")[0]

    async with _run_lock:
        try:
            async with nebula_mcp(token=token or None) as session:
                team = Team(anthropic.AsyncAnthropic(), session)
                if name == "supervisor":
                    report = await team.supervise(task)
                else:
                    report = await team.run(name, task, user_id=body.user_id)
        except anthropic.AuthenticationError as e:
            raise HTTPException(502, f"Anthropic rejected credentials: {e}") from e
        except TypeError as e:
            if "authentication method" in str(e):
                raise HTTPException(503, "No Anthropic credentials configured") from e
            raise HTTPException(500, str(e)) from e
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(502, f"agent run failed: {e}") from e

    return RunResponse(agent=name, model=MODEL, task=task, report=report)


@app.post("/agents/{name}/run", response_model=RunResponse)
@app.post("/examples/agents/{name}/run", response_model=RunResponse)
async def run_agent(name: str, body: RunRequest) -> RunResponse:
    return await _do_run(name, body)


@app.post("/seed")
@app.post("/examples/seed")
async def seed() -> dict[str, Any]:
    from seed import main as seed_main

    token = os.environ.get("NEBULA_TOKEN") or os.environ.get("NEBULA_API_KEYS", "").split(",")[0]
    if token:
        os.environ.setdefault("NEBULA_TOKEN", token)
    try:
        rc = await seed_main()
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(502, f"seed failed: {e}") from e
    if rc not in (0, None):
        raise HTTPException(502, f"seed exited with code {rc}")
    return {"status": "ok", "seeded": "runbooks"}
