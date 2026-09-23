"""Seed the runbooks the agents reason over, through nebula-mcp itself.

No model calls. This writes with the same `insert_document` MCP tool an
agent would use, so it also smoke-tests the MCP path. Safe to re-run:
the doc ids are stable, so re-seeding upserts.

    python seed.py
"""

from __future__ import annotations

import asyncio
import os
import sys

from nebula_agents import MCP_URL, nebula_mcp

BUCKET = "runbooks"

RUNBOOKS = [
    (
        "api-latency",
        {"service": "customer-api", "team": "sre", "kind": "runbook"},
        "API latency runbook. When customer API p95 latency rises: 1) check NebulaDB "
        "reliability_status: any mode other than normal means memory, CPU, or disk "
        "pressure, and disk_critical refuses writes outright; 2) read slow_queries: a "
        "semantic_match with a large LIMIT plus metadata filters overfetches LIMIT x 4 "
        "candidates and is the usual culprit; 3) check replication_status, because a "
        "lagging follower serves stale reads; 4) compare embed_cache_hits with "
        "embed_cache_misses in server_stats, since every miss adds an embedding provider "
        "round trip to a search. Mitigate by lowering LIMIT, adding filters, or raising "
        "the embedding cache size.",
    ),
    (
        "disaster-recovery",
        {"service": "nebuladb", "team": "platform", "kind": "runbook"},
        "Disaster recovery process. Recovery point objective 15 minutes, recovery time "
        "objective 1 hour. Take a snapshot every 6 hours and before every upgrade. To "
        "recover: provision a node with the same embedder model and dimension, restore "
        "the latest backup into an empty data directory, then replay the WAL from the "
        "snapshot's sequence number. A restore onto a different embedder model is "
        "rejected. Verify with healthz document counts and a known-answer search.",
    ),
    (
        "access-review",
        {"service": "nebuladb", "team": "security", "kind": "runbook"},
        "Access review. Every API key is scoped to one service. Investigate: more than 20 "
        "401 responses from one principal in an hour (credential stuffing or a rotated "
        "key); any 429 burst (runaway client); calls to /admin routes from a principal "
        "that is not an operator key; requests from raw client IPs when auth is enabled, "
        "which means a route bypassed the key check.",
    ),
    (
        "backup-policy",
        {"service": "nebuladb", "team": "platform", "kind": "policy"},
        "Backup policy. Production must have a backup newer than 24 hours and the WAL "
        "enabled (persistent=true). A deployment with no backups is not recoverable; "
        "raise it as a P1. Backups are only useful if restore has been tested within the "
        "last quarter.",
    ),
    (
        "slow-query-guide",
        {"service": "nebuladb", "team": "sre", "kind": "guide"},
        "Slow query guide. NebulaDB plans retrieval and filtering together: every query "
        "starts from semantic_match or vector_distance, then applies metadata filters to "
        "the retrieved candidates. Queries are slow when LIMIT is large, when GROUP BY "
        "aggregates over many candidates, or when a JOIN retrieves on both sides. Use "
        "explain_query to see the candidate count and the residual filters.",
    ),
]


async def main() -> int:
    print(f"seeding {len(RUNBOOKS)} docs into '{BUCKET}' via {MCP_URL}", file=sys.stderr)
    async with nebula_mcp(token=os.environ.get("NEBULA_TOKEN")) as session:
        for doc_id, metadata, text in RUNBOOKS:
            res = await session.call_tool(
                "insert_document",
                {"bucket": BUCKET, "doc_id": doc_id, "text": text, "metadata": metadata},
            )
            status = "error" if res.is_error else "ok"
            print(f"  {status:5} {doc_id}", file=sys.stderr)
            if res.is_error:
                print(res.content[0].text, file=sys.stderr)
                return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
