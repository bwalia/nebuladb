"""Run a NebulaDB agent, or the supervisor, from the command line.

    python run.py sre "Is the cluster healthy? Anything slow?"
    python run.py supervisor "Why has our customer API become slower?"
    python run.py assistant --user u-42 "I'm moving billing to NebulaDB. What's our DR process?"

Tool calls stream to stderr as they happen; the final report goes to stdout.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

import anthropic

from nebula_agents import AGENTS, MCP_URL, MODEL, Team, nebula_mcp

# The showcase's canned questions (AgentsTab.tsx), used when no task is given.
DEFAULT_TASKS = {
    "db": "What data is in this database? Find anything about disaster recovery with SQL.",
    "rag": "What is our disaster recovery process?",
    "sre": "Give me a health and performance read-out of this NebulaDB deployment.",
    "sec": "Review recent API access for anything suspicious.",
    "backup": "Could we recover if we lost the data directory right now?",
    "assistant": "What do you remember about me, and what should I read first?",
    "supervisor": "Why has our customer API become slower?",
}


async def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("agent", choices=[*AGENTS, "supervisor"])
    p.add_argument("task", nargs="?", help="what to ask (defaults to the showcase question)")
    p.add_argument("--user", help="user id for the memory-backed assistant")
    args = p.parse_args()

    if args.agent == "assistant" and not args.user:
        p.error("the assistant needs --user so memories are scoped to someone")
    task = args.task or DEFAULT_TASKS[args.agent]

    print(f"{args.agent} · {MODEL} · {MCP_URL}\n> {task}\n", file=sys.stderr)
    async with nebula_mcp(token=os.environ.get("NEBULA_TOKEN")) as session:
        team = Team(anthropic.AsyncAnthropic(), session)
        try:
            if args.agent == "supervisor":
                report = await team.supervise(task)
            else:
                report = await team.run(args.agent, task, user_id=args.user)
        except anthropic.AuthenticationError:
            print("Anthropic credentials were rejected: check ANTHROPIC_API_KEY.", file=sys.stderr)
            return 2
        except TypeError as e:
            # The SDK raises TypeError when it finds no credentials at all.
            if "authentication method" not in str(e):
                raise
            print("No Anthropic credentials: set ANTHROPIC_API_KEY.", file=sys.stderr)
            return 2
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
