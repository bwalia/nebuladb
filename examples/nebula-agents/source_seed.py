"""Seed the Source Supplies demo buckets for the agent team.

    python source_seed.py

The data lives once, in apps/showcase/src/source/data/*.ts (the showcase
seeds the same buckets when its tabs open). This script loads it with
Node (22.6+, for TypeScript type stripping) and writes it with the same
REST bulk upsert, doc ids and seed_version as the showcase's
ensureSeeded(), so the two never duplicate each other and a second run
writes 0 docs. No model calls, standard library only.

Environment: NEBULA_URL (default http://127.0.0.1:8080), NEBULA_TOKEN.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

NEBULA_URL = os.environ.get("NEBULA_URL", "http://127.0.0.1:8080").rstrip("/")
TOKEN = os.environ.get("NEBULA_TOKEN")
DATA = Path(__file__).resolve().parents[2] / "apps" / "showcase" / "src" / "source" / "data" / "index.ts"
BATCH = 32

DUMP = (
    "const m = await import(process.argv[1]);"
    "const out = Object.fromEntries(Object.entries(m.SEEDS).map(([k, s]) => [k, {bucket: s.bucket, version: s.version, docs: s.docs}]));"
    "process.stdout.write(JSON.stringify(out));"
)


def load_seeds() -> dict:
    url = DATA.as_uri()
    for flags in ([], ["--experimental-strip-types"]):
        r = subprocess.run(
            ["node", *flags, "--no-warnings", "--input-type=module", "-e", DUMP, url],
            capture_output=True,
            text=True,
        )
        if r.returncode == 0:
            return json.loads(r.stdout)
    raise SystemExit(f"could not load {DATA} with node (needs Node 22.6+):\n{r.stderr}")


def request(method: str, path: str, body: dict | None = None) -> tuple[int, dict | None]:
    headers = {"content-type": "application/json"}
    if TOKEN:
        headers["authorization"] = f"Bearer {TOKEN}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{NEBULA_URL}{path}", data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            text = resp.read().decode()
            return resp.status, json.loads(text) if text else None
    except urllib.error.HTTPError as e:
        return e.code, None


def seed(bucket: str, docs: list[dict], version: str) -> int:
    b = urllib.parse.quote(bucket, safe="")
    sentinel = urllib.parse.quote(docs[-1]["id"], safe="")
    status, doc = request("GET", f"/api/v1/bucket/{b}/doc/{sentinel}")
    if status == 200 and doc and doc.get("metadata", {}).get("seed_version") == version:
        return 0
    if status not in (200, 404):
        raise SystemExit(f"{bucket}: GET sentinel returned HTTP {status}")
    written = 0
    for i in range(0, len(docs), BATCH):
        items = [{**d, "metadata": {**d["metadata"], "seed_version": version}} for d in docs[i : i + BATCH]]
        status, out = request("POST", f"/api/v1/bucket/{b}/docs/bulk", {"items": items})
        if status != 200 or out is None:
            raise SystemExit(f"{bucket}: bulk upsert returned HTTP {status}")
        written += int(out.get("inserted", 0))
    return written


def main() -> int:
    seeds = load_seeds()
    print(f"seeding Source Supplies demo data into {NEBULA_URL}", file=sys.stderr)
    total = 0
    for s in seeds.values():
        n = seed(s["bucket"], s["docs"], s["version"])
        total += n
        print(f"  {s['bucket']:<18} {n:>4} written / {len(s['docs'])}", file=sys.stderr)
    print(f"done: {total} docs written (0 means already current)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
