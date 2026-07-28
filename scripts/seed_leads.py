#!/usr/bin/env python3
"""
seed_leads.py — push a small sample `leads` corpus into every NebulaDB
showcase environment so the semantic-search presets return rows.

Stdlib only (urllib) — no pip installs. Idempotent: every lead has a
stable id, so re-running just refreshes the content.

Usage:
    python3 scripts/seed_leads.py                # seed all envs
    python3 scripts/seed_leads.py int prod       # seed only these envs
    python3 scripts/seed_leads.py --url http://localhost:8080   # one-off target
"""
import json
import os
import sys
import urllib.request
import urllib.error

# Public showcase endpoints, per environment. (int is int-nebuladb, the
# rest are *-showcase; prod is the bare showcase host.)
ENVS = {
    "int":  "https://int-nebuladb.nebuladb.net",
    "test": "https://test-showcase.nebuladb.net",
    "acc":  "https://acc-showcase.nebuladb.net",
    "prod": "https://showcase.nebuladb.net",
}

BUCKET = "leads"

# Sample corpus: (sector, name suffix, description). Covers every SqlTab
# preset theme (engineering, flowers, food, motor parts, training, ...).
SECTORS = [
    ("engineering", "Engineering Ltd", "Precision engineering services: CNC machining, metal fabrication, and mechanical design for industrial and manufacturing clients."),
    ("engineering", "Precision Works", "Structural and civil engineering services, including surveying, CAD design, and on-site project management."),
    ("flowers", "Florist", "Independent florist offering bespoke bouquets, wedding flowers, funeral tributes, and same-day local delivery."),
    ("flowers", "Blooms", "Flower shop and garden centre supplying fresh cut flowers, houseplants, and seasonal arrangements."),
    ("food", "Kitchen", "Family-run restaurant serving seasonal British food and locally sourced produce; catering and takeaway available."),
    ("food", "Fine Foods", "Artisan food producer and deli specialising in cheese, charcuterie, bakery goods, and wholesale supply."),
    ("motor", "Motors", "Supplier of aftermarket motor parts, car spares, brakes, filters, and vehicle accessories for trade and retail."),
    ("motor", "Auto Parts", "Motor factor stocking engine components, batteries, and performance car parts with next-day delivery."),
    ("training", "Training Academy", "Accredited training provider delivering health & safety, first aid, forklift, and leadership development courses."),
    ("training", "Skills Hub", "Vocational training and apprenticeships in construction, engineering, and digital skills for employers."),
    ("it", "Digital", "IT services and software consultancy: cloud migration, managed support, and web application development."),
    ("cleaning", "Cleaning Services", "Commercial cleaning and facilities management for offices, schools, and industrial premises."),
    ("construction", "Construction", "Building and construction contractor covering groundworks, refurbishment, and commercial fit-out."),
    ("logistics", "Logistics", "Freight, warehousing, and same-day courier logistics across the UK and Europe."),
]

CITIES = ["London", "Birmingham", "Manchester", "Leeds", "Bristol", "Sheffield", "Glasgow"]


def build_leads():
    """Deterministic ~50-doc corpus spread across cities and sectors."""
    items = []
    i = 0
    for c_idx, city in enumerate(CITIES):
        for s_idx, (sector, suffix, desc) in enumerate(SECTORS):
            if (c_idx + s_idx) % 2 != 0:
                continue
            i += 1
            name = f"{city} {suffix}"
            status = "1" if (i % 4 != 0) else "0"  # ~75% active
            items.append({
                "id": f"lead-{i:04d}",
                "text": f"{name} — {desc}",
                "metadata": {"company_name": name, "city": city, "status": status, "sector": sector},
            })
    return items


def post_json(url, payload, timeout=60):
    data = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    # When the server has auth enabled, export NEBULA_TOKEN=<an API key>.
    token = os.environ.get("NEBULA_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, json.loads(resp.read().decode())


def seed(name, base, items):
    url = f"{base.rstrip('/')}/api/v1/bucket/{BUCKET}/docs/bulk"
    try:
        status, body = post_json(url, {"items": items})
        inserted = body.get("inserted", "?")
        print(f"  {name:<5} {base}  ->  HTTP {status}  inserted={inserted}/{len(items)}")
        return status == 200
    except urllib.error.HTTPError as e:
        print(f"  {name:<5} {base}  ->  HTTP {e.code}  {e.read()[:200]!r}")
    except Exception as e:  # network/timeout/etc.
        print(f"  {name:<5} {base}  ->  ERROR  {e}")
    return False


def main(argv):
    args = argv[1:]
    # one-off target: --url <base>
    if "--url" in args:
        idx = args.index("--url")
        base = args[idx + 1]
        targets = {"custom": base}
    else:
        chosen = [a for a in args if not a.startswith("-")]
        targets = {k: ENVS[k] for k in chosen} if chosen else dict(ENVS)
        unknown = [a for a in chosen if a not in ENVS]
        if unknown:
            sys.exit(f"unknown env(s): {unknown}. choose from {list(ENVS)}")

    items = build_leads()
    print(f"seeding {len(items)} leads into '{BUCKET}' across {len(targets)} env(s):")
    ok = all(seed(name, base, items) for name, base in targets.items())
    print("done." if ok else "done (with errors).")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main(sys.argv)
