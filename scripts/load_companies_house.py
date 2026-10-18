#!/usr/bin/env python3
"""Load Companies House BasicCompanyData CSV rows into a NebulaDB bucket.

usage: load_ch.py CSV --limit N [--skip N] [--bucket companies_house] [--active-only]
                     [--url https://showcase.nebuladb.net] [--batch 500] [--dry-run]
"""
import argparse, csv, http.client, json, os, sys, time, urllib.request, urllib.error

p = argparse.ArgumentParser()
p.add_argument("csv")
p.add_argument("--limit", type=int, required=True)
p.add_argument("--skip", type=int, default=0, help="skip this many matching rows first")
p.add_argument("--bucket", default="companies_house")
p.add_argument("--url", default="https://showcase.nebuladb.net")
p.add_argument("--batch", type=int, default=500)
p.add_argument("--active-only", action="store_true")
p.add_argument("--dry-run", action="store_true")
p.add_argument("--snapshot", default="2026-10-01")
p.add_argument("--max-mem-gb", type=float, default=25.0)
p.add_argument("--resume-mem-gb", type=float, default=21.0)
p.add_argument("--max-wait-s", type=int, default=1800)
a = p.parse_args()


def leader_mem_gb():
    try:
        with urllib.request.urlopen(f"{a.url.rstrip('/')}/api/v1/admin/reliability", timeout=30) as r:
            return json.loads(r.read())["sample"]["memory_used_bytes"] / 1e9
    except Exception:
        return float("inf")  # unknown -> treat as high, pause

SOURCE = f"ch_bulk_{a.snapshot}"


def g(row, k):
    return (row.get(k) or "").strip()


def to_doc(row):
    num = g(row, "CompanyNumber")
    name = g(row, "CompanyName")
    addr = ", ".join(x for x in (
        g(row, "RegAddress.CareOf"), g(row, "RegAddress.POBox"),
        g(row, "RegAddress.AddressLine1"), g(row, "RegAddress.AddressLine2"),
        g(row, "RegAddress.PostTown"), g(row, "RegAddress.County"),
        g(row, "RegAddress.Country"), g(row, "RegAddress.PostCode")) if x)
    sics = [g(row, f"SICCode.SicText_{i}") for i in range(1, 5)]
    sics = [s for s in sics if s and s.lower() != "none supplied"]
    prev = [g(row, f"PreviousName_{i}.CompanyName") for i in range(1, 11)]
    prev = [x for x in prev if x]
    parts = [
        f"{name} (company number {num}) is a {g(row, 'CompanyCategory')} registered with UK Companies House.",
        f"Status: {g(row, 'CompanyStatus')}.",
    ]
    if g(row, "IncorporationDate"):
        parts.append(f"Incorporated: {g(row, 'IncorporationDate')}.")
    if g(row, "DissolutionDate"):
        parts.append(f"Dissolved: {g(row, 'DissolutionDate')}.")
    if addr:
        parts.append(f"Registered office: {addr}.")
    if sics:
        parts.append("Nature of business (SIC): " + "; ".join(sics) + ".")
    if g(row, "Accounts.AccountCategory"):
        parts.append(f"Accounts category: {g(row, 'Accounts.AccountCategory')}; last made up {g(row, 'Accounts.LastMadeUpDate') or 'n/a'}, next due {g(row, 'Accounts.NextDueDate') or 'n/a'}.")
    if prev:
        parts.append("Previous names: " + "; ".join(prev) + ".")
    return {
        "id": f"{num}-register",
        "text": " ".join(parts),
        "metadata": {
            "source": SOURCE,
            "kind": "register_basic",
            "company_number": num,
            "company_name": name,
            "status": g(row, "CompanyStatus"),
            "category": g(row, "CompanyCategory"),
            "incorporation_date": g(row, "IncorporationDate"),
            "post_town": g(row, "RegAddress.PostTown"),
            "postcode": g(row, "RegAddress.PostCode"),
            "sic_codes": sics,
            "uri": g(row, "URI"),
        },
    }


def post(items):
    url = f"{a.url.rstrip('/')}/api/v1/bucket/{a.bucket}/docs/bulk"
    headers = {"Content-Type": "application/json"}
    if os.environ.get("NEBULA_TOKEN"):
        headers["Authorization"] = f"Bearer {os.environ['NEBULA_TOKEN']}"
    body = json.dumps({"items": items}).encode()
    for attempt in range(5):
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=300) as r:
                return json.loads(r.read())
        except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.HTTPException) as e:
            detail = e.read()[:300] if isinstance(e, urllib.error.HTTPError) else e
            print(f"  batch retry {attempt+1}: {detail}", flush=True)
            if isinstance(e, urllib.error.HTTPError) and e.code == 400:
                raise
            time.sleep(5 * (attempt + 1))
    raise SystemExit("batch failed after retries")


matched = sent = inserted = 0
batch = []
t0 = time.time()
with open(a.csv, newline="", encoding="utf-8", errors="replace") as f:
    r = csv.reader(f)
    header = [h.strip() for h in next(r)]
    for raw in r:
        if len(raw) < len(header):
            continue
        row = dict(zip(header, raw))
        if a.active_only and g(row, "CompanyStatus") != "Active":
            continue
        if not g(row, "CompanyNumber") or not g(row, "CompanyName"):
            continue
        matched += 1
        if matched <= a.skip:
            continue
        batch.append(to_doc(row))
        if len(batch) >= a.batch or sent + len(batch) >= a.limit:
            if a.dry_run:
                print(json.dumps(batch[0], indent=1)); sys.exit(0)
            mem = leader_mem_gb()
            if mem > a.max_mem_gb:
                print(f"PAUSE: leader memory {mem:.1f} GB > {a.max_mem_gb} GB (sent={sent}, resume_skip={matched-len(batch)})", flush=True)
                waited = 0
                while mem > a.resume_mem_gb:
                    if waited >= a.max_wait_s:
                        raise SystemExit(f"ABORT: memory stayed {mem:.1f} GB > {a.resume_mem_gb} GB for {waited}s (sent={sent}, resume_skip={matched-len(batch)})")
                    time.sleep(60); waited += 60
                    mem = leader_mem_gb()
                print(f"RESUME: leader memory {mem:.1f} GB after {waited}s", flush=True)
            res = post(batch)
            sent += len(batch); inserted += res.get("inserted", 0)
            batch = []
            el = time.time() - t0
            print(f"sent={sent} inserted={inserted} rate={sent/el:.0f}/s mem={mem:.1f}GB", flush=True)
            if sent >= a.limit:
                break
print(f"done: sent={sent} inserted={inserted} last_matched_row={matched} in {time.time()-t0:.0f}s")
