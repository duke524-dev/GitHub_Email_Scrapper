#!/usr/bin/env python3
"""
Daily collector: fetch 500 NEW emails.

Outputs (under out_us_devs/):
  - emails.csv                  total master list (new rows inserted each batch)
  - new_emails_YYYYMMDD_HHMMSS.csv
      one file at the end of the daily job with all newly fetched emails
"""
from __future__ import annotations

import csv
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

OUT = Path("out_us_devs")
TOTAL = OUT / "emails.csv"
TARGET = 500
QUERIES = "queries_expand_500.txt"
LIMIT = 350
MAX_BATCHES = 40
EMAIL_FIELDS = [
    "email", "name", "login", "company", "location",
    "us_match", "us_evidence", "role_score", "role_signals",
    "email_type", "email_source", "repo", "commit_sha",
    "commit_date", "commit_subject", "profile_url", "collected_at",
]


def load_total_rows() -> dict[str, dict]:
    if not TOTAL.exists():
        return {}
    with TOTAL.open(encoding="utf-8-sig") as fh:
        return {row["email"]: row for row in csv.DictReader(fh) if row.get("email")}


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    before = load_total_rows()
    before_emails = set(before)
    start_count = len(before_emails)
    batch = 0
    day = datetime.now().strftime("%Y%m%d")
    job_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"total emails at start: {start_count}", flush=True)
    print(f"daily target: +{TARGET} new -> will write new_emails_{job_stamp}.csv when done", flush=True)

    while True:
        current = load_total_rows()
        gained = len(set(current) - before_emails)
        print(f"\n=== progress: {gained}/{TARGET} new | total {len(current)} ===", flush=True)
        if gained >= TARGET:
            break
        if batch >= MAX_BATCHES:
            print(f"stopped after {MAX_BATCHES} batches at +{gained} emails", file=sys.stderr)
            # still write whatever was collected
            break

        batch += 1
        need = TARGET - gained
        seed = int(day) * 100 + batch
        limit = max(LIMIT, min(500, need * 4))
        cmd = [
            sys.executable, "-u", "gh_email_scraper_20261114.py", "run",
            "--queries-file", QUERIES,
            "--limit", str(limit),
            "--max-new-emails", str(need),
            "--min-score", "1",
            "--seed", str(seed),
            "--out", str(OUT),
        ]
        print(f"batch {batch}/{MAX_BATCHES}: seed={seed} limit={limit} need={need}", flush=True)
        print(" ".join(cmd), flush=True)
        started = time.time()
        rc = subprocess.call(cmd)
        elapsed = round(time.time() - started, 1)
        current = load_total_rows()
        gained2 = len(set(current) - before_emails)
        print(f"batch {batch} exit={rc} +{gained2 - gained} ({elapsed}s) "
              f"-> +{gained2}/{TARGET} new | total {len(current)}", flush=True)
        if rc != 0:
            print("batch failed; retrying after short pause", flush=True)
            time.sleep(15)
            continue
        if gained2 <= gained:
            print("no new emails this batch; continuing with next seed", flush=True)
            time.sleep(5)

    # Write one date+time file with all emails newly fetched in this daily job
    current = load_total_rows()
    new_rows = [current[e] for e in current if e not in before_emails]
    new_rows.sort(key=lambda r: (-int(r.get("role_score") or 0), r.get("email", "")))
    out_path = OUT / f"new_emails_{job_stamp}.csv"
    with out_path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=EMAIL_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in new_rows:
            writer.writerow({k: row.get(k, "") for k in EMAIL_FIELDS})

    print(f"\ntotal emails csv: {TOTAL.resolve()} ({len(current)} rows)", flush=True)
    print(f"new emails csv:   {out_path.resolve()} ({len(new_rows)} rows)", flush=True)
    if len(new_rows) >= TARGET:
        print(f"reached daily target +{TARGET}")
        return 0
    print(f"finished short of target: +{len(new_rows)}/{TARGET}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
