#!/usr/bin/env python3
"""Keep running discovery batches until out_us_devs has 500 unique emails."""
from __future__ import annotations

import csv
import subprocess
import sys
import time
from pathlib import Path

OUT = Path("out_us_devs")
TARGET = 500
QUERIES = "queries_expand_500.txt"
LIMIT = 350
MAX_BATCHES = 40
BASE_SEED = 20261117


def unique_count() -> int:
    path = OUT / "emails.csv"
    if not path.exists():
        return 0
    with path.open(encoding="utf-8-sig") as fh:
        return sum(1 for _ in csv.DictReader(fh))


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    batch = 0
    while True:
        n = unique_count()
        print(f"\n=== progress: {n}/{TARGET} unique emails ===", flush=True)
        if n >= TARGET:
            print(f"reached target {TARGET}")
            return 0
        if batch >= MAX_BATCHES:
            print(f"stopped after {MAX_BATCHES} batches at {n} emails", file=sys.stderr)
            return 1
        batch += 1
        seed = BASE_SEED + batch * 97
        need = TARGET - n
        # Oversample candidates vs remaining emails (many already processed / no email)
        limit = max(LIMIT, min(500, need * 4))
        cmd = [
            sys.executable, "-u", "gh_email_scraper_20261114.py", "run",
            "--queries-file", QUERIES,
            "--limit", str(limit),
            "--max-emails", str(TARGET),
            "--min-score", "1",
            "--seed", str(seed),
            "--out", str(OUT),
        ]
        print(f"batch {batch}/{MAX_BATCHES}: seed={seed} limit={limit} need={need}", flush=True)
        print(" ".join(cmd), flush=True)
        started = time.time()
        rc = subprocess.call(cmd)
        elapsed = round(time.time() - started, 1)
        n2 = unique_count()
        print(f"batch {batch} exit={rc} +{n2 - n} emails ({elapsed}s) -> {n2}/{TARGET}", flush=True)
        if rc != 0:
            print("batch failed; retrying after short pause", flush=True)
            time.sleep(15)
            continue
        if n2 <= n:
            print("no new emails this batch; continuing with next seed", flush=True)
            time.sleep(5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
