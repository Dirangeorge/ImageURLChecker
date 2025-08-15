#!/usr/bin/env python3
import csv
import os
import sys
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd

DEFAULT_TIMEOUT = 10
MAX_WORKERS = 24
RETRIES = 2

def check_one(url: str, timeout: int = DEFAULT_TIMEOUT) -> int | str:
    if not isinstance(url, str) or not url.strip():
        return "empty"
    url = url.strip()

    for attempt in range(RETRIES + 1):
        try:
            # Try HEAD first (fast); some servers don’t love HEAD so fall back to GET
            r = requests.head(url, allow_redirects=True, timeout=timeout)
            if r.status_code == 405 or (r.status_code >= 400 and r.status_code != 404):
                r = requests.get(url, allow_redirects=True, timeout=timeout, stream=True)
            return r.status_code
        except requests.RequestException as e:
            if attempt < RETRIES:
                # brief backoff then retry
                time.sleep(0.5 * (attempt + 1))
                continue
            return f"error: {type(e).__name__}"

def main():
    parser = argparse.ArgumentParser(description="Filter rows where IMAGE_URLS return HTTP 404 (or other errors).")
    parser.add_argument("--input", required=True, help="Path to input CSV (from /input)")
    parser.add_argument("--output", required=True, help="Path to write filtered CSV (to /output)")
    parser.add_argument("--column", default="IMAGE_URLS", help="Column name containing the image URL")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help="Max threads")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Per-request timeout seconds")
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    if args.column not in df.columns:
        raise SystemExit(f"Column '{args.column}' not found. Columns: {list(df.columns)}")

    urls = df[args.column].tolist()

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        future_to_idx = {ex.submit(check_one, url, args.timeout): i for i, url in enumerate(urls)}
        for future in as_completed(future_to_idx):
            i = future_to_idx[future]
            try:
                status = future.result()
            except Exception as e:
                status = f"error: {type(e).__name__}"
            results.append((i, status))

    # Map results back to rows
    status_map = {i: s for i, s in results}

    # Keep rows where status is 404 or any >=400 or an error
    def is_broken(s):
        if isinstance(s, int):
            return s >= 400
        return True  # any error/empty counts as broken

    df["IMAGE_STATUS"] = df.index.map(lambda i: status_map.get(i, "error: missing"))
    broken_df = df[df["IMAGE_STATUS"].apply(is_broken)].copy()

    # Save only relevant columns + status (keep full row so you can identify the product)
    if not os.path.isdir(os.path.dirname(args.output)) and os.path.dirname(args.output):
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
    broken_df.to_csv(args.output, index=False)

    print(f"Checked {len(df)} rows.")
    print(f"Broken rows: {len(broken_df)}")
    print(f"Wrote: {args.output}")

if __name__ == "__main__":
    main()
