#!/usr/bin/env python3
"""
test_simulator.py — Runs the Pseudogram simulation and compares results.

Usage:
    python test_simulator.py --url https://your-app.onrender.com
    python test_simulator.py --url http://localhost:8000  (local testing)

Steps performed:
  1. Reset the server's database (/reset)
  2. Create a PRICE rule on the server
  3. Start a simulation run (500 events, 10s)
  4. Wait for the simulation to finish + extra drain time
  5. Fetch /v1/simulate/{run_id}/truth from Pseudogram
  6. Fetch /stats from our server
  7. Compare and print discrepancies
"""

import argparse
import time
import httpx
import sys

PSEUDOGRAM_BASE = "https://pseudogram-api.onrender.com"


def main():
    parser = argparse.ArgumentParser(description="LinkPlease Simulator Test")
    parser.add_argument("--url", required=True, help="Base URL of deployed LinkPlease server")
    parser.add_argument("--api-key", default=None, help="Pseudogram API key (or set PSEUDOGRAM_API_KEY env)")
    parser.add_argument("--count", type=int, default=500, help="Number of events to simulate")
    parser.add_argument("--duration", type=int, default=10, help="Simulation duration in seconds")
    parser.add_argument("--drain", type=int, default=120, help="Extra seconds to wait for DM drain after simulation")
    args = parser.parse_args()

    import os
    api_key = args.api_key or os.getenv("PSEUDOGRAM_API_KEY", "")
    if not api_key:
        print("ERROR: No API key provided. Use --api-key or set PSEUDOGRAM_API_KEY.")
        sys.exit(1)

    server_url = args.url.rstrip("/")
    headers = {"X-API-Key": api_key}

    print(f"\n{'='*60}")
    print(f"  LinkPlease Simulator Test")
    print(f"  Server: {server_url}")
    print(f"  Events: {args.count} over {args.duration}s")
    print(f"{'='*60}\n")

    # ── Step 1: Reset server state ────────────────────────────
    print("Step 1: Resetting server database...")
    resp = httpx.delete(f"{server_url}/reset", timeout=10)
    if resp.status_code not in (200, 404):
        print(f"  WARNING: /reset returned {resp.status_code}: {resp.text}")
    else:
        print(f"  ✓ Reset: {resp.json()}")

    # ── Step 2: Create PRICE rule ─────────────────────────────
    print("\nStep 2: Creating PRICE rule...")
    resp = httpx.post(
        f"{server_url}/rules",
        json={"keyword": "PRICE", "dm_message": "Here is the price info you asked for!"},
        timeout=10,
    )
    if resp.status_code not in (200, 201):
        print(f"  ERROR creating rule: {resp.status_code} {resp.text}")
        sys.exit(1)
    rule_data = resp.json()
    print(f"  ✓ Rule created: rule_id={rule_data.get('rule_id')}, keyword={rule_data.get('keyword')}")

    # ── Step 3: Start simulation ──────────────────────────────
    webhook_url = f"{server_url}/webhook"
    print(f"\nStep 3: Starting simulation → webhook_url={webhook_url}")
    resp = httpx.post(
        f"{PSEUDOGRAM_BASE}/v1/simulate/start",
        json={
            "webhook_url": webhook_url,
            "count": args.count,
            "duration_seconds": args.duration,
        },
        headers=headers,
        timeout=30,
    )
    if resp.status_code != 200:
        print(f"  ERROR starting simulation: {resp.status_code} {resp.text}")
        sys.exit(1)
    sim_data = resp.json()
    run_id = sim_data.get("run_id")
    print(f"  ✓ Simulation started: run_id={run_id}")
    print(f"    {sim_data}")

    # ── Step 4: Wait for simulation + drain ───────────────────
    total_wait = args.duration + args.drain
    print(f"\nStep 4: Waiting {args.duration}s (simulation) + {args.drain}s (DM drain) = {total_wait}s total...")
    for i in range(total_wait):
        time.sleep(1)
        if (i + 1) % 15 == 0 or i == 0:
            # Peek at current stats
            try:
                s = httpx.get(f"{server_url}/stats", timeout=5).json()
                print(f"  [{i+1:3d}s] stats: sent={s.get('sent')}, queued={s.get('queued')}, "
                      f"failed={s.get('failed')}, dups={s.get('duplicates_blocked')}")
            except Exception as e:
                print(f"  [{i+1:3d}s] Could not fetch stats: {e}")

    # ── Step 5: Fetch ground truth ────────────────────────────
    print(f"\nStep 5: Fetching ground truth from Pseudogram (run_id={run_id})...")
    resp = httpx.get(
        f"{PSEUDOGRAM_BASE}/v1/simulate/{run_id}/truth",
        headers=headers,
        timeout=30,
    )
    if resp.status_code != 200:
        print(f"  ERROR fetching truth: {resp.status_code} {resp.text}")
        sys.exit(1)
    truth = resp.json()
    print(f"  ✓ Ground truth: {truth}")

    # ── Step 6: Fetch our stats ───────────────────────────────
    print(f"\nStep 6: Fetching our /stats...")
    resp = httpx.get(f"{server_url}/stats", timeout=10)
    our_stats = resp.json()
    print(f"  ✓ Our stats: {our_stats}")

    # ── Step 7: Compare ───────────────────────────────────────
    print(f"\n{'='*60}")
    print("  COMPARISON")
    print(f"{'='*60}")

    fields = ["sent", "failed", "queued", "duplicates_blocked"]
    all_match = True
    for field in fields:
        truth_val = truth.get(field, "N/A")
        our_val = our_stats.get(field, "N/A")
        match = "✓" if truth_val == our_val else "✗"
        if truth_val != our_val:
            all_match = False
        print(f"  {match} {field:25s}: truth={truth_val}, ours={our_val}")

    print()
    if all_match:
        print("  🎉 ALL MATCH — PASS!")
    else:
        print("  ❌ MISMATCH — investigate and rerun.")
    print(f"{'='*60}\n")

    # ── Detailed truth dump ───────────────────────────────────
    print("Full truth payload:")
    import json
    print(json.dumps(truth, indent=2))
    print("\nFull our stats payload:")
    print(json.dumps(our_stats, indent=2))

    return 0 if all_match else 1


if __name__ == "__main__":
    sys.exit(main())
