#!/usr/bin/env python3
"""
Simple streaming transaction generator.
Emits JSON objects (one per line) to stdout at the configured rate per second.
Usage: python generate_transactions.py --rate 5 --total 1000
"""
import argparse
import time
import json
import random
import uuid
import sys
from datetime import datetime, timezone

def make_tx():
    # create an ISO8601 UTC timestamp ending with Z
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": ts,
        "user_id": f"user_{random.randint(1,2000)}",
        "card_id": f"card_{random.randint(1,5000)}",
        "amount": round(random.random()*1000, 2),
        "currency": random.choice(["USD","EUR","GBP","EGP"]),
        "merchant": random.choice(["store_A","store_B","store_C","online_X"]),
        "device_id": f"dev_{random.randint(1,3000)}",
        "ip": f"192.168.{random.randint(0,255)}.{random.randint(1,254)}",
        "is_high_risk_country": random.random() < 0.02,
        "labels": []  # reserved for later labeling
    }

def main(rate, total):
    interval = 1.0 / rate if rate > 0 else 0.0
    emitted = 0
    try:
        while total <= 0 or emitted < total:
            tx = make_tx()
            print(json.dumps(tx), flush=True)
            emitted += 1
            if interval > 0:
                time.sleep(interval)
    except BrokenPipeError:
        # NiFi may close the pipe if it stops the process; exit quietly
        sys.exit(0)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=float, default=5.0, help="messages per second")
    p.add_argument("--total", type=int, default=0, help="total messages (0 = infinite)")
    args = p.parse_args()
    main(args.rate, args.total)
