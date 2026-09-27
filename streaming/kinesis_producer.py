#!/usr/bin/env python3
"""Replay held-out transactions into Amazon Kinesis as JSON events.

Reads data/output/stream/stream_events.csv (created by prepare_transactions.py).
By default events are re-stamped to "now" so the dashboard shows data freshness,
and a small share are deliberately corrupted so the streaming path also
exercises the quarantine logic.

Usage:
    python streaming/kinesis_producer.py --stream retail-bank-transactions --limit 5000 --rps 200
"""
import argparse
import csv
import json
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "data_generator"))
from common import STREAM_DIR  # noqa: E402


def corrupt(event: dict) -> dict:
    kind = random.choice(["null_account", "negative_amount", "bad_type", "null_channel"])
    if kind == "null_account":
        event["source_account_id"] = None
    elif kind == "negative_amount":
        event["amount"] = f"-{event['amount']}"
    elif kind == "bad_type":
        event["transaction_type"] = "UNKNOWN"
    else:
        event["channel"] = None
    return event


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stream", required=True)
    ap.add_argument("--region", default=None)
    ap.add_argument("--file", default=str(STREAM_DIR / "stream_events.csv"))
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument("--rps", type=int, default=200, help="records per second (1 shard handles 1000)")
    ap.add_argument("--bad-rate", type=float, default=0.02)
    ap.add_argument("--no-restamp", action="store_true", help="keep original timestamps")
    args = ap.parse_args()

    kinesis = boto3.client("kinesis", region_name=args.region)
    sent = failed = 0
    batch = []
    now = datetime.now(timezone.utc)

    def flush():
        nonlocal sent, failed, batch
        if not batch:
            return
        resp = kinesis.put_records(StreamName=args.stream, Records=batch)
        failed += resp.get("FailedRecordCount", 0)
        sent += len(batch) - resp.get("FailedRecordCount", 0)
        batch = []

    with open(args.file, newline="") as fh:
        for i, row in enumerate(csv.DictReader(fh)):
            if i >= args.limit:
                break
            event = {k: (v if v != "" else None) for k, v in row.items()}
            if not args.no_restamp:
                ts = now - timedelta(seconds=random.randint(0, 3600))
                event["transaction_timestamp"] = ts.strftime("%Y-%m-%d %H:%M:%S")
            if random.random() < args.bad_rate:
                event = corrupt(event)
            key = event.get("source_account_id") or event.get("transaction_id") or str(i)
            batch.append({"Data": json.dumps(event).encode("utf-8"), "PartitionKey": key})
            if len(batch) == min(500, args.rps):
                flush()
                time.sleep(min(500, args.rps) / args.rps)
                print(f"sent={sent:,} failed={failed:,}", end="\r")
    flush()
    print(f"\nDone. sent={sent:,} failed={failed:,}")


if __name__ == "__main__":
    main()
