"""Lambda consumer: Kinesis -> S3 raw landing zone (micro-batches).

Each invocation receives a batch of Kinesis records, decodes the JSON events,
and writes ONE gzip CSV with the same column contract as the batch files to
    s3://<BUCKET>/<PREFIX>/raw/transactions/source=stream/ingest_date=YYYY-MM-DD/
so the Glue job picks stream and batch data up with the same reader.

Validation is deliberately NOT done here: bad events are landed as-is and the
Glue job quarantines them, keeping raw/ an immutable record of what arrived.

Metrics are emitted with CloudWatch Embedded Metric Format (no extra API calls).
Env vars: BUCKET, PREFIX (default retail-bank)
"""
import base64
import csv
import gzip
import io
import json
import os
import time
import uuid
from datetime import datetime, timezone

import boto3

RAW_TXN_COLUMNS = [
    "transaction_id", "transaction_timestamp", "source_account_id", "counterparty_id",
    "transaction_type", "amount", "channel", "merchant_category", "branch_id",
    "transaction_status", "balance_before", "balance_after", "is_fraud_flag",
]
s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET"]
PREFIX = os.environ.get("PREFIX", "retail-bank")


def emit_metrics(function_name, **metrics):
    print(json.dumps({
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": "RetailBank/Streaming",
                "Dimensions": [["FunctionName"]],
                "Metrics": [{"Name": k, "Unit": "Count"} for k in metrics],
            }],
        },
        "FunctionName": function_name,
        **metrics,
    }))


def handler(event, context):
    rows, undecodable = [], 0
    for record in event.get("Records", []):
        try:
            payload = json.loads(base64.b64decode(record["kinesis"]["data"]))
            rows.append(["" if payload.get(c) is None else payload.get(c) for c in RAW_TXN_COLUMNS])
        except (ValueError, KeyError, TypeError):
            undecodable += 1

    key = None
    if rows:
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(RAW_TXN_COLUMNS)
        writer.writerows(rows)
        now = datetime.now(timezone.utc)
        key = (f"{PREFIX}/raw/transactions/source=stream/ingest_date={now:%Y-%m-%d}/"
               f"{now:%H%M%S}-{uuid.uuid4().hex[:8]}.csv.gz")
        s3.put_object(Bucket=BUCKET, Key=key, Body=gzip.compress(buf.getvalue().encode("utf-8")),
                      ContentType="text/csv", ContentEncoding="gzip")

    name = getattr(context, "function_name", "local")
    emit_metrics(name, RecordsReceived=len(event.get("Records", [])),
                 RecordsWritten=len(rows), RecordsUndecodable=undecodable)
    return {"written": len(rows), "undecodable": undecodable, "s3_key": key}
