#!/usr/bin/env python3
"""Create the Athena mart views and export each one to CSV for Tableau.

Why export? Tableau Public (free) can't connect live to Athena, and a Tableau
Public link is the easiest way for recruiters to open the dashboard. The marts
are pre-aggregated, so the CSVs are small and safe to commit.
(With Tableau Desktop you can instead connect live: Connect -> Amazon Athena.)

Usage:
    python tableau/export_marts.py                     # reads config.env for region/project
    python tableau/export_marts.py --skip-create       # only re-export
"""
import argparse
import re
import time
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[1]
MARTS = ["mart_daily_transactions", "mart_monthly_kpis", "mart_monthly_segment", "mart_customer_value",
         "mart_merchant_monthly", "mart_branch_activity", "mart_account_book", "mart_dq_rule_results",
         "mart_dq_run_history", "mart_quarantine_reasons"]


def read_config():
    cfg = {}
    path = ROOT / "config.env"
    if path.exists():
        for line in path.read_text().splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip()
    return cfg


def run_query(athena, sql, database, workgroup):
    qid = athena.start_query_execution(QueryString=sql, QueryExecutionContext={"Database": database},
                                       WorkGroup=workgroup)["QueryExecutionId"]
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = q["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1.5)
    if state != "SUCCEEDED":
        raise RuntimeError(f"{state}: {q['Status'].get('StateChangeReason')}\n{sql[:300]}")
    scanned = q["Statistics"].get("DataScannedInBytes", 0) / 1e6
    return q["ResultConfiguration"]["OutputLocation"], scanned


def main():
    cfg = read_config()
    project = cfg.get("PROJECT", "retail-bank")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--region", default=cfg.get("AWS_REGION"))
    ap.add_argument("--database", default="retail_bank_analytics")
    ap.add_argument("--workgroup", default=f"{project}-wg")
    ap.add_argument("--out", default=str(ROOT / "tableau" / "data"))
    ap.add_argument("--skip-create", action="store_true")
    args = ap.parse_args()

    athena = boto3.client("athena", region_name=args.region)
    s3 = boto3.client("s3", region_name=args.region)

    if not args.skip_create:
        sql_text = (ROOT / "sql" / "01_create_marts.sql").read_text()
        sql_text = re.sub(r"--[^\n]*", "", sql_text)
        for stmt in [s.strip() for s in sql_text.split(";") if s.strip()]:
            run_query(athena, stmt, args.database, args.workgroup)
            print("created", re.search(r"VIEW\s+(\w+)", stmt).group(1))

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for mart in MARTS:
        location, mb = run_query(athena, f"SELECT * FROM {mart}", args.database, args.workgroup)
        bucket, key = location.replace("s3://", "").split("/", 1)
        s3.download_file(bucket, key, str(out / f"{mart}.csv"))
        rows = sum(1 for _ in open(out / f"{mart}.csv", encoding="utf-8")) - 1
        print(f"exported {mart:<28} {rows:>8,} rows  ({mb:,.1f} MB scanned)")


if __name__ == "__main__":
    main()
