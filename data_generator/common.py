"""Shared constants for the synthetic retail-banking data generators.

The raw transaction column order defined here is the contract between the
batch files, the Kinesis producer / Lambda consumer and the Glue ETL job.
"""
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
# DATA_OUTPUT_DIR lets tests run against a temp directory.
OUTPUT_DIR = Path(os.environ.get("DATA_OUTPUT_DIR", REPO_ROOT / "data" / "output"))
CLEAN_DIR = OUTPUT_DIR / "clean"      # generated, defect-free
RAW_DIR = OUTPUT_DIR / "raw"          # what gets uploaded to s3://.../raw/
STREAM_DIR = OUTPUT_DIR / "stream"    # held-out events for the Kinesis producer

RAW_TXN_COLUMNS = [
    "transaction_id",
    "transaction_timestamp",
    "source_account_id",
    "counterparty_id",
    "transaction_type",
    "amount",
    "channel",
    "merchant_category",
    "branch_id",
    "transaction_status",
    "balance_before",
    "balance_after",
    "is_fraud_flag",
]

N_CUSTOMERS = 25_000
N_ACCOUNTS = 40_000
N_BRANCHES = 40

TRANSACTION_TYPES = ["DEPOSIT", "WITHDRAWAL", "PAYMENT", "TRANSFER", "DEBIT"]
CHANNELS = ["MOBILE", "ONLINE", "ATM", "BRANCH", "POS"]
TRANSACTION_STATUSES = ["POSTED", "FAILED", "REVERSED"]
ACCOUNT_STATUSES = ["ACTIVE", "DORMANT", "CLOSED", "FROZEN"]


def account_id(n: int) -> str:
    return f"ACC{n:07d}"
