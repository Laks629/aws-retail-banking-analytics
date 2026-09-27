#!/usr/bin/env python3
"""Map the public PaySim dataset onto the retail-banking transaction schema.

PaySim (Kaggle: ealaxi/paysim1) is a synthetic mobile-money simulation with
~6.36M rows. This script streams it in chunks (so it runs on a laptop) and:

  * renames/maps fields to the project's raw transaction contract (common.RAW_TXN_COLUMNS)
  * maps PaySim types -> banking types (CASH_IN -> DEPOSIT, CASH_OUT -> WITHDRAWAL, ...)
  * stretches the 743 hourly "steps" across ~12 months starting 2025-07-01
  * pools PaySim's millions of one-off origin IDs into the generated account master
    (stable hash), so customers/accounts have repeat behaviour
  * adds synthetic channel, merchant category, branch and posting status
  * writes gzip CSV batches (one per chunk) and holds out the newest rows as
    events for the Kinesis producer

Run generate_dimensions.py first.

Usage:
    python data_generator/prepare_transactions.py --input data/source/PS_20174392719_1491204439457_log.csv
    python data_generator/prepare_transactions.py --input ... --max-rows 300000   # dev sample
"""
import argparse

import numpy as np
import pandas as pd

from common import CLEAN_DIR, RAW_TXN_COLUMNS, STREAM_DIR

TYPE_MAP = {"CASH_IN": "DEPOSIT", "CASH_OUT": "WITHDRAWAL", "PAYMENT": "PAYMENT",
            "TRANSFER": "TRANSFER", "DEBIT": "DEBIT"}
CHANNEL_WEIGHTS = {
    "DEPOSIT":    (["BRANCH", "ATM", "MOBILE", "ONLINE"], [0.35, 0.35, 0.25, 0.05]),
    "WITHDRAWAL": (["ATM", "BRANCH"], [0.75, 0.25]),
    "PAYMENT":    (["ONLINE", "MOBILE", "BRANCH"], [0.45, 0.45, 0.10]),
    "TRANSFER":   (["MOBILE", "ONLINE", "BRANCH"], [0.50, 0.40, 0.10]),
    "DEBIT":      (["POS", "ONLINE"], [0.70, 0.30]),
}
MERCHANT_CATEGORIES = (["GROCERY", "UTILITIES", "DINING", "RETAIL", "TRAVEL",
                        "HEALTHCARE", "FUEL", "ENTERTAINMENT"],
                       [0.22, 0.15, 0.14, 0.18, 0.06, 0.08, 0.10, 0.07])
BASE_TS = pd.Timestamp("2025-07-01 00:00:00")
HOURS_PER_STEP = 12  # 743 steps * 12h ~= 371 days


def transform_chunk(df: pd.DataFrame, start_idx: int, n_accounts: int,
                    branch_ids: np.ndarray, rng: np.random.Generator) -> pd.DataFrame:
    n = len(df)
    out = pd.DataFrame(index=df.index)
    out["transaction_id"] = [f"TXN{i:010d}" for i in range(start_idx, start_idx + n)]

    seconds = (df["step"].to_numpy(dtype=np.int64) - 1) * HOURS_PER_STEP * 3600 \
        + rng.integers(0, HOURS_PER_STEP * 3600, n)
    out["transaction_timestamp"] = (BASE_TS + pd.to_timedelta(seconds, unit="s")).strftime("%Y-%m-%d %H:%M:%S")

    hashed = pd.util.hash_pandas_object(df["nameOrig"].astype(str), index=False).to_numpy()
    acct_num = (hashed % np.uint64(n_accounts)).astype(np.int64) + 1
    out["source_account_id"] = [f"ACC{x:07d}" for x in acct_num]
    out["counterparty_id"] = df["nameDest"].astype(str).to_numpy()

    ttype = df["type"].map(TYPE_MAP).to_numpy()
    out["transaction_type"] = ttype
    amount = df["amount"].to_numpy(dtype=float)
    out["amount"] = np.round(amount, 2)

    channel = np.empty(n, dtype=object)
    for t, (choices, weights) in CHANNEL_WEIGHTS.items():
        mask = ttype == t
        channel[mask] = rng.choice(choices, size=int(mask.sum()), p=weights)
    out["channel"] = channel

    merchant = np.full(n, "", dtype=object)
    m_mask = np.isin(ttype, ["PAYMENT", "DEBIT"])
    merchant[m_mask] = rng.choice(MERCHANT_CATEGORIES[0], size=int(m_mask.sum()), p=MERCHANT_CATEGORIES[1])
    out["merchant_category"] = merchant

    branch = np.full(n, "", dtype=object)
    b_mask = np.isin(channel, ["BRANCH", "ATM"])
    branch[b_mask] = rng.choice(branch_ids, size=int(b_mask.sum()))
    out["branch_id"] = branch

    balance_before = df["oldbalanceOrg"].to_numpy(dtype=float)
    outgoing = ttype != "DEPOSIT"
    insufficient = outgoing & (amount > balance_before)
    r = rng.random(n)
    status = np.full(n, "POSTED", dtype=object)
    status[(insufficient & (r < 0.05)) | (r > 0.996)] = "FAILED"
    status[(status == "POSTED") & (rng.random(n) < 0.002)] = "REVERSED"
    out["transaction_status"] = status

    out["balance_before"] = np.round(balance_before, 2)
    out["balance_after"] = np.round(df["newbalanceOrig"].to_numpy(dtype=float), 2)
    out["is_fraud_flag"] = df["isFraud"].astype(int).to_numpy()
    return out[RAW_TXN_COLUMNS]


def lookahead(iterable):
    """Yield (item, is_last) pairs."""
    it = iter(iterable)
    try:
        prev = next(it)
    except StopIteration:
        return
    for item in it:
        yield prev, False
        prev = item
    yield prev, True


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", required=True, help="Path to the PaySim CSV")
    ap.add_argument("--chunk-size", type=int, default=500_000)
    ap.add_argument("--max-rows", type=int, default=None, help="Only read the first N rows (dev)")
    ap.add_argument("--stream-holdout", type=int, default=20_000,
                    help="Newest N rows are held out for the Kinesis producer")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    accounts = pd.read_csv(CLEAN_DIR / "accounts.csv", usecols=["account_id"])
    branches = pd.read_csv(CLEAN_DIR / "branches.csv", usecols=["branch_id"])
    n_accounts, branch_ids = len(accounts), branches["branch_id"].to_numpy()

    out_dir = CLEAN_DIR / "transactions"
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("*.csv.gz"):
        old.unlink()
    STREAM_DIR.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(args.seed)
    reader = pd.read_csv(args.input, chunksize=args.chunk_size, nrows=args.max_rows)
    start_idx, batch_no, total = 1, 0, 0
    for chunk, is_last in lookahead(reader):
        txn = transform_chunk(chunk, start_idx, n_accounts, branch_ids, rng)
        start_idx += len(chunk)
        if is_last and args.stream_holdout:
            holdout = txn.tail(args.stream_holdout)
            txn = txn.iloc[: len(txn) - len(holdout)]
            holdout.to_csv(STREAM_DIR / "stream_events.csv", index=False)
            print(f"Held out {len(holdout):,} newest rows -> {STREAM_DIR / 'stream_events.csv'}")
        if len(txn):
            batch_no += 1
            path = out_dir / f"batch_{batch_no:03d}.csv.gz"
            txn.to_csv(path, index=False, compression="gzip")
            total += len(txn)
            print(f"  batch {batch_no:03d}: {len(txn):,} rows -> {path.name}")
    print(f"Done: {total:,} batch transactions in {batch_no} files")


if __name__ == "__main__":
    main()
