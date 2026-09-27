#!/usr/bin/env python3
"""Inject controlled data-quality defects and write the files that get uploaded to S3 raw/.

Every injected defect is recorded in data/output/injected_defects.json so the
pipeline's quarantine counts can be reconciled against a known ground truth
(an easy, credible interview talking point: "I injected N defects, the pipeline caught N").

Defects are applied to disjoint rows (one defect per row), so each injected
defect produces exactly one quarantined record.

Also applies *cosmetic* noise (lower-case / padded channel and type values)
that is NOT a defect - the ETL must standardise it, not quarantine it.

Usage:
    python data_generator/inject_dq_issues.py
    python data_generator/inject_dq_issues.py --scale 2.0   # double every defect rate
"""
import argparse
import json
import shutil

import numpy as np
import pandas as pd

from common import CLEAN_DIR, OUTPUT_DIR, RAW_DIR

# Share of rows per file receiving each defect (total ~1.6% + duplicates).
TXN_DEFECT_RATES = {
    "NULL_TXN_ID": 0.0005,
    "NULL_ACCOUNT": 0.0020,
    "ORPHAN_ACCOUNT": 0.0020,
    "NON_POSITIVE_AMOUNT": 0.0020,
    "INVALID_TYPE": 0.0015,
    "FUTURE_TIMESTAMP": 0.0015,
    "INVALID_TIMESTAMP": 0.0005,
    "NULL_CHANNEL": 0.0025,
    "DUPLICATE_TXN_ID": 0.0035,   # appended copies (exact + near-duplicates)
}
ACCOUNT_STATUS_DEFECT_RATE = 0.003
COSMETIC_RATE = 0.05


def inject_transactions(df: pd.DataFrame, rng: np.random.Generator, scale: float):
    n = len(df)
    order = rng.permutation(n)
    cursor = 0
    counts = {}

    def take(code):
        nonlocal cursor
        k = int(round(n * TXN_DEFECT_RATES[code] * scale))
        sel = order[cursor: cursor + k]
        cursor += k
        counts[code] = int(len(sel))
        return df.index[sel]

    # Cosmetic noise first (defects below may overwrite some of it - that's fine).
    cos = df.index[rng.random(n) < COSMETIC_RATE]
    df.loc[cos, "channel"] = [f" {v.lower()} " if i % 2 else v.title() for i, v in enumerate(df.loc[cos, "channel"])]
    cos = df.index[rng.random(n) < COSMETIC_RATE / 2]
    df.loc[cos, "transaction_type"] = df.loc[cos, "transaction_type"].str.lower()

    idx = take("NULL_TXN_ID");        df.loc[idx, "transaction_id"] = ""
    idx = take("NULL_ACCOUNT");       df.loc[idx, "source_account_id"] = ""
    idx = take("ORPHAN_ACCOUNT")
    df.loc[idx, "source_account_id"] = [f"ACC9{x:06d}" for x in rng.integers(0, 999_999, len(idx))]
    idx = take("NON_POSITIVE_AMOUNT")
    df.loc[idx, "amount"] = [("0" if i % 4 == 0 else f"-{v}") for i, v in enumerate(df.loc[idx, "amount"])]
    idx = take("INVALID_TYPE")
    df.loc[idx, "transaction_type"] = rng.choice(["UNKNOWN", "XFER", "CHARGEBACK"], len(idx))
    idx = take("FUTURE_TIMESTAMP")
    df.loc[idx, "transaction_timestamp"] = "2027" + df.loc[idx, "transaction_timestamp"].str[4:]
    idx = take("INVALID_TIMESTAMP")
    df.loc[idx, "transaction_timestamp"] = rng.choice(["31/13/2025 25:61", "not-a-date", "2025-02-30 10:00:00"], len(idx))
    idx = take("NULL_CHANNEL");       df.loc[idx, "channel"] = ""

    # Duplicates: copy untouched rows. Half exact copies, half near-duplicates
    # (same transaction_id, re-sent 60s later). Either way one row per pair is quarantined.
    idx = take("DUPLICATE_TXN_ID")
    dups = df.loc[idx].copy()
    half = len(dups) // 2
    near = dups.index[:half]
    dups.loc[near, "transaction_timestamp"] = (
        pd.to_datetime(dups.loc[near, "transaction_timestamp"]) + pd.Timedelta(seconds=60)
    ).dt.strftime("%Y-%m-%d %H:%M:%S")
    out = pd.concat([df, dups], ignore_index=True)
    out = out.sample(frac=1.0, random_state=int(rng.integers(0, 2**31 - 1))).reset_index(drop=True)
    return out, counts


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", type=float, default=1.0, help="Multiply all defect rates")
    ap.add_argument("--seed", type=int, default=99)
    args = ap.parse_args()
    rng = np.random.default_rng(args.seed)

    if RAW_DIR.exists():
        shutil.rmtree(RAW_DIR)
    txn_out = RAW_DIR / "transactions" / "source=batch"
    txn_out.mkdir(parents=True)

    totals = {k: 0 for k in TXN_DEFECT_RATES}
    rows_in = rows_out = 0
    files = sorted((CLEAN_DIR / "transactions").glob("*.csv.gz"))
    if not files:
        raise SystemExit("No clean transaction batches found - run prepare_transactions.py first")
    for f in files:
        df = pd.read_csv(f, dtype=str, keep_default_na=False)
        rows_in += len(df)
        out, counts = inject_transactions(df, rng, args.scale)
        rows_out += len(out)
        for k, v in counts.items():
            totals[k] += v
        out.to_csv(txn_out / f.name, index=False, compression="gzip")
        print(f"  {f.name}: {len(df):,} -> {len(out):,} rows, {sum(counts.values()):,} defects")

    # Accounts: a few invalid statuses + cosmetic lower-casing.
    acc = pd.read_csv(CLEAN_DIR / "accounts.csv", dtype=str, keep_default_na=False)
    k = int(round(len(acc) * ACCOUNT_STATUS_DEFECT_RATE * args.scale))
    bad = rng.choice(acc.index, size=k, replace=False)
    acc.loc[bad, "account_status"] = rng.choice(["ACTV", "UNKNOWN", ""], k)
    cos = acc.index[(rng.random(len(acc)) < COSMETIC_RATE) & ~acc.index.isin(bad)]
    acc.loc[cos, "account_status"] = acc.loc[cos, "account_status"].str.lower()
    for name, frame in {"accounts": acc}.items():
        (RAW_DIR / name).mkdir(parents=True)
        frame.to_csv(RAW_DIR / name / f"{name}.csv", index=False)
    for name in ["customers", "branches"]:
        (RAW_DIR / name).mkdir(parents=True)
        shutil.copy(CLEAN_DIR / f"{name}.csv", RAW_DIR / name / f"{name}.csv")

    manifest = {
        "clean_rows": rows_in,
        "raw_rows": rows_out,
        "transaction_defects": totals,
        "expected_quarantined_transactions": sum(totals.values()),
        "account_status_defects": k,
    }
    (OUTPUT_DIR / "injected_defects.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
