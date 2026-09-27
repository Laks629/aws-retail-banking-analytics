#!/usr/bin/env python3
"""Create a small PaySim-shaped CSV for dry runs and CI (no Kaggle download needed).

Usage:
    python data_generator/make_sample_paysim.py --rows 200000 --out data/source/paysim_sample.csv
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

TYPES = (["CASH_IN", "CASH_OUT", "PAYMENT", "TRANSFER", "DEBIT"], [0.22, 0.35, 0.34, 0.08, 0.01])


def make(rows: int, seed: int = 1) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    step = np.sort(rng.integers(1, 744, rows))
    ttype = rng.choice(TYPES[0], rows, p=TYPES[1])
    amount = np.round(rng.lognormal(10.5, 1.3, rows), 2)
    old = np.round(np.where(rng.random(rows) < 0.3, 0, rng.lognormal(10, 1.5, rows)), 2)
    new = np.where(ttype == "CASH_IN", old + amount, np.clip(old - amount, 0, None)).round(2)
    return pd.DataFrame({
        "step": step, "type": ttype, "amount": amount,
        "nameOrig": [f"C{x}" for x in rng.integers(1e8, 2e9, rows)],
        "oldbalanceOrg": old, "newbalanceOrig": new,
        "nameDest": [f"{'M' if t == 'PAYMENT' else 'C'}{x}" for t, x in zip(ttype, rng.integers(1e8, 2e9, rows))],
        "oldbalanceDest": 0.0, "newbalanceDest": 0.0,
        "isFraud": (rng.random(rows) < 0.0013).astype(int), "isFlaggedFraud": 0,
    })


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=200_000)
    ap.add_argument("--out", default="data/source/paysim_sample.csv")
    a = ap.parse_args()
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    make(a.rows).to_csv(a.out, index=False)
    print(f"Wrote {a.rows:,} rows -> {a.out}")
