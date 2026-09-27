#!/usr/bin/env python3
"""Generate synthetic customer, account and branch dimension tables.

Outputs (defect-free) to data/output/clean/:
    customers.csv, accounts.csv, branches.csv

Usage:
    python data_generator/generate_dimensions.py
    python data_generator/generate_dimensions.py --customers 2000 --accounts 3000   # quick dev run
"""
import argparse

import numpy as np
import pandas as pd

from common import CLEAN_DIR, N_ACCOUNTS, N_BRANCHES, N_CUSTOMERS, account_id

CITY_BY_STATE = {
    "MD": ["Baltimore", "Hyattsville"], "VA": ["Arlington", "Richmond"],
    "DC": ["Washington", "Georgetown"], "PA": ["Philadelphia", "Pittsburgh"],
    "NY": ["New York", "Buffalo"], "NJ": ["Newark", "Jersey City"],
    "DE": ["Wilmington", "Dover"], "NC": ["Charlotte", "Raleigh"],
    "GA": ["Atlanta", "Savannah"], "FL": ["Miami", "Tampa"],
    "TX": ["Dallas", "Houston"], "CA": ["San Francisco", "Los Angeles"],
    "IL": ["Chicago", "Naperville"], "OH": ["Columbus", "Cleveland"],
    "MA": ["Boston", "Cambridge"], "WA": ["Seattle", "Spokane"],
    "AZ": ["Phoenix", "Tucson"], "CO": ["Denver", "Boulder"],
    "MN": ["Minneapolis", "St. Paul"], "TN": ["Nashville", "Memphis"],
}
REGION_BY_STATE = {
    "MD": "MID_ATLANTIC", "VA": "MID_ATLANTIC", "DC": "MID_ATLANTIC", "PA": "MID_ATLANTIC",
    "NY": "NORTHEAST", "NJ": "NORTHEAST", "DE": "MID_ATLANTIC", "MA": "NORTHEAST",
    "NC": "SOUTHEAST", "GA": "SOUTHEAST", "FL": "SOUTHEAST", "TN": "SOUTHEAST",
    "TX": "SOUTHWEST", "AZ": "SOUTHWEST", "CA": "WEST", "WA": "WEST", "CO": "WEST",
    "IL": "MIDWEST", "OH": "MIDWEST", "MN": "MIDWEST",
}
STATES = list(CITY_BY_STATE)
# Skew customers toward the DC/MD/VA metro to make geography charts interesting.
STATE_WEIGHTS = np.array([12, 11, 8, 6, 7, 5, 2, 5, 5, 6, 7, 6, 4, 3, 4, 2, 2, 2, 2, 1], dtype=float)
STATE_WEIGHTS /= STATE_WEIGHTS.sum()

SEGMENTS = (["MASS", "MASS_AFFLUENT", "AFFLUENT", "SMALL_BUSINESS"], [0.55, 0.25, 0.10, 0.10])
RISK_TIERS = (["LOW", "MEDIUM", "HIGH"], [0.75, 0.20, 0.05])
ACCOUNT_TYPES = (["CHECKING", "SAVINGS", "MONEY_MARKET", "CD"], [0.55, 0.30, 0.10, 0.05])
ACCOUNT_STATUS = (["ACTIVE", "DORMANT", "CLOSED", "FROZEN"], [0.90, 0.06, 0.035, 0.005])
BALANCE_MU = {"CHECKING": 8.0, "SAVINGS": 9.0, "MONEY_MARKET": 10.2, "CD": 10.0}

HISTORY_START = pd.Timestamp("2005-01-01")
DIM_CUTOFF = pd.Timestamp("2025-06-30")  # transactions start 2025-07-01


def random_dates(rng, start, end, n):
    span = (end - start).days
    return start + pd.to_timedelta(rng.integers(0, max(span, 1), n), unit="D")


def build_branches(rng, n):
    rows = []
    for i in range(n):
        state = STATES[i % len(STATES)]
        city = CITY_BY_STATE[state][(i // len(STATES)) % 2]
        rows.append({
            "branch_id": f"BR{i + 1:03d}",
            "branch_name": f"{city} {'Main' if i < len(STATES) else 'Plaza'} Branch",
            "city": city,
            "state": state,
            "region": REGION_BY_STATE[state],
        })
    return pd.DataFrame(rows)


def build_customers(rng, n, branches):
    state = rng.choice(STATES, size=n, p=STATE_WEIGHTS)
    segment = rng.choice(SEGMENTS[0], size=n, p=SEGMENTS[1])
    since = random_dates(rng, HISTORY_START, DIM_CUTOFF, n)
    # Newer customers are more likely to be digitally enrolled.
    tenure_years = (DIM_CUTOFF - since).days / 365.25
    p_digital = np.clip(0.9 - tenure_years * 0.02, 0.45, 0.9)
    digital = np.where(rng.random(n) < p_digital, "Y", "N")
    branch_by_state = branches.groupby("state")["branch_id"].apply(list).to_dict()
    home_branch = [rng.choice(branch_by_state[s]) for s in state]
    return pd.DataFrame({
        "customer_id": [f"CUST{i:06d}" for i in range(1, n + 1)],
        "customer_since": since.strftime("%Y-%m-%d"),
        "state": state,
        "customer_segment": segment,
        "risk_tier": rng.choice(RISK_TIERS[0], size=n, p=RISK_TIERS[1]),
        "digital_enrolled_flag": digital,
        "home_branch_id": home_branch,
    })


def build_accounts(rng, n, customers):
    n_cust = len(customers)
    if n < n_cust:
        raise ValueError("Need at least one account per customer")
    # Every customer gets one account; the rest are distributed randomly.
    owner_idx = np.concatenate([np.arange(n_cust), rng.integers(0, n_cust, n - n_cust)])
    owners = customers.iloc[owner_idx].reset_index(drop=True)
    acct_type = rng.choice(ACCOUNT_TYPES[0], size=n, p=ACCOUNT_TYPES[1])
    since = pd.to_datetime(owners["customer_since"])
    span_days = (DIM_CUTOFF - since).dt.days.clip(lower=1).to_numpy()
    open_date = since + pd.to_timedelta((rng.random(n) * span_days).astype(int), unit="D")
    mu = np.array([BALANCE_MU[t] for t in acct_type])
    balance = np.round(rng.lognormal(mean=mu, sigma=1.0), 2)
    return pd.DataFrame({
        "account_id": [account_id(i) for i in range(1, n + 1)],
        "customer_id": owners["customer_id"].to_numpy(),
        "account_type": acct_type,
        "open_date": open_date.dt.strftime("%Y-%m-%d"),
        "account_status": rng.choice(ACCOUNT_STATUS[0], size=n, p=ACCOUNT_STATUS[1]),
        "current_balance": balance,
    })


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--customers", type=int, default=N_CUSTOMERS)
    ap.add_argument("--accounts", type=int, default=N_ACCOUNTS)
    ap.add_argument("--branches", type=int, default=N_BRANCHES)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    branches = build_branches(rng, args.branches)
    customers = build_customers(rng, args.customers, branches)
    accounts = build_accounts(rng, args.accounts, customers)

    branches.to_csv(CLEAN_DIR / "branches.csv", index=False)
    customers.to_csv(CLEAN_DIR / "customers.csv", index=False)
    accounts.to_csv(CLEAN_DIR / "accounts.csv", index=False)
    print(f"Wrote {len(customers):,} customers, {len(accounts):,} accounts, "
          f"{len(branches):,} branches to {CLEAN_DIR}")


if __name__ == "__main__":
    main()
