# Data

Nothing in this folder is committed except this README (see `.gitignore`).

| Path | Created by | Contents |
|---|---|---|
| `data/source/` | you (Kaggle download) | PaySim CSV `PS_20174392719_1491204439457_log.csv` (~6.36M rows, ~470 MB) |
| `data/output/clean/` | `generate_dimensions.py`, `prepare_transactions.py` | defect-free dims + transaction batches |
| `data/output/raw/` | `inject_dq_issues.py` | files uploaded to `s3://<bucket>/retail-bank/raw/` |
| `data/output/stream/` | `prepare_transactions.py` | held-out events for the Kinesis producer |
| `data/output/injected_defects.json` | `inject_dq_issues.py` | ground truth for reconciling quarantine counts |

**Source:** PaySim synthetic mobile-money simulation (Kaggle `ealaxi/paysim1`; see the Kaggle page for license terms).
All customers, accounts, branches, channels and statuses are synthetic. This project does not
represent any real bank or real customers.
