# Business glossary & lineage

Definitions are also stored as `COMMENT`s on Snowflake tables/columns/views, so they show up in
Snowsight and `INFORMATION_SCHEMA.COLUMNS` next to the data.

## KPI definitions
| Term | Definition | Source / logic | Owner |
|---|---|---|---|
| Transaction value | Sum of absolute `amount` for all transactions in the period (all statuses unless stated) | `fact_transactions.amount` | Payments & Deposits Analytics |
| Deposit value | Sum of `amount` where `flow_direction = 'INFLOW'` (transaction_type DEPOSIT) | `mart_monthly_kpis.deposit_value` | Payments & Deposits Analytics |
| Net flow | Deposits minus outflows = `SUM(signed_amount)` | `mart_monthly_kpis.net_flow` | Payments & Deposits Analytics |
| Active customer | Customer with ≥ 1 transaction (any status) in the month | `COUNT(DISTINCT customer_id)` per month | Customer Data Office |
| Digital share | Share of transactions via MOBILE or ONLINE | `AVG(digital_channel_flag)` | Digital Banking |
| Failed rate | FAILED transactions ÷ all transactions | `AVG(is_failed)` | Bank Operations |
| Net-outflow customer | Customer whose posted transactions sum to a negative net flow over the data window | `mart_customer_value.flow_band` | Customer Data Office |
| Quarantine rate | Rows rejected by row-level DQ rules ÷ raw rows in the run | `glue_run_summary.quarantine_rate` | Data Quality (steward) |
| DQ score | Share of Glue Data Quality DQDL rules passed for a dataset in a run | `glue_run_summary.raw_dq_score / curated_dq_score` | Data Quality (steward) |
| Pass rate (per rule) | 1 − failing rows ÷ evaluated rows | `dq.v_dq_scorecard.pass_rate_pct` | Data Quality (steward) |

## Data quality dimensions → rules
| Dimension | Enforced where | Examples |
|---|---|---|
| Completeness | Glue row rules, Glue DQ `IsComplete`, Snowflake `NULL_COUNT` DMF | transaction_id, source_account_id, channel present |
| Uniqueness | Glue dedupe window, Glue DQ `IsUnique`, Snowflake `DUPLICATE_COUNT` DMF | one row per transaction_id |
| Validity | Glue row rules, Glue DQ `ColumnValues`, custom DMFs | amount > 0; approved type/channel/status domains |
| Referential integrity | Glue anti-join vs account master; `dq.v_referential_integrity` | account exists; account owner exists |
| Timeliness | Glue future-timestamp rule; `future_timestamp_count` DMF | no events after processing time |
| Reconciliation | `dq.v_load_reconciliation`, Athena Q20 | raw = curated + quarantined, S3 = Snowflake |

## Lineage (column-level highlights)
```
PaySim CSV ──prepare_transactions.py──▶ s3 raw/transactions (CSV.gz)
   step            → transaction_timestamp (step × 12h from 2025-07-01)
   type            → transaction_type (CASH_IN→DEPOSIT, CASH_OUT→WITHDRAWAL, …)
   nameOrig        → source_account_id (stable hash into account master)
   oldbalanceOrg   → balance_before
raw ──Glue retail_bank_etl.py──▶ curated/fact_transactions (Parquet) | quarantine/ (with reasons)
   trim/upper codes, cast decimals/timestamps, dedupe, + customer_id, account_type (dim_account join),
   + flow_direction, signed_amount, digital_channel_flag, is_failed, source_file, run_id
curated ──Snowflake COPY INTO (ops.refresh_from_lake)──▶ RETAIL_BANK.CURATED.*
   ──secure views──▶ RETAIL_BANK.ANALYTICS.mart_* ──unload──▶ Tableau
```
Every curated row keeps `source_file` and `run_id`, so any dashboard number can be traced back to the
S3 object and pipeline run that produced it. Snowsight's lineage view and `SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY`
show downstream object and access lineage inside Snowflake.

## Access model
| Role | Sees | Identifiers | Rows |
|---|---|---|---|
| BANK_DATA_ENGINEER | everything | raw | all |
| BANK_DQ_STEWARD | analytics + dq/quarantine | raw | all |
| BANK_ANALYST | analytics marts | hashed (`H_…`), balances banded to $1,000 | all |
| BANK_ANALYST_MID_ATLANTIC | analytics marts | hashed | MD, VA, DC, DE, PA only |
