# Data dictionary

Database: `retail_bank_analytics` (AWS Glue Data Catalog). All data is synthetic.

## curated.fact_transactions  — grain: one posted/attempted transaction
Parquet, partitioned by `year`, `month`. Written only when the quality gate passes.

| Column | Type | Definition |
|---|---|---|
| transaction_id | string | Unique transaction key (uniqueness enforced; latest version kept) |
| transaction_ts | timestamp | Event time (UTC) |
| transaction_date | date | Calendar date of `transaction_ts` |
| transaction_hour | int | Hour of day 0–23 |
| source_account_id | string | Originating account; must exist in account master |
| customer_id | string | Account owner (from dim_account) |
| account_type | string | CHECKING, SAVINGS, MONEY_MARKET, CD |
| counterparty_id | string | Receiving party (C… = customer, M… = merchant) |
| transaction_type | string | DEPOSIT, WITHDRAWAL, PAYMENT, TRANSFER, DEBIT |
| flow_direction | string | INFLOW (deposits) / OUTFLOW (everything else) |
| amount | decimal(18,2) | Absolute amount, always > 0 |
| signed_amount | decimal(18,2) | +amount for inflows, −amount for outflows |
| channel | string | MOBILE, ONLINE, ATM, BRANCH, POS |
| digital_channel_flag | boolean | channel in (MOBILE, ONLINE) |
| merchant_category | string | Only for PAYMENT / DEBIT |
| branch_id | string | Only for BRANCH / ATM channel |
| transaction_status | string | POSTED, FAILED, REVERSED |
| is_failed | boolean | status = FAILED |
| balance_before / balance_after | decimal(18,2) | Origin balance around the transaction (from PaySim) |
| is_fraud_flag | int | PaySim simulation fraud label (not used for modelling here) |
| ingest_source | string | BATCH or STREAM (Kinesis → Lambda path) |
| source_file | string | S3 object the row came from (row-level lineage) |
| run_id | string | Glue run that produced the row |

## curated.dim_customer — grain: customer
customer_id, customer_since (date), state, customer_segment (MASS, MASS_AFFLUENT, AFFLUENT, SMALL_BUSINESS),
risk_tier (LOW/MEDIUM/HIGH), digital_enrolled_flag (boolean), home_branch_id, tenure_years.

## curated.dim_account — grain: account
account_id, customer_id, account_type, open_date, account_status (ACTIVE, DORMANT, CLOSED, FROZEN), current_balance.
Accounts with an invalid status are routed to `quarantine_accounts` (their IDs still count as "known"
for transaction referential-integrity checks, so one bad master record doesn't cascade).

## curated.dim_branch — grain: branch
branch_id, branch_name, city, state, region.

## quarantine.quarantine_transactions
Standardised columns plus `raw_amount`, `raw_transaction_timestamp` (original strings),
`dq_fail_reasons` (comma-separated codes), `run_id`, `quarantined_at`.

| Reason code | DQ dimension | Rule |
|---|---|---|
| NULL_TXN_ID | Completeness | transaction_id present |
| NULL_ACCOUNT | Completeness | source_account_id present |
| NULL_CHANNEL | Completeness | channel present |
| ORPHAN_ACCOUNT | Referential integrity | source_account_id exists in account master |
| NON_POSITIVE_AMOUNT / INVALID_AMOUNT | Validity | amount parses and is > 0 |
| INVALID_TYPE / INVALID_CHANNEL / INVALID_STATUS | Validity | value in allowed domain |
| INVALID_TIMESTAMP | Validity | parses as `yyyy-MM-dd HH:mm:ss` |
| FUTURE_TIMESTAMP | Timeliness | not later than processing time |
| DUPLICATE_TXN_ID | Uniqueness | one row per transaction_id (latest kept) |

## Snowflake (RETAIL_BANK database)
Schemas: `CURATED` (same tables as above, with COMMENT definitions and policies), `DQ` (quarantine, Glue results, DQ views), `ANALYTICS` (secure marts), `GOVERNANCE` (tags, policies, entitlements), `LAKE` (stage), `OPS` (load procedure, task). See [business_glossary.md](business_glossary.md).

## dq-results
- `dq_rule_outcomes`: one row per Glue Data Quality (DQDL) rule per dataset per run — rule, outcome, failure_reason, evaluated_metrics.
- `dq_run_summary`: one row per run — raw/curated/quarantined rows, quarantine_rate, threshold, raw & curated DQ scores, pipeline_status (PUBLISHED / BLOCKED_BY_QUALITY_GATE).
- `dq_quarantine_reasons`: quarantined row counts by reason × ingest_source × run.

**DQ score** = share of DQDL rules passed (Glue Data Quality definition).
**Quarantine rate** = quarantined rows ÷ raw rows; gate threshold = 5 % (`--MAX_QUARANTINE_RATE`).
