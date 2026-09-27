/* =====================================================================
   02_tables_and_load.sql — run as BANK_DATA_ENGINEER
   Tables carry business definitions as COMMENTs (metadata lives with the data).
   ops.refresh_from_lake() does an idempotent full refresh from S3 (TRUNCATE + COPY),
   matching the Glue job's full-reprocess design.
   ===================================================================== */
USE ROLE bank_data_engineer; USE WAREHOUSE bank_wh; USE DATABASE retail_bank;

CREATE TABLE IF NOT EXISTS curated.fact_transactions (
  transaction_id       STRING         COMMENT 'Unique transaction key; uniqueness enforced upstream (latest version kept)',
  transaction_ts       TIMESTAMP_NTZ  COMMENT 'Event time in UTC',
  transaction_date     DATE           COMMENT 'Calendar date of the transaction (UTC)',
  transaction_hour     NUMBER(2,0)    COMMENT 'Hour of day 0-23',
  source_account_id    STRING         COMMENT 'Originating account; must exist in the account master',
  customer_id          STRING         COMMENT 'Owner of the source account',
  account_type         STRING         COMMENT 'CHECKING, SAVINGS, MONEY_MARKET, CD',
  counterparty_id      STRING         COMMENT 'Receiving party: C* = customer, M* = merchant',
  transaction_type     STRING         COMMENT 'DEPOSIT, WITHDRAWAL, PAYMENT, TRANSFER, DEBIT',
  flow_direction       STRING         COMMENT 'INFLOW = deposits; OUTFLOW = all other types',
  amount               NUMBER(18,2)   COMMENT 'Absolute transaction amount in USD; always > 0',
  signed_amount        NUMBER(18,2)   COMMENT '+amount for inflows, -amount for outflows; sums to net flow',
  channel              STRING         COMMENT 'MOBILE, ONLINE, ATM, BRANCH, POS',
  digital_channel_flag BOOLEAN        COMMENT 'TRUE when channel is MOBILE or ONLINE',
  merchant_category    STRING         COMMENT 'Merchant category for PAYMENT and DEBIT only',
  branch_id            STRING         COMMENT 'Branch for BRANCH/ATM channel transactions',
  transaction_status   STRING         COMMENT 'POSTED, FAILED, REVERSED',
  is_failed            BOOLEAN        COMMENT 'TRUE when status = FAILED',
  balance_before       NUMBER(18,2)   COMMENT 'Origin balance before the transaction (simulation value)',
  balance_after        NUMBER(18,2)   COMMENT 'Origin balance after the transaction (simulation value)',
  is_fraud_flag        NUMBER(1,0)    COMMENT 'Simulation fraud label from PaySim',
  ingest_source        STRING         COMMENT 'BATCH or STREAM ingestion path',
  source_file          STRING         COMMENT 'S3 object the row was read from (row-level lineage)',
  run_id               STRING         COMMENT 'Glue job run that produced the row'
) COMMENT = 'Grain: one transaction. Source: s3 curated/fact_transactions (Glue PySpark). Synthetic data.';

CREATE TABLE IF NOT EXISTS curated.dim_customer (
  customer_id STRING COMMENT 'Customer key', customer_since DATE COMMENT 'Relationship start date',
  state STRING COMMENT 'US state of residence', customer_segment STRING COMMENT 'MASS, MASS_AFFLUENT, AFFLUENT, SMALL_BUSINESS',
  risk_tier STRING COMMENT 'LOW, MEDIUM, HIGH', digital_enrolled_flag BOOLEAN COMMENT 'Enrolled in online/mobile banking',
  home_branch_id STRING COMMENT 'Primary branch', tenure_years NUMBER(5,1) COMMENT 'Years since customer_since'
) COMMENT = 'Grain: one customer. Synthetic.';

CREATE TABLE IF NOT EXISTS curated.dim_account (
  account_id STRING COMMENT 'Account key', customer_id STRING COMMENT 'Owning customer',
  account_type STRING COMMENT 'CHECKING, SAVINGS, MONEY_MARKET, CD', open_date DATE COMMENT 'Account open date',
  account_status STRING COMMENT 'ACTIVE, DORMANT, CLOSED, FROZEN', current_balance NUMBER(18,2) COMMENT 'Ledger balance in USD'
) COMMENT = 'Grain: one account. Invalid-status records are quarantined upstream.';

CREATE TABLE IF NOT EXISTS curated.dim_branch (
  branch_id STRING, branch_name STRING, city STRING, state STRING, region STRING
) COMMENT = 'Grain: one branch.';

CREATE TABLE IF NOT EXISTS dq.quarantine_transactions (
  transaction_id STRING, transaction_ts TIMESTAMP_NTZ, source_account_id STRING, counterparty_id STRING,
  transaction_type STRING, amount NUMBER(18,2), channel STRING, merchant_category STRING, branch_id STRING,
  transaction_status STRING, balance_before NUMBER(18,2), balance_after NUMBER(18,2), is_fraud_flag NUMBER(1,0),
  ingest_source STRING, source_file STRING, raw_transaction_timestamp STRING, raw_amount STRING,
  dq_fail_reasons STRING COMMENT 'Comma-separated DQ reason codes', run_id STRING, quarantined_at TIMESTAMP_NTZ
) COMMENT = 'Rows rejected by Glue row-level DQ rules, with reasons';

CREATE TABLE IF NOT EXISTS dq.glue_rule_outcomes (
  run_id STRING, run_ts TIMESTAMP_NTZ, dataset STRING, rule STRING, outcome STRING,
  failure_reason STRING, evaluated_metrics STRING
) COMMENT = 'AWS Glue Data Quality (DQDL) rule results per run';

CREATE TABLE IF NOT EXISTS dq.glue_run_summary (
  run_id STRING, run_ts TIMESTAMP_NTZ, raw_rows NUMBER, curated_rows NUMBER, quarantined_rows NUMBER,
  quarantine_rate FLOAT, max_quarantine_rate FLOAT, raw_dq_score FLOAT, curated_dq_score FLOAT,
  accounts_quarantined NUMBER, pipeline_status STRING
) COMMENT = 'One row per Glue pipeline run';

CREATE TABLE IF NOT EXISTS dq.glue_quarantine_reasons (
  reason STRING, ingest_source STRING, row_count NUMBER, run_id STRING, run_ts TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS dq.load_audit (
  loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), table_name STRING, rows_loaded NUMBER
) COMMENT = 'Row counts per Snowflake load (reconciliation against Glue run summary)';

CREATE OR REPLACE PROCEDURE ops.refresh_from_lake()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  TRUNCATE TABLE curated.fact_transactions;
  COPY INTO curated.fact_transactions FROM @lake.lake_stage/curated/fact_transactions/
    PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;
  TRUNCATE TABLE curated.dim_customer;
  COPY INTO curated.dim_customer FROM @lake.lake_stage/curated/dim_customer/
    PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;
  TRUNCATE TABLE curated.dim_account;
  COPY INTO curated.dim_account FROM @lake.lake_stage/curated/dim_account/
    PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;
  TRUNCATE TABLE curated.dim_branch;
  COPY INTO curated.dim_branch FROM @lake.lake_stage/curated/dim_branch/
    PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;
  TRUNCATE TABLE dq.quarantine_transactions;
  COPY INTO dq.quarantine_transactions FROM @lake.lake_stage/quarantine/quarantine_transactions/
    PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;
  TRUNCATE TABLE dq.glue_rule_outcomes;
  COPY INTO dq.glue_rule_outcomes FROM @lake.lake_stage/dq-results/dq_rule_outcomes/
    PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;
  TRUNCATE TABLE dq.glue_run_summary;
  COPY INTO dq.glue_run_summary FROM @lake.lake_stage/dq-results/dq_run_summary/
    PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;
  TRUNCATE TABLE dq.glue_quarantine_reasons;
  COPY INTO dq.glue_quarantine_reasons FROM @lake.lake_stage/dq-results/dq_quarantine_reasons/
    PATTERN = '.*[.]parquet' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE FORCE = TRUE;

  INSERT INTO dq.load_audit (table_name, rows_loaded)
    SELECT 'curated.fact_transactions', COUNT(*) FROM curated.fact_transactions
    UNION ALL SELECT 'curated.dim_customer', COUNT(*) FROM curated.dim_customer
    UNION ALL SELECT 'curated.dim_account', COUNT(*) FROM curated.dim_account
    UNION ALL SELECT 'dq.quarantine_transactions', COUNT(*) FROM dq.quarantine_transactions;
  RETURN 'Refreshed from s3 lake at ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;

CALL ops.refresh_from_lake();
SELECT * FROM dq.load_audit ORDER BY loaded_at DESC LIMIT 4;

-- Optional daily schedule (serverless-friendly XS warehouse). Resume only while demoing.
CREATE OR REPLACE TASK ops.daily_refresh
  WAREHOUSE = bank_wh
  SCHEDULE = 'USING CRON 0 7 * * * UTC'
  COMMENT = 'Daily refresh after the Glue pipeline'
AS CALL ops.refresh_from_lake();
-- ALTER TASK ops.daily_refresh RESUME;
