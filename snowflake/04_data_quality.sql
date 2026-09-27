/* =====================================================================
   04_data_quality.sql — run as BANK_DATA_ENGINEER
   Second line of DQ defence *inside the warehouse* using Snowflake Data Metric
   Functions (DMFs, Enterprise edition), plus a unified DQ scorecard combining
   Glue pipeline results and Snowflake checks.
   ===================================================================== */
USE ROLE bank_data_engineer; USE WAREHOUSE bank_wh; USE DATABASE retail_bank;

-- Custom DMFs for bank-specific rules
CREATE OR REPLACE DATA METRIC FUNCTION governance.non_positive_amount_count(arg_t TABLE(arg_c1 NUMBER(18,2)))
  RETURNS NUMBER AS 'SELECT COUNT_IF(arg_c1 <= 0) FROM arg_t';
CREATE OR REPLACE DATA METRIC FUNCTION governance.future_timestamp_count(arg_t TABLE(arg_c1 TIMESTAMP_NTZ))
  RETURNS NUMBER AS 'SELECT COUNT_IF(arg_c1 > CURRENT_TIMESTAMP()::TIMESTAMP_NTZ) FROM arg_t';
CREATE OR REPLACE DATA METRIC FUNCTION governance.invalid_channel_count(arg_t TABLE(arg_c1 STRING))
  RETURNS NUMBER AS $$SELECT COUNT_IF(arg_c1 IS NULL OR arg_c1 NOT IN ('MOBILE','ONLINE','ATM','BRANCH','POS')) FROM arg_t$$;

-- Evaluate whenever the table changes (i.e. after each refresh_from_lake)
ALTER TABLE curated.fact_transactions SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
ALTER TABLE curated.fact_transactions ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();
ALTER TABLE curated.fact_transactions ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (transaction_id);
ALTER TABLE curated.fact_transactions ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (source_account_id);
ALTER TABLE curated.fact_transactions ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (customer_id);
ALTER TABLE curated.fact_transactions ADD DATA METRIC FUNCTION governance.non_positive_amount_count ON (amount);
ALTER TABLE curated.fact_transactions ADD DATA METRIC FUNCTION governance.future_timestamp_count ON (transaction_ts);
ALTER TABLE curated.fact_transactions ADD DATA METRIC FUNCTION governance.invalid_channel_count ON (channel);

-- Run once on demand to verify (doesn't wait for the schedule):
SELECT SNOWFLAKE.CORE.DUPLICATE_COUNT(SELECT transaction_id FROM curated.fact_transactions) AS dup_txn_ids,
       governance.non_positive_amount_count(SELECT amount FROM curated.fact_transactions) AS non_positive_amounts;

-- DMF results land here a few minutes after a change (re-run CALL ops.refresh_from_lake() to trigger):
-- SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS ORDER BY measurement_time DESC;

-- Referential integrity checks the DMFs can't express simply
CREATE OR REPLACE VIEW dq.v_referential_integrity AS
SELECT 'fact.source_account_id -> dim_account' AS check_name,
       COUNT_IF(a.account_id IS NULL) AS failing_rows, COUNT(*) AS evaluated_rows
FROM curated.fact_transactions f LEFT JOIN curated.dim_account a ON f.source_account_id = a.account_id
UNION ALL
SELECT 'dim_account.customer_id -> dim_customer', COUNT_IF(c.customer_id IS NULL), COUNT(*)
FROM curated.dim_account a LEFT JOIN curated.dim_customer c ON a.customer_id = c.customer_id;

-- Reconciliation: Glue run summary vs rows actually loaded into Snowflake
CREATE OR REPLACE VIEW dq.v_load_reconciliation AS
WITH g AS (SELECT * FROM dq.glue_run_summary QUALIFY ROW_NUMBER() OVER (ORDER BY run_ts DESC) = 1)
SELECT g.run_id, g.raw_rows, g.curated_rows AS glue_curated_rows,
       (SELECT COUNT(*) FROM curated.fact_transactions) AS snowflake_fact_rows,
       g.quarantined_rows AS glue_quarantined_rows,
       (SELECT COUNT(*) FROM dq.quarantine_transactions) AS snowflake_quarantine_rows,
       g.raw_rows - (SELECT COUNT(*) FROM curated.fact_transactions)
                  - (SELECT COUNT(*) FROM dq.quarantine_transactions) AS unexplained_diff
FROM g;

-- Unified DQ scorecard by dimension (feeds the Tableau data-quality page)
CREATE OR REPLACE VIEW dq.v_dq_scorecard AS
WITH latest AS (SELECT run_id, raw_rows FROM dq.glue_run_summary QUALIFY ROW_NUMBER() OVER (ORDER BY run_ts DESC) = 1),
reasons AS (
  SELECT r.reason, SUM(r.row_count) AS failing_rows
  FROM dq.glue_quarantine_reasons r JOIN latest l ON r.run_id = l.run_id GROUP BY 1),
dims AS (
  SELECT column1 AS reason, column2 AS dq_dimension, column3 AS business_rule FROM VALUES
    ('NULL_TXN_ID', 'Completeness', 'Every transaction has an ID'),
    ('NULL_ACCOUNT', 'Completeness', 'Every transaction has a source account'),
    ('NULL_CHANNEL', 'Completeness', 'Every transaction has a channel'),
    ('ORPHAN_ACCOUNT', 'Referential integrity', 'Source account exists in account master'),
    ('NON_POSITIVE_AMOUNT', 'Validity', 'Amount is greater than zero'),
    ('INVALID_AMOUNT', 'Validity', 'Amount is numeric'),
    ('INVALID_TYPE', 'Validity', 'Type is an approved transaction type'),
    ('INVALID_CHANNEL', 'Validity', 'Channel is an approved channel'),
    ('INVALID_STATUS', 'Validity', 'Status is POSTED, FAILED or REVERSED'),
    ('INVALID_TIMESTAMP', 'Validity', 'Timestamp is parseable'),
    ('FUTURE_TIMESTAMP', 'Timeliness', 'Timestamp is not in the future'),
    ('DUPLICATE_TXN_ID', 'Uniqueness', 'One record per transaction ID'))
SELECT d.dq_dimension, d.reason AS rule_code, d.business_rule,
       COALESCE(r.failing_rows, 0) AS failing_rows, l.raw_rows AS evaluated_rows,
       ROUND(100 * (1 - COALESCE(r.failing_rows, 0) / NULLIF(l.raw_rows, 0)), 4) AS pass_rate_pct,
       l.run_id
FROM dims d CROSS JOIN latest l LEFT JOIN reasons r ON d.reason = r.reason;

SELECT * FROM dq.v_dq_scorecard ORDER BY dq_dimension, rule_code;
SELECT * FROM dq.v_load_reconciliation;
SELECT * FROM dq.v_referential_integrity;
