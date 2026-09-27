/* =====================================================================
   05_marts_and_export.sql — run as BANK_DATA_ENGINEER
   Secure, self-service marts (the "data products" analysts and Tableau use),
   a demo of governance by role, and an unload of the marts to S3 for Tableau Public.
   ===================================================================== */
USE ROLE bank_data_engineer; USE WAREHOUSE bank_wh; USE DATABASE retail_bank;

CREATE OR REPLACE SECURE VIEW analytics.mart_monthly_kpis
  COMMENT = 'Bank-level monthly KPIs. Active customer = >=1 transaction in month.' AS
SELECT DATE_TRUNC('month', f.transaction_date)                          AS month_start,
       COUNT(*)                                                         AS txn_count,
       SUM(f.amount)                                                    AS txn_value,
       AVG(f.amount)                                                    AS avg_txn_value,
       SUM(IFF(f.flow_direction = 'INFLOW', f.amount, 0))               AS deposit_value,
       SUM(IFF(f.flow_direction = 'OUTFLOW', f.amount, 0))              AS outflow_value,
       SUM(f.signed_amount)                                             AS net_flow,
       COUNT(DISTINCT f.customer_id)                                    AS active_customers,
       COUNT(DISTINCT f.source_account_id)                              AS active_accounts,
       COUNT(*) / NULLIF(COUNT(DISTINCT f.customer_id), 0)              AS txns_per_customer,
       AVG(IFF(f.is_failed, 1, 0))                                      AS failed_rate,
       AVG(IFF(f.digital_channel_flag, 1, 0))                           AS digital_share
FROM curated.fact_transactions f
JOIN curated.dim_customer c ON f.customer_id = c.customer_id   -- inner join => row access policy applies
GROUP BY 1;

CREATE OR REPLACE SECURE VIEW analytics.mart_daily_transactions
  COMMENT = 'Daily volume/value by type and channel' AS
SELECT f.transaction_date, f.transaction_type, f.flow_direction, f.channel, f.digital_channel_flag, f.ingest_source,
       COUNT(*) AS txn_count, SUM(f.amount) AS txn_value,
       COUNT_IF(f.is_failed) AS failed_txn_count, COUNT_IF(f.is_fraud_flag = 1) AS fraud_labelled_count
FROM curated.fact_transactions f
JOIN curated.dim_customer c ON f.customer_id = c.customer_id
GROUP BY 1, 2, 3, 4, 5, 6;

CREATE OR REPLACE SECURE VIEW analytics.mart_monthly_segment
  COMMENT = 'Monthly activity by segment, state, risk tier, digital enrolment' AS
SELECT DATE_TRUNC('month', f.transaction_date) AS month_start, c.customer_segment, c.state, c.risk_tier,
       c.digital_enrolled_flag, COUNT(*) AS txn_count, SUM(f.amount) AS txn_value,
       COUNT(DISTINCT f.customer_id) AS active_customers,
       AVG(IFF(f.digital_channel_flag, 1, 0)) AS digital_share
FROM curated.fact_transactions f
JOIN curated.dim_customer c ON f.customer_id = c.customer_id
GROUP BY 1, 2, 3, 4, 5;

CREATE OR REPLACE SECURE VIEW analytics.mart_customer_value
  COMMENT = 'Customer-level net flow; NET_OUTFLOW customers form an attrition-risk watchlist. customer_id is masked for analysts.' AS
SELECT f.customer_id, c.customer_segment, c.state, c.risk_tier, c.tenure_years, c.digital_enrolled_flag,
       COUNT(*) AS txn_count, SUM(f.amount) AS txn_value,
       SUM(IFF(f.flow_direction = 'INFLOW', f.amount, 0)) AS inflow_value,
       SUM(IFF(f.flow_direction = 'OUTFLOW', f.amount, 0)) AS outflow_value,
       SUM(f.signed_amount) AS net_flow,
       AVG(IFF(f.digital_channel_flag, 1, 0)) AS digital_share,
       MAX(f.transaction_date) AS last_txn_date,
       IFF(SUM(f.signed_amount) < 0, 'NET_OUTFLOW', 'NET_INFLOW') AS flow_band
FROM curated.fact_transactions f
JOIN curated.dim_customer c ON f.customer_id = c.customer_id
WHERE NOT f.is_failed
GROUP BY 1, 2, 3, 4, 5, 6;

CREATE OR REPLACE SECURE VIEW analytics.mart_merchant_monthly AS
SELECT DATE_TRUNC('month', f.transaction_date) AS month_start, f.merchant_category, f.channel,
       COUNT(*) AS txn_count, SUM(f.amount) AS txn_value
FROM curated.fact_transactions f
JOIN curated.dim_customer c ON f.customer_id = c.customer_id
WHERE f.merchant_category IS NOT NULL
GROUP BY 1, 2, 3;

CREATE OR REPLACE SECURE VIEW analytics.mart_branch_activity AS
SELECT b.branch_id, b.branch_name, b.city, b.state, b.region, f.channel,
       DATE_TRUNC('month', f.transaction_date) AS month_start, COUNT(*) AS txn_count, SUM(f.amount) AS txn_value
FROM curated.fact_transactions f
JOIN curated.dim_customer c ON f.customer_id = c.customer_id
JOIN curated.dim_branch b ON f.branch_id = b.branch_id
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- Data-quality marts (steward + Tableau DQ page)
CREATE OR REPLACE SECURE VIEW analytics.mart_dq_scorecard AS SELECT * FROM dq.v_dq_scorecard;
CREATE OR REPLACE SECURE VIEW analytics.mart_dq_run_history AS
SELECT run_id, run_ts, pipeline_status, raw_rows, curated_rows, quarantined_rows, quarantine_rate,
       max_quarantine_rate, raw_dq_score, curated_dq_score, accounts_quarantined
FROM dq.glue_run_summary;
CREATE OR REPLACE SECURE VIEW analytics.mart_dq_rule_results AS
SELECT run_id, run_ts, dataset, rule, outcome, failure_reason, IFF(outcome = 'Passed', 1, 0) AS passed_flag
FROM dq.glue_rule_outcomes
QUALIFY run_ts = MAX(run_ts) OVER (PARTITION BY dataset);
CREATE OR REPLACE SECURE VIEW analytics.mart_quarantine_reasons AS
SELECT run_id, run_ts, reason, ingest_source, row_count FROM dq.glue_quarantine_reasons;

-- ---------------------------------------------------------------------
-- Governance demo (screenshot these three result sets)
-- ---------------------------------------------------------------------
USE ROLE bank_analyst;              -- all states, identifiers hashed
SELECT customer_id, state, net_flow FROM analytics.mart_customer_value ORDER BY net_flow LIMIT 5;
SELECT COUNT(DISTINCT state) AS states_visible FROM analytics.mart_monthly_segment;

USE ROLE bank_analyst_mid_atlantic; -- only MD/VA/DC/DE/PA, identifiers hashed
SELECT COUNT(DISTINCT state) AS states_visible, LISTAGG(DISTINCT state, ',') AS states FROM analytics.mart_monthly_segment;

USE ROLE bank_dq_steward;           -- raw identifiers visible for investigation
SELECT customer_id, state, net_flow FROM analytics.mart_customer_value ORDER BY net_flow LIMIT 5;

-- ---------------------------------------------------------------------
-- Unload marts to S3 for Tableau Public (then: aws s3 cp ... tableau/data/)
-- Run as the analyst so exports carry the analyst's masking/row policies.
-- ---------------------------------------------------------------------
USE ROLE bank_data_engineer;
GRANT USAGE ON SCHEMA lake TO ROLE bank_analyst;
GRANT USAGE ON STAGE lake.lake_stage TO ROLE bank_analyst;
GRANT USAGE ON FILE FORMAT lake.csv_export_ff TO ROLE bank_analyst;
USE ROLE bank_analyst;
COPY INTO @retail_bank.lake.lake_stage/exports/mart_monthly_kpis.csv       FROM (SELECT * FROM retail_bank.analytics.mart_monthly_kpis)       FILE_FORMAT = (FORMAT_NAME = retail_bank.lake.csv_export_ff) HEADER = TRUE SINGLE = TRUE OVERWRITE = TRUE MAX_FILE_SIZE = 268435456;
COPY INTO @retail_bank.lake.lake_stage/exports/mart_daily_transactions.csv FROM (SELECT * FROM retail_bank.analytics.mart_daily_transactions) FILE_FORMAT = (FORMAT_NAME = retail_bank.lake.csv_export_ff) HEADER = TRUE SINGLE = TRUE OVERWRITE = TRUE MAX_FILE_SIZE = 268435456;
COPY INTO @retail_bank.lake.lake_stage/exports/mart_monthly_segment.csv    FROM (SELECT * FROM retail_bank.analytics.mart_monthly_segment)    FILE_FORMAT = (FORMAT_NAME = retail_bank.lake.csv_export_ff) HEADER = TRUE SINGLE = TRUE OVERWRITE = TRUE MAX_FILE_SIZE = 268435456;
COPY INTO @retail_bank.lake.lake_stage/exports/mart_customer_value.csv     FROM (SELECT * FROM retail_bank.analytics.mart_customer_value)     FILE_FORMAT = (FORMAT_NAME = retail_bank.lake.csv_export_ff) HEADER = TRUE SINGLE = TRUE OVERWRITE = TRUE MAX_FILE_SIZE = 268435456;
COPY INTO @retail_bank.lake.lake_stage/exports/mart_merchant_monthly.csv   FROM (SELECT * FROM retail_bank.analytics.mart_merchant_monthly)   FILE_FORMAT = (FORMAT_NAME = retail_bank.lake.csv_export_ff) HEADER = TRUE SINGLE = TRUE OVERWRITE = TRUE MAX_FILE_SIZE = 268435456;
COPY INTO @retail_bank.lake.lake_stage/exports/mart_branch_activity.csv    FROM (SELECT * FROM retail_bank.analytics.mart_branch_activity)    FILE_FORMAT = (FORMAT_NAME = retail_bank.lake.csv_export_ff) HEADER = TRUE SINGLE = TRUE OVERWRITE = TRUE MAX_FILE_SIZE = 268435456;
COPY INTO @retail_bank.lake.lake_stage/exports/mart_dq_scorecard.csv       FROM (SELECT * FROM retail_bank.analytics.mart_dq_scorecard)       FILE_FORMAT = (FORMAT_NAME = retail_bank.lake.csv_export_ff) HEADER = TRUE SINGLE = TRUE OVERWRITE = TRUE MAX_FILE_SIZE = 268435456;
COPY INTO @retail_bank.lake.lake_stage/exports/mart_dq_run_history.csv     FROM (SELECT * FROM retail_bank.analytics.mart_dq_run_history)     FILE_FORMAT = (FORMAT_NAME = retail_bank.lake.csv_export_ff) HEADER = TRUE SINGLE = TRUE OVERWRITE = TRUE MAX_FILE_SIZE = 268435456;
COPY INTO @retail_bank.lake.lake_stage/exports/mart_dq_rule_results.csv    FROM (SELECT * FROM retail_bank.analytics.mart_dq_rule_results)    FILE_FORMAT = (FORMAT_NAME = retail_bank.lake.csv_export_ff) HEADER = TRUE SINGLE = TRUE OVERWRITE = TRUE MAX_FILE_SIZE = 268435456;
COPY INTO @retail_bank.lake.lake_stage/exports/mart_quarantine_reasons.csv FROM (SELECT * FROM retail_bank.analytics.mart_quarantine_reasons) FILE_FORMAT = (FORMAT_NAME = retail_bank.lake.csv_export_ff) HEADER = TRUE SINGLE = TRUE OVERWRITE = TRUE MAX_FILE_SIZE = 268435456;
