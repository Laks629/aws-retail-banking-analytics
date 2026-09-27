-- Reusable Athena views ("marts") that feed Tableau.
-- Run in the Athena console (database: retail_bank_analytics, workgroup: retail-bank-wg)
-- one statement at a time, or let tableau/export_marts.py run them all.
-- Each view is pre-aggregated so the Tableau extracts stay small.

-- 1. Daily transaction volume/value by type and channel (core trend mart)
CREATE OR REPLACE VIEW mart_daily_transactions AS
SELECT transaction_date,
       transaction_type,
       flow_direction,
       channel,
       digital_channel_flag,
       ingest_source,
       COUNT(*)                                          AS txn_count,
       SUM(amount)                                       AS txn_value,
       SUM(CASE WHEN is_failed THEN 1 ELSE 0 END)        AS failed_txn_count,
       SUM(CASE WHEN is_fraud_flag = 1 THEN 1 ELSE 0 END) AS fraud_labelled_count
FROM fact_transactions
GROUP BY 1, 2, 3, 4, 5, 6;

-- 2. Monthly bank-level KPIs (distinct counts computed here, not in Tableau)
CREATE OR REPLACE VIEW mart_monthly_kpis AS
SELECT date_trunc('month', transaction_date)                                     AS month_start,
       COUNT(*)                                                                  AS txn_count,
       SUM(amount)                                                               AS txn_value,
       AVG(amount)                                                               AS avg_txn_value,
       SUM(CASE WHEN flow_direction = 'INFLOW'  THEN amount ELSE 0 END)          AS deposit_value,
       SUM(CASE WHEN flow_direction = 'OUTFLOW' THEN amount ELSE 0 END)          AS outflow_value,
       SUM(signed_amount)                                                        AS net_flow,
       COUNT(DISTINCT customer_id)                                               AS active_customers,
       COUNT(DISTINCT source_account_id)                                         AS active_accounts,
       CAST(COUNT(*) AS double) / NULLIF(COUNT(DISTINCT customer_id), 0)         AS txns_per_customer,
       AVG(CASE WHEN is_failed THEN 1.0 ELSE 0.0 END)                            AS failed_rate,
       AVG(CASE WHEN digital_channel_flag THEN 1.0 ELSE 0.0 END)                 AS digital_share
FROM fact_transactions
GROUP BY 1;

-- 3. Monthly activity by customer segment / state / risk tier
CREATE OR REPLACE VIEW mart_monthly_segment AS
SELECT date_trunc('month', t.transaction_date) AS month_start,
       c.customer_segment,
       c.state,
       c.risk_tier,
       c.digital_enrolled_flag,
       COUNT(*)                      AS txn_count,
       SUM(t.amount)                 AS txn_value,
       COUNT(DISTINCT t.customer_id) AS active_customers
FROM fact_transactions t
LEFT JOIN dim_customer c ON t.customer_id = c.customer_id
GROUP BY 1, 2, 3, 4, 5;

-- 4. Customer-level value & net flow (identifies net-outflow / attrition-risk customers)
CREATE OR REPLACE VIEW mart_customer_value AS
SELECT t.customer_id,
       c.customer_segment,
       c.state,
       c.risk_tier,
       c.tenure_years,
       c.digital_enrolled_flag,
       COUNT(*)                                                         AS txn_count,
       SUM(t.amount)                                                    AS txn_value,
       SUM(CASE WHEN t.flow_direction = 'INFLOW' THEN t.amount ELSE 0 END)  AS inflow_value,
       SUM(CASE WHEN t.flow_direction = 'OUTFLOW' THEN t.amount ELSE 0 END) AS outflow_value,
       SUM(t.signed_amount)                                             AS net_flow,
       AVG(CASE WHEN t.digital_channel_flag THEN 1.0 ELSE 0.0 END)      AS digital_share,
       MAX(t.transaction_date)                                          AS last_txn_date,
       CASE WHEN SUM(t.signed_amount) < 0 THEN 'NET_OUTFLOW' ELSE 'NET_INFLOW' END AS flow_band
FROM fact_transactions t
LEFT JOIN dim_customer c ON t.customer_id = c.customer_id
WHERE NOT t.is_failed
GROUP BY 1, 2, 3, 4, 5, 6;

-- 5. Merchant category spend by month (card / bill-pay behaviour)
CREATE OR REPLACE VIEW mart_merchant_monthly AS
SELECT date_trunc('month', transaction_date) AS month_start,
       merchant_category,
       channel,
       COUNT(*)    AS txn_count,
       SUM(amount) AS txn_value
FROM fact_transactions
WHERE merchant_category IS NOT NULL
GROUP BY 1, 2, 3;

-- 6. Branch & ATM network activity
CREATE OR REPLACE VIEW mart_branch_activity AS
SELECT b.branch_id, b.branch_name, b.city, b.state, b.region,
       t.channel,
       date_trunc('month', t.transaction_date) AS month_start,
       COUNT(*)      AS txn_count,
       SUM(t.amount) AS txn_value
FROM fact_transactions t
JOIN dim_branch b ON t.branch_id = b.branch_id
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- 7. Account book: openings by year and current status/balance
CREATE OR REPLACE VIEW mart_account_book AS
SELECT date_trunc('year', open_date) AS open_year,
       account_type,
       account_status,
       COUNT(*)             AS accounts,
       SUM(current_balance) AS total_balance,
       AVG(current_balance) AS avg_balance
FROM dim_account
GROUP BY 1, 2, 3;

-- 8. Glue Data Quality rule outcomes (latest run per dataset)
CREATE OR REPLACE VIEW mart_dq_rule_results AS
SELECT r.run_id, r.run_ts, r.dataset, r.rule, r.outcome, r.failure_reason,
       CASE WHEN r.outcome = 'Passed' THEN 1 ELSE 0 END AS passed_flag
FROM dq_rule_outcomes r
JOIN (SELECT dataset, MAX(run_ts) AS run_ts FROM dq_rule_outcomes GROUP BY dataset) l
  ON r.dataset = l.dataset AND r.run_ts = l.run_ts;

-- 9. Pipeline run history (DQ score + quarantine-rate trend)
CREATE OR REPLACE VIEW mart_dq_run_history AS
SELECT run_id, run_ts, pipeline_status, raw_rows, curated_rows, quarantined_rows,
       quarantine_rate, max_quarantine_rate, raw_dq_score, curated_dq_score, accounts_quarantined
FROM dq_run_summary;

-- 10. Quarantine reasons by run and ingestion path (batch vs stream)
CREATE OR REPLACE VIEW mart_quarantine_reasons AS
SELECT run_id, run_ts, reason, ingest_source, row_count
FROM dq_quarantine_reasons;
