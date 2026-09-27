-- Business KPI queries (Athena / Trino SQL). Database: retail_bank_analytics

-- Q1. Total transaction value and count by month
SELECT date_trunc('month', transaction_date) AS month_start,
       COUNT(*) AS txn_count, SUM(amount) AS txn_value
FROM fact_transactions
GROUP BY 1 ORDER BY 1;

-- Q2. Deposits vs withdrawals/outflows trend with month-over-month growth
WITH m AS (
  SELECT date_trunc('month', transaction_date) AS month_start,
         SUM(CASE WHEN flow_direction = 'INFLOW'  THEN amount ELSE 0 END) AS deposits,
         SUM(CASE WHEN flow_direction = 'OUTFLOW' THEN amount ELSE 0 END) AS outflows
  FROM fact_transactions WHERE NOT is_failed GROUP BY 1)
SELECT month_start, deposits, outflows, deposits - outflows AS net_flow,
       ROUND(100.0 * (deposits - LAG(deposits) OVER (ORDER BY month_start))
             / NULLIF(LAG(deposits) OVER (ORDER BY month_start), 0), 2) AS deposits_mom_pct
FROM m ORDER BY month_start;

-- Q3. Average, median and p95 transaction value by type
SELECT transaction_type, COUNT(*) AS txn_count,
       ROUND(AVG(amount), 2) AS avg_value,
       ROUND(approx_percentile(amount, 0.5), 2)  AS median_value,
       ROUND(approx_percentile(amount, 0.95), 2) AS p95_value
FROM fact_transactions GROUP BY 1 ORDER BY txn_count DESC;

-- Q4. Monthly active customers and transactions per active customer
SELECT date_trunc('month', transaction_date) AS month_start,
       COUNT(DISTINCT customer_id) AS active_customers,
       ROUND(CAST(COUNT(*) AS double) / COUNT(DISTINCT customer_id), 2) AS txns_per_customer
FROM fact_transactions GROUP BY 1 ORDER BY 1;

-- Q5. Failed-transaction rate by type and channel (operational friction)
SELECT transaction_type, channel, COUNT(*) AS txn_count,
       ROUND(100.0 * AVG(CASE WHEN is_failed THEN 1.0 ELSE 0.0 END), 3) AS failed_pct
FROM fact_transactions GROUP BY 1, 2 HAVING COUNT(*) > 1000 ORDER BY failed_pct DESC;

-- Q6. Top states / segments by transaction value
SELECT c.state, c.customer_segment, COUNT(*) AS txn_count, SUM(t.amount) AS txn_value,
       RANK() OVER (ORDER BY SUM(t.amount) DESC) AS value_rank
FROM fact_transactions t JOIN dim_customer c ON t.customer_id = c.customer_id
GROUP BY 1, 2 ORDER BY value_rank LIMIT 15;

-- Q7. Customers with net outflow in the last 3 months of data (attrition-risk watchlist)
WITH bounds AS (SELECT MAX(transaction_date) AS max_d FROM fact_transactions)
SELECT t.customer_id, c.customer_segment, c.state,
       SUM(t.signed_amount) AS net_flow_3m, COUNT(*) AS txn_count_3m
FROM fact_transactions t
CROSS JOIN bounds b
JOIN dim_customer c ON t.customer_id = c.customer_id
WHERE t.transaction_date > date_add('month', -3, b.max_d) AND NOT t.is_failed
GROUP BY 1, 2, 3
HAVING SUM(t.signed_amount) < 0
ORDER BY net_flow_3m ASC LIMIT 50;

-- Q8. New accounts opened by year and type
SELECT year(open_date) AS open_year, account_type, COUNT(*) AS accounts_opened
FROM dim_account GROUP BY 1, 2 ORDER BY 1, 2;

-- Q9. Dormant accounts that still transact (possible reactivation or misclassification)
SELECT a.account_id, a.account_status, COUNT(*) AS txn_count, MAX(t.transaction_date) AS last_txn
FROM dim_account a JOIN fact_transactions t ON t.source_account_id = a.account_id
WHERE a.account_status IN ('DORMANT', 'CLOSED')
GROUP BY 1, 2 ORDER BY txn_count DESC LIMIT 25;
