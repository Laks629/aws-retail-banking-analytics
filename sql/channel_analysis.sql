-- Channel & behaviour analysis (Athena / Trino SQL)

-- Q10. Digital vs branch/ATM channel mix by month (share of transactions)
SELECT date_trunc('month', transaction_date) AS month_start, channel,
       COUNT(*) AS txn_count,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY date_trunc('month', transaction_date)), 2) AS share_pct
FROM fact_transactions GROUP BY 1, 2 ORDER BY 1, 2;

-- Q11. Digital adoption by customer segment (enrolled vs actual digital usage)
SELECT c.customer_segment, c.digital_enrolled_flag,
       COUNT(DISTINCT t.customer_id) AS customers,
       ROUND(100.0 * AVG(CASE WHEN t.digital_channel_flag THEN 1.0 ELSE 0.0 END), 2) AS digital_txn_pct
FROM fact_transactions t JOIN dim_customer c ON t.customer_id = c.customer_id
GROUP BY 1, 2 ORDER BY 1, 2;

-- Q12. Hour-of-day profile by channel (staffing / capacity planning)
SELECT transaction_hour, channel, COUNT(*) AS txn_count
FROM fact_transactions GROUP BY 1, 2 ORDER BY 1, 2;

-- Q13. Busiest branches and ATM share per branch
SELECT b.branch_name, b.region,
       COUNT(*) AS txn_count,
       ROUND(100.0 * AVG(CASE WHEN t.channel = 'ATM' THEN 1.0 ELSE 0.0 END), 1) AS atm_share_pct
FROM fact_transactions t JOIN dim_branch b ON t.branch_id = b.branch_id
GROUP BY 1, 2 ORDER BY txn_count DESC LIMIT 15;

-- Q14. Merchant category spend ranking with share of card/bill-pay value
SELECT merchant_category, SUM(amount) AS spend,
       ROUND(100.0 * SUM(amount) / SUM(SUM(amount)) OVER (), 2) AS spend_share_pct
FROM fact_transactions WHERE merchant_category IS NOT NULL
GROUP BY 1 ORDER BY spend DESC;
