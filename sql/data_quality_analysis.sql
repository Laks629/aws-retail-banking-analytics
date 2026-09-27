-- Data-quality & pipeline-health queries (Athena / Trino SQL)

-- Q15. Latest run: rows in, published, quarantined, rate, DQ scores
SELECT * FROM dq_run_summary ORDER BY run_ts DESC LIMIT 1;

-- Q16. Quarantine reasons for the latest run (batch vs stream)
SELECT reason, ingest_source, row_count
FROM dq_quarantine_reasons
WHERE run_id = (SELECT MAX(run_id) FROM dq_quarantine_reasons)
ORDER BY row_count DESC;

-- Q17. Glue Data Quality rules that failed on raw data but pass on curated
SELECT r.rule, r.outcome AS raw_outcome, c.outcome AS curated_outcome, r.failure_reason
FROM mart_dq_rule_results r
LEFT JOIN mart_dq_rule_results c ON r.rule = c.rule AND c.dataset = 'curated_transactions'
WHERE r.dataset = 'raw_transactions'
ORDER BY r.outcome, r.rule;

-- Q18. DQ score and quarantine-rate trend across runs
SELECT run_ts, pipeline_status, raw_dq_score, curated_dq_score, ROUND(100 * quarantine_rate, 3) AS quarantine_pct
FROM dq_run_summary ORDER BY run_ts;

-- Q19. Sample quarantined records for a reason (root-cause investigation)
SELECT transaction_id, source_account_id, raw_amount, raw_transaction_timestamp, transaction_type,
       channel, dq_fail_reasons, source_file
FROM quarantine_transactions
WHERE dq_fail_reasons LIKE '%ORPHAN_ACCOUNT%' LIMIT 20;

-- Q20. Reconciliation: raw = curated + quarantined (should return 0 difference)
SELECT s.raw_rows, s.curated_rows, s.quarantined_rows,
       (SELECT COUNT(*) FROM fact_transactions) AS curated_in_catalog,
       s.raw_rows - (SELECT COUNT(*) FROM fact_transactions) - s.quarantined_rows AS unexplained_diff
FROM dq_run_summary s ORDER BY s.run_ts DESC LIMIT 1;

-- Q21. Freshness: newest transaction per ingestion path
SELECT ingest_source, MAX(transaction_ts) AS newest_txn,
       date_diff('hour', MAX(transaction_ts), CAST(now() AS timestamp)) AS hours_since_newest
FROM fact_transactions GROUP BY 1;
