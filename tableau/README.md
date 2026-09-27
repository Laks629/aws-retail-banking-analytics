# Tableau dashboard spec

**Data:** CSV marts unloaded from Snowflake by `snowflake/05_marts_and_export.sql` (run as `BANK_ANALYST`,
so exports carry the analyst's masking and row policies), then
`aws s3 cp s3://<bucket>/retail-bank/exports/ tableau/data/ --recursive`.

Tableau Public (free) connects to files only. With a Tableau Desktop trial you can instead connect live:
*Connect → Snowflake* (warehouse `BANK_WH`, role `BANK_ANALYST`, database `RETAIL_BANK`, schema `ANALYTICS`).
`tableau/export_marts.py` is an alternative that exports the equivalent Athena views.

Subtitle on every dashboard: *"Synthetic data — PaySim-derived retail-banking simulation, not real customers."*

| CSV | Used for |
|---|---|
| `mart_monthly_kpis` | KPI cards, monthly deposits vs outflows, active customers |
| `mart_daily_transactions` | type / channel mix, failed rate |
| `mart_monthly_segment` | state map, segment × digital adoption |
| `mart_customer_value` | net-flow distribution, net-outflow watchlist (hashed IDs) |
| `mart_merchant_monthly` | merchant category spend |
| `mart_branch_activity` | branch & ATM network |
| `mart_dq_scorecard`, `mart_dq_run_history`, `mart_dq_rule_results`, `mart_quarantine_reasons` | Data-quality page |

## Dashboard 1 — Executive Banking Overview
KPI cards (transaction value, deposits, net flow, active customers, digital share, failed rate) ·
monthly deposits vs outflows line · value by transaction type · 100 % stacked channel mix by month · date filter.

## Dashboard 2 — Customers & Channels
Filled map of value by state · segment × digital-share heatmap · net-flow histogram coloured by `flow_band` ·
top-20 net-outflow customers table · merchant-category treemap.

## Dashboard 3 — Data Quality & Governance
KPI cards: raw vs curated DQ score, quarantine rate vs 5 % threshold, rows quarantined ·
pass rate by DQ dimension (`mart_dq_scorecard`) · quarantined rows by reason (colour = batch/stream) ·
Glue DQ rule table (Passed/Failed, raw vs curated) · run-history trend with threshold reference line ·
a text box explaining the access model (hashed IDs, regional row security).

## Calculated fields
```
Failed rate            SUM([failed_txn_count]) / SUM([txn_count])
Digital share (daily)  SUM(IIF([digital_channel_flag], [txn_count], 0)) / SUM([txn_count])
Latest month           [month_start] = {FIXED : MAX([month_start])}
Quarantine rate %      [quarantine_rate] * 100
```
Publish: File → Save to Tableau Public; add the link and PNG screenshots to the root README.
