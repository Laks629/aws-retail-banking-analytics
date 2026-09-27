# One-day build runbook

**Scope for one day (~9 hours):** S3 lake → Glue/PySpark + Glue Data Quality → Step Functions →
Snowflake (governance + DQ + marts) → Tableau. The Kinesis/Lambda streaming path is a **stretch** goal.

**Cost:** a few dollars on AWS if you tear down the same day (Glue ≈ $0.44/DPU-hour, 5 DPUs for ~5–10 min per run).
Snowflake trial credits cover the Snowflake side. Verify current pricing.

Windows: run `.sh` scripts from **Git Bash** or **WSL**.

---

## 0 · Accounts & tools (45 min)
1. AWS: MFA on root; Billing → Budgets → $10 budget alert. IAM user for the CLI (personal sandbox:
   `AdministratorAccess`), create an access key, `aws configure` (region `us-east-1`). Check: `aws sts get-caller-identity`.
2. Snowflake: start a 30-day trial — **Enterprise edition**, **AWS**, region **US East (N. Virginia)**
   (same region as the bucket; needed for masking, row access policies and Data Metric Functions).
3. Install Python 3.10+, Git, AWS CLI v2, Java 17 (local tests), Tableau Public.
4. Kaggle → Settings → API → *Create New Token* → put `kaggle.json` in `~/.kaggle/`.

## 1 · Repo to GitHub (20 min)
```bash
cd aws-retail-banking-analytics
python -m venv .venv && source .venv/bin/activate     # Git Bash: source .venv/Scripts/activate
pip install -r requirements-dev.txt
git init -b main && git add . && git commit -m "Scaffold: governed retail banking analytics platform"
git remote add origin https://github.com/Laks629/aws-retail-banking-analytics.git   # create empty repo first
git push -u origin main
```

## 2 · Data prep (60 min)
```bash
kaggle datasets download -d ealaxi/paysim1 -p data/source --unzip
python data_generator/generate_dimensions.py
python data_generator/prepare_transactions.py --input data/source/PS_20174392719_1491204439457_log.csv
python data_generator/inject_dq_issues.py      # note the numbers in data/output/injected_defects.json
pytest -q tests
```
Short on time or disk? `python data_generator/make_sample_paysim.py --rows 1000000 --out data/source/paysim_sample.csv`
and point `--input` at it.

## 3 · Deploy AWS (30 min)
```bash
cp config.env.example config.env      # unique BUCKET, your ALERT_EMAIL
bash infra/deploy.sh                  # confirm the SNS email afterwards
```

## 4 · Run the orchestrated pipeline (45 min)
```bash
aws stepfunctions start-execution --state-machine-arn <ARN printed by deploy.sh>
```
Screenshots: Step Functions graph (green), Glue run + **Data quality** tab, S3 `curated/` partitions, Catalog tables.
If Glue fails: Glue → job → Runs → error + CloudWatch logs; fix; `bash infra/deploy.sh`; rerun.

**Quality-gate demo:** set `"--MAX_QUARANTINE_RATE": "0.001"` in `infra/deploy.sh`, redeploy, run → job fails
with *Quality gate failed*, curated/ untouched, failure email. Restore `0.05`, redeploy, rerun.

## 5 · Snowflake: connect, load, govern, monitor (90 min)
Run each file in a Snowsight SQL worksheet (Projects → Worksheets), top to bottom.
```bash
bash infra/snowflake_iam.sh           # step 1: creates the IAM role, prints the ARN + S3 location
```
1. `snowflake/01_setup.sql` — replace `<ACCOUNT_ID>` / `<BUCKET>`, run. From `DESC INTEGRATION` copy
   `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`, then:
   ```bash
   bash infra/snowflake_iam.sh <STORAGE_AWS_IAM_USER_ARN> <STORAGE_AWS_EXTERNAL_ID>
   ```
   Wait ~30 s; `LIST @retail_bank.lake.lake_stage/curated/fact_transactions/;` should list Parquet files.
2. `snowflake/02_tables_and_load.sql` — tables with business-definition comments, `CALL ops.refresh_from_lake()`.
3. `snowflake/03_governance.sql` — tags, masking, row access policy, grants.
4. `snowflake/04_data_quality.sql` — DMFs, referential integrity, reconciliation, DQ scorecard.
   `unexplained_diff` in `v_load_reconciliation` should be 0.

Common errors: *Access Denied* on LIST → trust policy / external ID mismatch (rerun step 2 with exact values);
*insufficient privileges* → re-run the grant section of `01_setup.sql` as ACCOUNTADMIN.

## 6 · Marts, governance demo, export (45 min)
`snowflake/05_marts_and_export.sql`. Screenshot the three governance result sets: analyst (hashed IDs, all states),
Mid-Atlantic analyst (5 states), steward (raw IDs). Then:
```bash
source config.env
aws s3 cp "s3://$BUCKET/retail-bank/exports/" tableau/data/ --recursive
```

## 7 · Tableau (2 h)
Build the three dashboards in `tableau/README.md`, publish to Tableau Public, screenshot to `dashboard/screenshots/`.

## 8 · SQL insights (30 min, can overlap with 7)
Run a handful of queries from `sql/*.sql` in Athena (or port them to Snowflake) and write 3–5 business insights.

## 9 · README + push (45 min)
Fill the Results table, add the Tableau link and screenshots; commit and push. Check the CI badge is green.

## 10 · Tear down (10 min)
```bash
bash infra/teardown.sh
```
and run `snowflake/99_teardown.sql` once you have your screenshots (or keep Snowflake until the trial ends).

## Stretch — streaming (60 min)
`bash infra/deploy_streaming.sh`, then
`python streaming/kinesis_producer.py --stream retail-bank-transactions --region us-east-1 --limit 5000`,
rerun Step Functions and `CALL ops.refresh_from_lake();`. Delete the stream afterwards.

---

## Suggested commits
1. Scaffold + CI · 2. Data generators & defect injection · 3. Glue PySpark ETL + quality gate ·
4. Glue DQ ruleset · 5. AWS deploy scripts + Step Functions · 6. Snowflake load & governance ·
7. Snowflake DQ monitoring · 8. Marts + Tableau · 9. README results & screenshots

## Resume bullets (claim only what you ran; fill in real numbers)
**Retail Banking Governed Analytics Platform | AWS (S3, Glue/PySpark, Glue Data Quality, Step Functions, Athena), Snowflake, SQL, Tableau**
- Built a Step Functions–orchestrated pipeline processing 6.3M+ synthetic retail-bank transactions with AWS Glue/PySpark into partitioned Parquet, loading a Snowflake star schema through a keyless S3 storage integration.
- Implemented layered data-quality controls — 11 row-level rules with reason-coded quarantine, a 12-rule Glue Data Quality ruleset with an automated publish gate, and Snowflake Data Metric Functions — reconciling 100% of injected defects and S3-to-Snowflake row counts.
- Enforced data access governance in Snowflake with role-based access, classification tags, dynamic masking and row-level security, and documented business definitions and lineage; delivered secure self-service marts and Tableau dashboards on deposits, channel adoption, customer net flow and DQ KPIs.
