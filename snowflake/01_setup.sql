/* =====================================================================
   01_setup.sql  —  run as ACCOUNTADMIN in a Snowflake trial (Enterprise edition)
   Creates: warehouse, database, schemas, functional roles, S3 storage
   integration + external stage over the curated lake.
   Replace <ACCOUNT_ID> and <BUCKET> (values printed by infra/snowflake_iam.sh).
   ===================================================================== */
USE ROLE ACCOUNTADMIN;

-- Cost guardrail: XS warehouse that suspends after 60 s idle + monthly credit cap
CREATE WAREHOUSE IF NOT EXISTS bank_wh
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE RESOURCE MONITOR IF NOT EXISTS bank_rm WITH CREDIT_QUOTA = 20
  TRIGGERS ON 80 PERCENT DO NOTIFY ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE bank_wh SET RESOURCE_MONITOR = bank_rm;

-- Functional roles (least privilege; mirrors a business-role / technical-role design)
CREATE ROLE IF NOT EXISTS bank_data_engineer   COMMENT = 'Builds and loads the governed banking data products';
CREATE ROLE IF NOT EXISTS bank_dq_steward      COMMENT = 'Monitors data quality; can see quarantined records and raw identifiers';
CREATE ROLE IF NOT EXISTS bank_analyst         COMMENT = 'Self-service analytics on marts; PII masked';
CREATE ROLE IF NOT EXISTS bank_analyst_mid_atlantic COMMENT = 'Regional analyst; row-level restricted to Mid-Atlantic states';
GRANT ROLE bank_analyst, bank_analyst_mid_atlantic, bank_dq_steward TO ROLE bank_data_engineer;
GRANT ROLE bank_data_engineer TO ROLE SYSADMIN;

SET me = CURRENT_USER();
GRANT ROLE bank_data_engineer        TO USER IDENTIFIER($me);
GRANT ROLE bank_dq_steward           TO USER IDENTIFIER($me);
GRANT ROLE bank_analyst              TO USER IDENTIFIER($me);
GRANT ROLE bank_analyst_mid_atlantic TO USER IDENTIFIER($me);

GRANT USAGE ON WAREHOUSE bank_wh TO ROLE bank_data_engineer;
GRANT USAGE ON WAREHOUSE bank_wh TO ROLE bank_dq_steward;
GRANT USAGE ON WAREHOUSE bank_wh TO ROLE bank_analyst;
GRANT USAGE ON WAREHOUSE bank_wh TO ROLE bank_analyst_mid_atlantic;

-- Governance privileges the engineer needs to apply policies, tags and data metric functions
GRANT APPLY MASKING POLICY    ON ACCOUNT TO ROLE bank_data_engineer;
GRANT APPLY ROW ACCESS POLICY ON ACCOUNT TO ROLE bank_data_engineer;
GRANT APPLY TAG               ON ACCOUNT TO ROLE bank_data_engineer;
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT TO ROLE bank_data_engineer;
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE bank_data_engineer;
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE bank_dq_steward;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE bank_data_engineer;

-- S3 access without keys: IAM role assumed via storage integration.
-- IF NOT EXISTS matters: CREATE OR REPLACE would rotate the external ID and break the trust policy.
CREATE STORAGE INTEGRATION IF NOT EXISTS s3_retail_bank_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/retail-bank-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<BUCKET>/retail-bank/');
GRANT USAGE ON INTEGRATION s3_retail_bank_int TO ROLE bank_data_engineer;

-- >>> Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID, then run:
-- >>>   bash infra/snowflake_iam.sh <STORAGE_AWS_IAM_USER_ARN> <STORAGE_AWS_EXTERNAL_ID>
DESC INTEGRATION s3_retail_bank_int;

-- Database owned by the engineering role
CREATE DATABASE IF NOT EXISTS retail_bank COMMENT = 'Synthetic retail-banking data products (PaySim-derived; not real customers)';
GRANT OWNERSHIP ON DATABASE retail_bank TO ROLE bank_data_engineer COPY CURRENT GRANTS;

USE ROLE bank_data_engineer;
USE WAREHOUSE bank_wh;
USE DATABASE retail_bank;
CREATE SCHEMA IF NOT EXISTS lake       COMMENT = 'External stage + file formats over the S3 data lake';
CREATE SCHEMA IF NOT EXISTS curated    COMMENT = 'Conformed star schema loaded from S3 curated/ (Glue PySpark output)';
CREATE SCHEMA IF NOT EXISTS dq         COMMENT = 'Quarantine records, Glue DQ results, Snowflake DQ scorecards';
CREATE SCHEMA IF NOT EXISTS governance COMMENT = 'Tags, masking and row access policies, entitlement mappings';
CREATE SCHEMA IF NOT EXISTS analytics  COMMENT = 'Secure, self-service marts for analysts and Tableau';
CREATE SCHEMA IF NOT EXISTS ops        COMMENT = 'Load procedures and tasks';

CREATE FILE FORMAT IF NOT EXISTS lake.parquet_ff TYPE = PARQUET;
CREATE FILE FORMAT IF NOT EXISTS lake.csv_export_ff TYPE = CSV COMPRESSION = NONE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ();
CREATE STAGE IF NOT EXISTS lake.lake_stage
  URL = 's3://<BUCKET>/retail-bank/'
  STORAGE_INTEGRATION = s3_retail_bank_int
  FILE_FORMAT = lake.parquet_ff;

-- After step 2 of snowflake_iam.sh this should list Parquet files:
-- LIST @lake.lake_stage/curated/fact_transactions/;
