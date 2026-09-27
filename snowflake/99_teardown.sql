-- Removes all Snowflake objects for this project (run as ACCOUNTADMIN).
USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS retail_bank;
DROP INTEGRATION IF EXISTS s3_retail_bank_int;
DROP WAREHOUSE IF EXISTS bank_wh;
DROP RESOURCE MONITOR IF EXISTS bank_rm;
DROP ROLE IF EXISTS bank_analyst_mid_atlantic;
DROP ROLE IF EXISTS bank_analyst;
DROP ROLE IF EXISTS bank_dq_steward;
DROP ROLE IF EXISTS bank_data_engineer;
