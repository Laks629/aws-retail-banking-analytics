/* =====================================================================
   03_governance.sql — run as BANK_DATA_ENGINEER
   Data access governance: classification tags, dynamic masking, row-level
   security, secure self-service views and role-based grants.
   ===================================================================== */
USE ROLE bank_data_engineer; USE WAREHOUSE bank_wh; USE DATABASE retail_bank;

-- 1. Classification tags (metadata that travels with the column)
CREATE TAG IF NOT EXISTS governance.data_classification
  ALLOWED_VALUES 'DIRECT_IDENTIFIER', 'QUASI_IDENTIFIER', 'SENSITIVE_FINANCIAL', 'INTERNAL'
  COMMENT = 'Sensitivity classification used to drive masking and access reviews';
CREATE TAG IF NOT EXISTS governance.data_owner COMMENT = 'Accountable business owner';

ALTER TABLE curated.fact_transactions MODIFY COLUMN customer_id       SET TAG governance.data_classification = 'DIRECT_IDENTIFIER';
ALTER TABLE curated.fact_transactions MODIFY COLUMN source_account_id SET TAG governance.data_classification = 'DIRECT_IDENTIFIER';
ALTER TABLE curated.fact_transactions MODIFY COLUMN counterparty_id   SET TAG governance.data_classification = 'DIRECT_IDENTIFIER';
ALTER TABLE curated.fact_transactions MODIFY COLUMN balance_before    SET TAG governance.data_classification = 'SENSITIVE_FINANCIAL';
ALTER TABLE curated.fact_transactions MODIFY COLUMN balance_after     SET TAG governance.data_classification = 'SENSITIVE_FINANCIAL';
ALTER TABLE curated.dim_customer      MODIFY COLUMN customer_id       SET TAG governance.data_classification = 'DIRECT_IDENTIFIER';
ALTER TABLE curated.dim_customer      MODIFY COLUMN state             SET TAG governance.data_classification = 'QUASI_IDENTIFIER';
ALTER TABLE curated.dim_account       MODIFY COLUMN account_id        SET TAG governance.data_classification = 'DIRECT_IDENTIFIER';
ALTER TABLE curated.dim_account       MODIFY COLUMN current_balance   SET TAG governance.data_classification = 'SENSITIVE_FINANCIAL';
ALTER TABLE curated.fact_transactions SET TAG governance.data_owner = 'Retail Bank - Payments & Deposits Analytics';
ALTER TABLE curated.dim_customer      SET TAG governance.data_owner = 'Retail Bank - Customer Data Office';

-- 2. Dynamic masking: analysts get a deterministic hash (joins still work), stewards/engineers see raw.
--    CURRENT_ROLE() (primary role) is used on purpose: Snowsight enables secondary roles by default,
--    which would make IS_ROLE_IN_SESSION() unmask data during an analyst-role demo.
CREATE OR REPLACE MASKING POLICY governance.mask_identifier AS (val STRING) RETURNS STRING ->
  CASE WHEN CURRENT_ROLE() IN ('BANK_DQ_STEWARD', 'BANK_DATA_ENGINEER', 'ACCOUNTADMIN')
       THEN val
       ELSE 'H_' || LEFT(SHA2(val, 256), 16) END
  COMMENT = 'Pseudonymise direct identifiers for non-privileged roles';

CREATE OR REPLACE MASKING POLICY governance.mask_balance AS (val NUMBER(18,2)) RETURNS NUMBER(18,2) ->
  CASE WHEN CURRENT_ROLE() IN ('BANK_DQ_STEWARD', 'BANK_DATA_ENGINEER', 'ACCOUNTADMIN')
       THEN val
       ELSE ROUND(val, -3) END
  COMMENT = 'Band balances to the nearest $1,000 for analysts';

ALTER TABLE curated.fact_transactions MODIFY COLUMN customer_id       SET MASKING POLICY governance.mask_identifier;
ALTER TABLE curated.fact_transactions MODIFY COLUMN source_account_id SET MASKING POLICY governance.mask_identifier;
ALTER TABLE curated.fact_transactions MODIFY COLUMN counterparty_id   SET MASKING POLICY governance.mask_identifier;
ALTER TABLE curated.fact_transactions MODIFY COLUMN balance_before    SET MASKING POLICY governance.mask_balance;
ALTER TABLE curated.fact_transactions MODIFY COLUMN balance_after     SET MASKING POLICY governance.mask_balance;
ALTER TABLE curated.dim_customer      MODIFY COLUMN customer_id       SET MASKING POLICY governance.mask_identifier;
ALTER TABLE curated.dim_account       MODIFY COLUMN account_id        SET MASKING POLICY governance.mask_identifier;
ALTER TABLE curated.dim_account       MODIFY COLUMN customer_id       SET MASKING POLICY governance.mask_identifier;
ALTER TABLE curated.dim_account       MODIFY COLUMN current_balance   SET MASKING POLICY governance.mask_balance;

-- 3. Row-level security: regional roles only see customers in their entitled states
CREATE TABLE IF NOT EXISTS governance.state_entitlements (role_name STRING, state STRING)
  COMMENT = 'Which restricted roles may see which states (maintained via access requests)';
TRUNCATE TABLE governance.state_entitlements;
INSERT INTO governance.state_entitlements VALUES
  ('BANK_ANALYST_MID_ATLANTIC', 'MD'), ('BANK_ANALYST_MID_ATLANTIC', 'VA'),
  ('BANK_ANALYST_MID_ATLANTIC', 'DC'), ('BANK_ANALYST_MID_ATLANTIC', 'DE'),
  ('BANK_ANALYST_MID_ATLANTIC', 'PA');

CREATE OR REPLACE ROW ACCESS POLICY governance.rap_customer_state AS (state_val STRING) RETURNS BOOLEAN ->
  CURRENT_ROLE() IN ('BANK_DATA_ENGINEER', 'BANK_DQ_STEWARD', 'BANK_ANALYST', 'ACCOUNTADMIN')
  OR EXISTS (SELECT 1 FROM governance.state_entitlements e
             WHERE e.role_name = CURRENT_ROLE() AND e.state = state_val)
  COMMENT = 'Unrestricted roles see all states; regional roles see entitled states only';
ALTER TABLE curated.dim_customer ADD ROW ACCESS POLICY governance.rap_customer_state ON (state);

-- 4. Grants: analysts consume analytics marts only; stewards also see DQ + quarantine
GRANT USAGE ON DATABASE retail_bank TO ROLE bank_analyst;
GRANT USAGE ON DATABASE retail_bank TO ROLE bank_analyst_mid_atlantic;
GRANT USAGE ON DATABASE retail_bank TO ROLE bank_dq_steward;
GRANT USAGE ON SCHEMA analytics TO ROLE bank_analyst;
GRANT USAGE ON SCHEMA analytics TO ROLE bank_analyst_mid_atlantic;
GRANT USAGE ON SCHEMA analytics TO ROLE bank_dq_steward;
GRANT USAGE ON SCHEMA dq        TO ROLE bank_dq_steward;
GRANT SELECT ON ALL TABLES IN SCHEMA dq TO ROLE bank_dq_steward;
GRANT SELECT ON FUTURE TABLES IN SCHEMA dq TO ROLE bank_dq_steward;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA dq TO ROLE bank_dq_steward;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA analytics TO ROLE bank_analyst;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA analytics TO ROLE bank_analyst_mid_atlantic;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA analytics TO ROLE bank_dq_steward;

-- 5. Access review evidence: what is tagged, masked and restricted
SELECT * FROM TABLE(retail_bank.information_schema.policy_references(ref_entity_name => 'retail_bank.curated.fact_transactions', ref_entity_domain => 'table'));
SELECT * FROM TABLE(retail_bank.information_schema.tag_references_all_columns('retail_bank.curated.fact_transactions', 'table'));
