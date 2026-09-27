#!/usr/bin/env bash
# IAM role that Snowflake assumes (storage integration) to read the curated lake and unload marts.
# Two-step handshake:
#   1) bash infra/snowflake_iam.sh
#        -> creates the role with a placeholder trust policy, prints the role ARN for 01_setup.sql
#   2) run snowflake/01_setup.sql, then DESC INTEGRATION and copy the two values:
#      bash infra/snowflake_iam.sh <STORAGE_AWS_IAM_USER_ARN> <STORAGE_AWS_EXTERNAL_ID>
#        -> locks the trust policy to your Snowflake account's IAM user + external ID
source "$(dirname "$0")/common.sh"
SF_ROLE="${PROJECT}-snowflake-role"
render infra/iam/snowflake-s3-policy.json "$BUILD_DIR/snowflake-s3-policy.json"

if [ $# -eq 0 ]; then
  render infra/iam/snowflake-trust-bootstrap.json "$BUILD_DIR/snowflake-trust.json"
  ensure_role "$SF_ROLE" "$BUILD_DIR/snowflake-trust.json"
  aws iam put-role-policy --role-name "$SF_ROLE" --policy-name lake-read-export \
    --policy-document "file://$BUILD_DIR/snowflake-s3-policy.json"
  log "Step 1 done. Use this in snowflake/01_setup.sql:"
  echo "  STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::${ACCOUNT_ID}:role/${SF_ROLE}'"
  echo "  STORAGE_ALLOWED_LOCATIONS = ('${S3_BASE}/')"
elif [ $# -eq 2 ]; then
  sed -e "s|__SF_IAM_USER_ARN__|$1|g" -e "s|__SF_EXTERNAL_ID__|$2|g" infra/iam/snowflake-trust.json \
    > "$BUILD_DIR/snowflake-trust-final.json"
  aws iam update-assume-role-policy --role-name "$SF_ROLE" \
    --policy-document "file://$BUILD_DIR/snowflake-trust-final.json"
  log "Step 2 done. Trust policy now allows only your Snowflake account. Wait ~30s, then in Snowflake run:"
  echo "  LIST @retail_bank.lake.lake_stage/curated/;"
else
  echo "usage: $0 [<STORAGE_AWS_IAM_USER_ARN> <STORAGE_AWS_EXTERNAL_ID>]"; exit 1
fi
