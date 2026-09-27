#!/usr/bin/env bash
# Deploys the batch pipeline: S3 lake, IAM, Glue job + DQ ruleset, Data Catalog
# crawler, Athena workgroup, SNS alerts, Step Functions orchestration, CloudWatch alarm.
# Idempotent: safe to re-run after code changes (re-uploads scripts, updates job/state machine).
source "$(dirname "$0")/common.sh"

log "Account ${ACCOUNT_ID} | region ${AWS_REGION} | bucket ${BUCKET}"

log "S3 bucket + guardrails"
if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  if [ "$AWS_REGION" = "us-east-1" ]; then aws s3api create-bucket --bucket "$BUCKET" >/dev/null
  else aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint="$AWS_REGION" >/dev/null; fi
fi
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
cat > "$BUILD_DIR/lifecycle.json" <<JSON
{"Rules":[{"ID":"expire-athena-results","Status":"Enabled","Filter":{"Prefix":"${PREFIX}/athena-results/"},"Expiration":{"Days":7}},
          {"ID":"expire-glue-temp","Status":"Enabled","Filter":{"Prefix":"${PREFIX}/tmp/"},"Expiration":{"Days":3}}]}
JSON
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" --lifecycle-configuration "file://$BUILD_DIR/lifecycle.json"

log "Upload raw data (raw/ is treated as immutable) + job code"
[ -d data/output/raw ] || { echo "data/output/raw missing - run the data_generator scripts first"; exit 1; }
aws s3 sync data/output/raw/ "${S3_BASE}/raw/" --only-show-errors
aws s3 cp glue/retail_bank_etl.py "${S3_BASE}/scripts/retail_bank_etl.py" --only-show-errors
aws s3 cp glue/data_quality_rules.dqdl "${S3_BASE}/scripts/data_quality_rules.dqdl" --only-show-errors

log "IAM roles"
render infra/iam/glue-s3-policy.json "$BUILD_DIR/glue-s3-policy.json"
ensure_role "$GLUE_ROLE" infra/iam/glue-trust.json
aws iam attach-role-policy --role-name "$GLUE_ROLE" --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
aws iam put-role-policy --role-name "$GLUE_ROLE" --policy-name project-s3 --policy-document "file://$BUILD_DIR/glue-s3-policy.json"

log "SNS alerts topic"
aws sns create-topic --name "$TOPIC" >/dev/null
if [ -n "${ALERT_EMAIL:-}" ]; then
  if ! aws sns list-subscriptions-by-topic --topic-arn "$TOPIC_ARN" --query "Subscriptions[].Endpoint" --output text | grep -q "$ALERT_EMAIL"; then
    aws sns subscribe --topic-arn "$TOPIC_ARN" --protocol email --notification-endpoint "$ALERT_EMAIL" >/dev/null
    echo "  check ${ALERT_EMAIL} and confirm the subscription"
  fi
fi

log "Glue Data Catalog database"
aws glue get-database --name "$DATABASE" >/dev/null 2>&1 || \
  aws glue create-database --database-input "{\"Name\":\"${DATABASE}\",\"Description\":\"Retail banking analytics lake (synthetic data)\"}"

log "Glue ETL job (PySpark, Glue 4.0)"
cat > "$BUILD_DIR/job.json" <<JSON
{
  "Role": "arn:aws:iam::${ACCOUNT_ID}:role/${GLUE_ROLE}",
  "Command": {"Name": "glueetl", "ScriptLocation": "${S3_BASE}/scripts/retail_bank_etl.py", "PythonVersion": "3"},
  "DefaultArguments": {
    "--BUCKET": "${BUCKET}", "--PREFIX": "${PREFIX}", "--MAX_QUARANTINE_RATE": "0.05",
    "--DQ_RULESET_S3": "${S3_BASE}/scripts/data_quality_rules.dqdl",
    "--TempDir": "${S3_BASE}/tmp/", "--job-language": "python",
    "--enable-metrics": "true", "--enable-observability-metrics": "true",
    "--enable-continuous-cloudwatch-log": "true", "--enable-spark-ui": "false"
  },
  "GlueVersion": "4.0", "WorkerType": "G.1X", "NumberOfWorkers": 5,
  "Timeout": 45, "MaxRetries": 0, "ExecutionProperty": {"MaxConcurrentRuns": 1}
}
JSON
if aws glue get-job --job-name "$GLUE_JOB" >/dev/null 2>&1; then
  aws glue update-job --job-name "$GLUE_JOB" --job-update "file://$BUILD_DIR/job.json" >/dev/null
else
  aws glue create-job --name "$GLUE_JOB" --cli-input-json "file://$BUILD_DIR/job.json" >/dev/null
fi

log "Glue crawler over curated/, quarantine/ and dq-results/"
cat > "$BUILD_DIR/targets.json" <<JSON
{"S3Targets": [
  {"Path": "${S3_BASE}/curated/fact_transactions/", "Exclusions": ["**/_SUCCESS"]},
  {"Path": "${S3_BASE}/curated/dim_customer/"},
  {"Path": "${S3_BASE}/curated/dim_account/"},
  {"Path": "${S3_BASE}/curated/dim_branch/"},
  {"Path": "${S3_BASE}/quarantine/quarantine_transactions/"},
  {"Path": "${S3_BASE}/quarantine/quarantine_accounts/"},
  {"Path": "${S3_BASE}/dq-results/dq_rule_outcomes/"},
  {"Path": "${S3_BASE}/dq-results/dq_run_summary/"},
  {"Path": "${S3_BASE}/dq-results/dq_quarantine_reasons/"}
]}
JSON
CRAWLER_ARGS=(--role "arn:aws:iam::${ACCOUNT_ID}:role/${GLUE_ROLE}" --database-name "$DATABASE"
  --targets "file://$BUILD_DIR/targets.json"
  --schema-change-policy UpdateBehavior=UPDATE_IN_DATABASE,DeleteBehavior=LOG
  --configuration '{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}}}')
if aws glue get-crawler --name "$CRAWLER" >/dev/null 2>&1; then
  aws glue update-crawler --name "$CRAWLER" "${CRAWLER_ARGS[@]}"
else
  aws glue create-crawler --name "$CRAWLER" "${CRAWLER_ARGS[@]}"
fi

log "Athena workgroup (results in S3, 10 GB per-query scan cap)"
WG_CONFIG="{\"ResultConfiguration\":{\"OutputLocation\":\"${S3_BASE}/athena-results/\"},\"EnforceWorkGroupConfiguration\":true,\"PublishCloudWatchMetricsEnabled\":true,\"BytesScannedCutoffPerQuery\":10737418240}"
if aws athena get-work-group --work-group "$WORKGROUP" >/dev/null 2>&1; then
  aws athena update-work-group --work-group "$WORKGROUP" --configuration-updates \
    "{\"ResultConfigurationUpdates\":{\"OutputLocation\":\"${S3_BASE}/athena-results/\"},\"EnforceWorkGroupConfiguration\":true,\"PublishCloudWatchMetricsEnabled\":true,\"BytesScannedCutoffPerQuery\":10737418240}"
else
  aws athena create-work-group --name "$WORKGROUP" --configuration "$WG_CONFIG"
fi

log "Step Functions orchestration"
render infra/iam/sfn-policy.json "$BUILD_DIR/sfn-policy.json"
render orchestration/state_machine.asl.json "$BUILD_DIR/state_machine.json"
ensure_role "$SFN_ROLE" infra/iam/sfn-trust.json
aws iam put-role-policy --role-name "$SFN_ROLE" --policy-name pipeline --policy-document "file://$BUILD_DIR/sfn-policy.json"
if aws stepfunctions describe-state-machine --state-machine-arn "$SM_ARN" >/dev/null 2>&1; then
  aws stepfunctions update-state-machine --state-machine-arn "$SM_ARN" \
    --definition "file://$BUILD_DIR/state_machine.json" --role-arn "arn:aws:iam::${ACCOUNT_ID}:role/${SFN_ROLE}" >/dev/null
else
  for attempt in 1 2 3 4 5; do   # a brand-new role can take a few seconds to become assumable
    aws stepfunctions create-state-machine --name "$STATE_MACHINE" --definition "file://$BUILD_DIR/state_machine.json" \
      --role-arn "arn:aws:iam::${ACCOUNT_ID}:role/${SFN_ROLE}" >/dev/null && break
    echo "  retrying in 10s ($attempt)"; sleep 10
  done
fi

log "CloudWatch alarm: any failed pipeline execution -> SNS"
aws cloudwatch put-metric-alarm --alarm-name "${PROJECT}-pipeline-failed" \
  --namespace AWS/States --metric-name ExecutionsFailed --dimensions Name=StateMachineArn,Value="$SM_ARN" \
  --statistic Sum --period 300 --evaluation-periods 1 --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold --treat-missing-data notBreaching \
  --alarm-actions "$TOPIC_ARN"

log "Done. Start the pipeline with:"
echo "  aws stepfunctions start-execution --state-machine-arn $SM_ARN"
