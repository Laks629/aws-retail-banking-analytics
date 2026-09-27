#!/usr/bin/env bash
# Deletes every chargeable resource created by deploy.sh / deploy_streaming.sh, including the S3 bucket.
source "$(dirname "$0")/common.sh"
read -r -p "Delete ALL project resources and s3://${BUCKET}? Type the bucket name to confirm: " answer
[ "$answer" = "$BUCKET" ] || { echo "Aborted."; exit 1; }
set +e
log "Streaming";      for u in $(aws lambda list-event-source-mappings --function-name "$LAMBDA_FN" --query 'EventSourceMappings[].UUID' --output text 2>/dev/null); do aws lambda delete-event-source-mapping --uuid "$u" >/dev/null; done
aws lambda delete-function --function-name "$LAMBDA_FN" 2>/dev/null
aws kinesis delete-stream --stream-name "$STREAM" --enforce-consumer-deletion 2>/dev/null
log "Orchestration";  aws stepfunctions delete-state-machine --state-machine-arn "$SM_ARN" 2>/dev/null
aws cloudwatch delete-alarms --alarm-names "${PROJECT}-pipeline-failed" "${PROJECT}-stream-lambda-errors" 2>/dev/null
log "Glue";           aws glue delete-crawler --name "$CRAWLER" 2>/dev/null
aws glue delete-job --job-name "$GLUE_JOB" >/dev/null 2>&1
aws glue delete-database --name "$DATABASE" 2>/dev/null
log "Athena";         aws athena delete-work-group --work-group "$WORKGROUP" --recursive-delete-option 2>/dev/null
log "SNS";            aws sns delete-topic --topic-arn "$TOPIC_ARN" 2>/dev/null
log "IAM"
for r in "$GLUE_ROLE" "$SFN_ROLE" "$LAMBDA_ROLE" "${PROJECT}-snowflake-role"; do
  for p in $(aws iam list-attached-role-policies --role-name "$r" --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null); do aws iam detach-role-policy --role-name "$r" --policy-arn "$p"; done
  for p in $(aws iam list-role-policies --role-name "$r" --query 'PolicyNames' --output text 2>/dev/null); do aws iam delete-role-policy --role-name "$r" --policy-name "$p"; done
  aws iam delete-role --role-name "$r" 2>/dev/null
done
log "S3 bucket";      aws s3 rb "s3://${BUCKET}" --force
log "CloudWatch log groups (optional cleanup)"
aws logs delete-log-group --log-group-name "/aws/lambda/${LAMBDA_FN}" 2>/dev/null
echo "Teardown complete. Glue job logs under /aws-glue/* can be deleted from the CloudWatch console."
