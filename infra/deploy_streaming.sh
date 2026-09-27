#!/usr/bin/env bash
# Optional streaming path: Kinesis Data Stream (1 shard) -> Lambda -> S3 raw/source=stream/
source "$(dirname "$0")/common.sh"

log "Kinesis stream ${STREAM} (provisioned, 1 shard - delete when finished)"
if ! aws kinesis describe-stream-summary --stream-name "$STREAM" >/dev/null 2>&1; then
  aws kinesis create-stream --stream-name "$STREAM" --shard-count 1
  aws kinesis wait stream-exists --stream-name "$STREAM"
fi
STREAM_ARN="$(aws kinesis describe-stream-summary --stream-name "$STREAM" --query StreamDescriptionSummary.StreamARN --output text)"

log "Lambda role"
render infra/iam/lambda-s3-policy.json "$BUILD_DIR/lambda-s3-policy.json"
ensure_role "$LAMBDA_ROLE" infra/iam/lambda-trust.json
aws iam attach-role-policy --role-name "$LAMBDA_ROLE" --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole
aws iam put-role-policy --role-name "$LAMBDA_ROLE" --policy-name land-stream --policy-document "file://$BUILD_DIR/lambda-s3-policy.json"

log "Package + deploy Lambda"
PY_BIN="$(command -v python3 || command -v python)"
"$PY_BIN" - <<PY
import zipfile
with zipfile.ZipFile("$BUILD_DIR/lambda.zip", "w", zipfile.ZIP_DEFLATED) as z:
    z.write("streaming/lambda_consumer.py", "lambda_consumer.py")
PY
if aws lambda get-function --function-name "$LAMBDA_FN" >/dev/null 2>&1; then
  aws lambda update-function-code --function-name "$LAMBDA_FN" --zip-file "fileb://$BUILD_DIR/lambda.zip" >/dev/null
else
  for attempt in 1 2 3 4 5; do
    aws lambda create-function --function-name "$LAMBDA_FN" --runtime python3.12 \
      --handler lambda_consumer.handler --zip-file "fileb://$BUILD_DIR/lambda.zip" \
      --role "arn:aws:iam::${ACCOUNT_ID}:role/${LAMBDA_ROLE}" --timeout 60 --memory-size 256 \
      --environment "Variables={BUCKET=${BUCKET},PREFIX=${PREFIX}}" >/dev/null && break
    echo "  retrying in 10s ($attempt)"; sleep 10
  done
  aws lambda wait function-active-v2 --function-name "$LAMBDA_FN"
fi

log "Kinesis -> Lambda trigger (batches of up to 500 records / 30 s)"
if [ -z "$(aws lambda list-event-source-mappings --function-name "$LAMBDA_FN" --event-source-arn "$STREAM_ARN" --query 'EventSourceMappings[0].UUID' --output text | grep -v None)" ]; then
  aws lambda create-event-source-mapping --function-name "$LAMBDA_FN" --event-source-arn "$STREAM_ARN" \
    --starting-position LATEST --batch-size 500 --maximum-batching-window-in-seconds 30 >/dev/null
fi

log "Alarm on Lambda errors"
aws cloudwatch put-metric-alarm --alarm-name "${PROJECT}-stream-lambda-errors" \
  --namespace AWS/Lambda --metric-name Errors --dimensions Name=FunctionName,Value="$LAMBDA_FN" \
  --statistic Sum --period 300 --evaluation-periods 1 --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold --treat-missing-data notBreaching --alarm-actions "$TOPIC_ARN"

log "Done. Send events with:"
echo "  python streaming/kinesis_producer.py --stream ${STREAM} --region ${AWS_REGION} --limit 5000"
