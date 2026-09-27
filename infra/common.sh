#!/usr/bin/env bash
# Shared settings for deploy/teardown scripts. Sourced, not executed.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
[ -f config.env ] || { echo "config.env not found - copy config.env.example to config.env and edit it"; exit 1; }
# shellcheck disable=SC1091
source config.env
: "${AWS_REGION:?set AWS_REGION in config.env}" "${BUCKET:?set BUCKET in config.env}"
PREFIX="${PREFIX:-retail-bank}"
PROJECT="${PROJECT:-retail-bank}"
export AWS_DEFAULT_REGION="$AWS_REGION" AWS_PAGER=""

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
DATABASE="retail_bank_analytics"
GLUE_ROLE="${PROJECT}-glue-role"
SFN_ROLE="${PROJECT}-sfn-role"
LAMBDA_ROLE="${PROJECT}-lambda-role"
GLUE_JOB="${PROJECT}-etl"
CRAWLER="${PROJECT}-curated-crawler"
STATE_MACHINE="${PROJECT}-pipeline"
WORKGROUP="${PROJECT}-wg"
TOPIC="${PROJECT}-alerts"
STREAM="${PROJECT}-transactions"
LAMBDA_FN="${PROJECT}-stream-consumer"
TOPIC_ARN="arn:aws:sns:${AWS_REGION}:${ACCOUNT_ID}:${TOPIC}"
SM_ARN="arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:${STATE_MACHINE}"
S3_BASE="s3://${BUCKET}/${PREFIX}"
BUILD_DIR="infra/.build"
mkdir -p "$BUILD_DIR"

render() {  # render <template> <output>: replace __TOKENS__ with values
  sed -e "s|__BUCKET__|${BUCKET}|g" -e "s|__PREFIX__|${PREFIX}|g" \
      -e "s|__REGION__|${AWS_REGION}|g" -e "s|__ACCOUNT_ID__|${ACCOUNT_ID}|g" \
      -e "s|__GLUE_JOB__|${GLUE_JOB}|g" -e "s|__CRAWLER__|${CRAWLER}|g" \
      -e "s|__DATABASE__|${DATABASE}|g" -e "s|__WORKGROUP__|${WORKGROUP}|g" \
      -e "s|__TOPIC_ARN__|${TOPIC_ARN}|g" "$1" > "$2"
}

ensure_role() {  # ensure_role <name> <trust-policy-file>
  if ! aws iam get-role --role-name "$1" >/dev/null 2>&1; then
    aws iam create-role --role-name "$1" --assume-role-policy-document "file://$2" >/dev/null
    echo "  created role $1 (waiting for IAM propagation)"; sleep 15
  fi
}

log() { printf '\n==> %s\n' "$*"; }
