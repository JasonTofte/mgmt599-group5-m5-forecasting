#!/usr/bin/env bash
# ============================================================================
# upload_to_s3.sh: one-time ingestion of the M5 CSVs into the raw zone
# MGMT 599 Group 5
# ============================================================================
# Use the CLI, not the browser console: sell_prices.csv is ~200 MB and browser
# uploads to S3 stall or time out.
#
#   ./upload_to_s3.sh /path/to/m5-forecasting-accuracy
# ============================================================================
set -euo pipefail

BUCKET="${BUCKET:-mgmt599-group5-m5}"
REGION="${REGION:-us-east-1}"
DATA_DIR="${1:-.}"

echo "==> checking credentials"
aws sts get-caller-identity

echo "==> creating bucket s3://${BUCKET} (ignore error if it already exists)"
if [ "$REGION" = "us-east-1" ]; then
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null || true
else
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION" 2>/dev/null || true
fi

# Block public access. Good practice and a governance point for the report
aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration \
  "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

echo "==> creating zone prefixes"
for zone in raw processed curated athena-results; do
  aws s3api put-object --bucket "$BUCKET" --key "${zone}/" >/dev/null
  echo "    ${zone}/"
done

echo "==> uploading CSVs from ${DATA_DIR}"
for f in calendar.csv sell_prices.csv sales_train_validation.csv \
         sales_train_evaluation.csv sample_submission.csv; do
  if [ -f "${DATA_DIR}/${f}" ]; then
    aws s3 cp "${DATA_DIR}/${f}" "s3://${BUCKET}/raw/${f}"
  else
    echo "    WARNING: ${f} not found in ${DATA_DIR}"
  fi
done

echo "==> raw zone contents (screenshot this)"
aws s3 ls "s3://${BUCKET}/raw/" --human-readable --summarize

echo "==> done. Next: run the Glue job, then the crawler."
