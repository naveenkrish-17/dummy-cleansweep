#!/usr/bin/env bash
set -euo pipefail

JOB_NAME="$1"
PROJECT_ID="$2"
REGION="$3"
ENV_FILE="$4"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Env file $ENV_FILE not found"
  exit 2
fi

# Read env file and assemble comma-separated KEY=VALUE list for gcloud
ENV_VARS=$(awk 'NF {printf sep $0; sep=","}' "$ENV_FILE")

echo "Triggering Cloud Run job $JOB_NAME in project $PROJECT_ID region $REGION with env vars from $ENV_FILE"

# Use gcloud to execute a job and pass env vars. This requires gcloud installed and authenticated.
# The exact flags supported may vary by gcloud version. Adjust as necessary.
gcloud run jobs execute "$JOB_NAME" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --set-env-vars="$ENV_VARS" \
  --wait

echo "Execution requested for $JOB_NAME"
