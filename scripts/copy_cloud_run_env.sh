#!/bin/bash
# region imports
# Source required scripts

source "$(dirname "$0")/colours.sh"
source "$(dirname "$0")/log.sh"
source "$(dirname "$0")/gcs.sh"
source "$(dirname "$0")/cloud_run.sh"

# endregion

# region help

print_help() {
  echo -e "Usage: $0 [-h] [EXECUTION_NAME] [PROJECT]"
  echo -e "  -h            \tPrint this help message"
  echo -e "  EXECUTION_NAME\tThe name of the Cloud Run execution to copy the environment from"
  echo -e "  PROJECT       \tThe Google Cloud project ID"
}

if [[ "$1" == "-h" ]]
then
  print_help
  exit 0
fi

# endregion

EXECUTION_NAME="$1"
PROJECT="$2"

if [[ -z "$EXECUTION_NAME" || -z "$PROJECT" ]]
then
  print_help
  exit 1
fi

temp_dir="$(mktemp -d)"

# copy the execution variable and write it to a temp file
gcloud run jobs executions describe "$EXECUTION_NAME" --format yaml --region=europe-west1 --project="$PROJECT" >> "${temp_dir}/env.yml"
if [[ $? -ne 0 ]]
then
  log_error "Failed to describe Cloud Run execution: $EXECUTION_NAME"
  rm -rf "$temp_dir"
  exit 1
fi

log_info "Environment variables copied to: ${temp_dir}/env.yml"
if python scripts/read_yaml.py "${temp_dir}/env.yml"
then
  log_info "Successfully read environment variables"
else
  log_error "Failed to read environment variables"
fi

# remove the temporary directory
rm -rf "$temp_dir"
