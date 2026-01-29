#!/bin/bash
# region functions
# Function to check if running in Google Cloud Build environment
is_gcloud_build() {
  if [[ -n "${BUILD_ID}" ]]; then
    return 0
  else
    return 1
  fi
}

init_test_config() {

  log_info "Running tests with configuration $TEST_CONFIG"

  export NAME=$(jq -r '.name' "$TEST_CONFIG")
  export CONFIG=$(jq -r '.config' "$TEST_CONFIG")
  export DATA=$(jq -r '.data' "$TEST_CONFIG")
  export TARGET_BUCKET=$(jq -r '.target_bucket' "$TEST_CONFIG")
  export TARGET_DIRECTORY=$(jq -r '.target_directory' "$TEST_CONFIG")
  export DATA_TYPE=file
  export MAPPING=$(jq -r '.mapping' "$TEST_CONFIG")
  export PLUGIN=$(jq -r '.plugin' "$TEST_CONFIG")
  export STEPS=$(jq -c '.steps' "$TEST_CONFIG")
  export PLATFORM=$(jq -r '.platform' "$TEST_CONFIG")
  export RUN_UUID=$(echo -n "$(uuidgen)" | shasum | cut -c 1-7)
  export CLOUD_RUN_JOB="cleansweep-${ENV_NAME}${RUN_UUID}"

  log_info "Name: $NAME"

  if [[ -z $RUN_UUID ]]
  then
    log_error "Failed to generate run UUID"
    exit 1
  fi

  log_info "Run UUID: $RUN_UUID"

  if [[ -z $CONFIG ]]
  then
    log_info "No configuration provided"
  else
    CONFIG="$(pwd)/smoke_tests/config/${CONFIG}"
    log_info "Configuration: $CONFIG"
  fi

  if [[ ! -f $CONFIG ]]
  then
    log_error "Configuration file not found"
    exit 1
  fi

  if [[ $CONFIG == "null" || -z $CONFIG ]]
  then
    log_error "Configuration file not provided"
    exit 1
  fi

  if [[ -z $STEPS || $STEPS == "null" ]]
  then
    log_error "No steps provided"
    exit 1
  fi

  if [[ $PLUGIN == "null" ]]
  then
    PLUGIN=
  fi

  if [[ $MAPPING == "null" ]]
  then
    MAPPING=
  fi

  if [[ $DATA == "null" ]]
  then
    DATA=
  fi

  if [[ -n $DATA ]]
  then
    DATA="$(pwd)/smoke_tests/data/${DATA}"
    log_info "Data: $DATA"
  fi

  if [[ -d $DATA ]]
  then
    log_info "Data is a directory"
    DATA_TYPE=directory
  else
    if [[ -n $DATA && ! -f $DATA ]]
    then
      log_error "Data file not found"
      exit 1
    fi
  fi

  if [[ -n $PLUGIN ]]
  then
    PLUGIN="$(pwd)/smoke_tests/plugins/${PLUGIN}"
    log_info "Plugin: $PLUGIN"
  fi

  if [[ -n $PLUGIN && ! -f $PLUGIN ]]
  then
    log_error "Plugin file not found"
    exit 1
  fi

  if [[ -n $MAPPING ]]
  then
    MAPPING="$(pwd)/smoke_tests/mapping/${MAPPING}"
    log_info "Mapping: $MAPPING"
  fi

  if [[ -n $MAPPING && ! -f $MAPPING ]]
  then
    log_error "Mapping file not found"
    exit 1
  fi

  if [[ -z $TARGET_DIRECTORY || $TARGET_DIRECTORY == "null" ]]
  then
    TARGET_DIRECTORY="landing"
  fi
  log_info "Target directory: $TARGET_DIRECTORY"

}

init_buckets() {
  # create local name variable with spaces replaced with dashes
  local name=$(echo "$NAME" | tr ' ' '-')
  export LANDING_BUCKET="skygenai-uk-lan-${PLATFORM}-${name}-ir-${ENV_NAME}${RUN_UUID}"
  export STAGING_BUCKET="skygenai-uk-stg-${PLATFORM}-${name}-ir-${ENV_NAME}${RUN_UUID}"
  export UTILITY_BUCKET="skygenai-uk-utl-${PLATFORM}-composer-ir-${ENV_NAME}${RUN_UUID}"

  log_info "Landing bucket: $LANDING_BUCKET"
  if ! create_bucket_if_not_exists "$LANDING_BUCKET"; then
    log_error "Failed to create landing bucket"
    exit 1
  fi

  log_info "Staging bucket: $STAGING_BUCKET"
  if ! create_bucket_if_not_exists "$STAGING_BUCKET"; then
    log_error "Failed to create staging bucket"
    exit 1
  fi

  log_info "Staging bucket: $UTILITY_BUCKET"
  if ! create_bucket_if_not_exists "$UTILITY_BUCKET"; then
    log_error "Failed to create utility bucket"
    exit 1
  fi

}

tear_down_buckets() {
  log_info "Tearing down buckets..."

  log_info "Deleting landing bucket..."
  delete_bucket_if_exists "$LANDING_BUCKET"

  log_info "Deleting staging bucket..."
  delete_bucket_if_exists "$STAGING_BUCKET"

  log_info "Deleting utility bucket..."
  delete_bucket_if_exists "$UTILITY_BUCKET"
}

run_step() {
  local step=$1
  local step_name=$(echo $step | jq -r '.name')
  local step_description=$(echo $step | jq -r '.description')
  local step_command=$(echo $step | jq -r '.command')
  local step_env_vars=$(echo $step | jq -r '.env_args')
  var_string="run_id=${RUN_UUID},config_file_uri=${CONFIG_URI}"
  if [[ -n $step_env_vars && $step_env_vars != "null" ]]
  then
    while read -r key value; do
      var_string+=",$key=$value"
    done <<< "$(echo "$step_env_vars" | jq -r 'to_entries[] | "\(.key)=\(.value)"')"
  fi

  local step_check_file=$(echo $step | jq -r '.check_file')
  local step_target_dir=$(echo $step | jq -r '.target_dir')
  local step_target_extension=$(echo $step | jq -r '.target_extension')
  ISSUE_TITLE="Smoke test failure: $step_name"
  ISSUE_BODY="
# Smoke test failure

<table>
<tbody>
<tr><td><strong>Step</strong></td><td>$step_name</td></tr>
<tr><td><strong>Run Id</strong></td><td>$RUN_UUID</td></tr>
<tr><td><strong>Version</strong></td><td>$TAG_NAME</td></tr>
</tbody>
</table>

## Logs
[here](https://console.cloud.google.com/cloud-build/builds;region=$REGION/$BUILD_ID?project=$PROJECT_ID)

"

  log_info "Running step: $step_name - $step_description"

  gcloud run jobs execute ${CLOUD_RUN_JOB} --project ${PROJECT_ID} \
    --region ${REGION} --args="-m,app.${step_command}" \
    --update-env-vars="${var_string}" --wait &> /dev/null

  if [[ $? -eq 0 ]]
  then
    log_info "Step $step_name completed successfully"
  else
    log_error "Step $step_name failed"
    export ISSUE_TITLE
    export ISSUE_BODY
    return 1
  fi

  if [[ -n $step_check_file && $step_check_file == "true" ]]
  then
    FILE_NAME_WITHOUT_EXTENSION=$(basename "$DATA" | cut -d '.' -f 1)
    EXTENSION=$(basename "$DATA" | rev | cut -d '.' -f 1 | rev)
    TARGET_FILE="${step_target_dir}/*_${RUN_UUID}.${step_target_extension}"

    log_info "Checking file $TARGET_FILE in bucket $STAGING_BUCKET"
    if ! file_exists_in_bucket "$STAGING_BUCKET" "$TARGET_FILE"
    then
      log_error "File $TARGET_FILE not found in bucket $STAGING_BUCKET"
      export ISSUE_TITLE
      export ISSUE_BODY
      return 1
    fi
  fi
}

# endregion

# region imports
# Source required scripts
# Call the function
if ! is_gcloud_build
then
  source "$(dirname "$0")/colours.sh"
fi

source "$(dirname "$0")/log.sh"
source "$(dirname "$0")/gcs.sh"
source "$(dirname "$0")/cloud_run.sh"

# endregion

# region help

print_help() {
  echo -e "Usage: $0 [-h] [-t TAG] [-c TEST_CONFIG]"
  echo -e "  -h\tPrint this help message"
  echo -e "  -t\tThe tag to use for the image"
  echo -e "  -c\tThe test configuration to use"
}

if [[ "$1" == "-h" ]]
then
  print_help
  exit 0
fi

# endregion

# region variables

if [[ -z $ENV_NAME ]]
then
  export ENV_NAME="dev"
fi

if [[ -z $PROJECT_ID ]]
then
  export PROJECT_ID="grp-cec-kosmo-dev"
fi

if [[ -z $TAG_NAME ]]
then
  export TAG_NAME="latest"
fi

export TEST_CONFIG
while [ $# -gt 0 ]
do
  case $1 in
    -h)
      print_help
      exit 0
      ;;
    -t)
      shift
      TAG_NAME=$1
      ;;
    -c)
      shift
      TEST_CONFIG=$1
      ;;
    *)
      log_error "Invalid argument $1"
      print_help
      exit 1
      ;;
  esac
  shift
done

IMAGE_URI="europe-west1-docker.pkg.dev/${PROJECT_ID}/kosmo-${ENV_NAME}-artifact-registry/cleansweep:${TAG_NAME}"
REGION="europe-west1"
ERR=0

if [[ -z $TEST_CONFIG ]]
then
  log_error "No test configuration provided"
  exit 1
fi

if [[ ! -f $TEST_CONFIG ]]
then
  log_error "Test configuration file not found"
  exit 1
fi

if ! jq -e . $TEST_CONFIG > /dev/null
then
  log_error "Invalid JSON in test configuration file"
  exit 1
fi

# endregion

init_test_config

log_info "Running tests..."

# create buckets
if ! init_buckets; then
  log_error "Failed to create buckets"
  exit 1
fi

# set TARGET_BUCKET if empty - default to LANDING_BUCKET
if [[ -z $TARGET_BUCKET || $TARGET_BUCKET == "null" ]]
then
  TARGET_BUCKET=$LANDING_BUCKET
elif [[ $TARGET_BUCKET == "STAGING" ]]
then
  TARGET_BUCKET=$STAGING_BUCKET
elif [[ $TARGET_BUCKET == "LANDING" ]]
then
  TARGET_BUCKET=$LANDING_BUCKET
else
  log_error "Invalid target bucket: $TARGET_BUCKET"
  exit 1
fi
log_info "Target bucket: $TARGET_BUCKET"

################################################
# Starting to interact with GCS so don't exit
# on error, set ERR=1 and exit at the end with
# cleanup
################################################

# copy config to utility
CONFIG_TARGET="cleansweep/config/$(basename "$CONFIG")"
if ! upload_file_to_bucket $UTILITY_BUCKET $CONFIG $CONFIG_TARGET
then
  log_error "Failed to upload configuration file"
  ERR=1
fi
CONFIG_URI="gs://${UTILITY_BUCKET}/${CONFIG_TARGET}"

# upload plugin and mapping
if [[ -n $PLUGIN ]]
then
  PLUGIN_TARGET="cleansweep/plugins/$(basename "$PLUGIN")"
  if ! upload_file_to_bucket $UTILITY_BUCKET $PLUGIN $PLUGIN_TARGET
  then
    log_error "Failed to upload plugin file"
    ERR=1
  fi
  PLUGIN_URI="gs://${UTILITY_BUCKET}/${PLUGIN_TARGET}"
fi

if [[ -n $MAPPING ]]
then
  MAPPING_TARGET="cleansweep/mapping/$(basename "$MAPPING")"
  if ! upload_file_to_bucket $UTILITY_BUCKET $MAPPING $MAPPING_TARGET
  then
    log_error "Failed to upload mapping file"
    ERR=1
  fi
  MAPPING_URI="gs://${UTILITY_BUCKET}/${MAPPING_TARGET}"
fi

# copy data file to landing
if [[ -n $DATA && $DATA_TYPE == "file" ]]
then
  DATA_TARGET="${TARGET_DIRECTORY}/$(basename "$DATA")"
  if ! upload_file_to_bucket $TARGET_BUCKET $DATA "$DATA_TARGET"
  then
    log_error "Failed to upload data file"
    ERR=1
  fi
  DATA_URI="gs://${TARGET_BUCKET}/${DATA_TARGET}"
fi

# copy data directory to target
if [[ -n $DATA && $DATA_TYPE == "directory" ]]
then
  DATA_TARGET="${TARGET_DIRECTORY}/"

  # loop files in directory and upload each file
  for file in "$DATA"/*
  do
    if [[ -f $file ]]
    then
      file_name=$(basename "$file")
      DATA_TARGET="${TARGET_DIRECTORY}/${file_name}"
      if ! upload_file_to_bucket $TARGET_BUCKET "$file" "$DATA_TARGET"
      then
        log_error "Failed to upload data file"
        ERR=1
      fi
    fi
  done

  DATA_URI="gs://${TARGET_BUCKET}/${DATA_TARGET}"
fi

# deploy cloud run job
if ! deploy_cloud_run_job $PROJECT_ID $REGION $CLOUD_RUN_JOB $IMAGE_URI
then
  log_error "Failed to deploy cloud run job"
  ERR=1
fi

# run tests
if [[ $ERR -eq 0 ]]
then
  log_info "Running tests..."
  while read -r step; do
    if ! run_step "$step"
    then
      ERR=1
      break
    fi
  done <<< "$(echo "$STEPS" | jq -cr '.[]')"
else
  log_error "Skipping tests due to previous errors"
fi

if [[ $ERR -eq 0 ]]
then
  log_info "All tests passed"

  # cloud run tear down
  delete_cloud_run_job $PROJECT_ID $REGION $CLOUD_RUN_JOB

  # tear down buckets
  tear_down_buckets
else
  log_error "Some tests failed"
fi

exit $ERR