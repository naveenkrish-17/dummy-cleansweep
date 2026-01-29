#!/bin/bash
source ./scripts/log.sh
source ./scripts/gh.sh

if ! command -v jq >/dev/null 2>&1; then
    log_info "installing jq"
    apt install jq -qqy
fi


if ! command -v uuidgen >/dev/null 2>&1; then
    log_info "installing uuid-runtime"
    apt install uuid-runtime -qqy
fi

run() {
  context="continuous-integration/smoke-test:${NAME}"

  log_info "Setting commit status to pending"
  submit_commit_status -r ${REPO_NAME} \
    -o sky-uk \
    -u ${GIT_USER} \
    -t ${GIT_TOKEN} \
    -s pending \
    -c ${context} \
    -d "${DESCRIPTION}"
  log_info "Commit status set to pending"
  log_info "Running simple ${NAME} tests"
  if ! ./scripts/test_runner.sh -c ${TEST_CONFIG}
  then
    log_error "Tests failed"
    state="failure"
    if [[ -n $ISSUE_TITLE && -n $ISSUE_BODY ]]
    then
      submit_issue -r ${REPO_NAME} \
        -o sky-uk \
        -u ${GIT_USER} \
        -t ${GIT_TOKEN} \
        -l bug \
        -c "${ISSUE_BODY}" \
        -i "${ISSUE_TITLE}"
    fi
  else
    log_info "Tests passed"
    state="success"
  fi

  submit_commit_status -r ${REPO_NAME} \
    -o sky-uk \
    -u ${GIT_USER} \
    -t ${GIT_TOKEN} \
    -s ${state} \
    -c ${context}

}


# region help

print_help() {
  echo -e "Usage: $0 [-h] [-c TEST_CONFIG]"
  echo -e "  -h\tPrint this help message"
  echo -e "  -c\tThe test configuration to use"
}

if [[ "$1" == "-h" ]]
then
  print_help
  exit 0
fi

# endregion

# region variables

export TEST_CONFIG
while [ $# -gt 0 ]
do
  case $1 in
    -h)
      print_help
      exit 0
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

export NAME=$(jq -r '.name' "$TEST_CONFIG")
NAME=${NAME// /-}
export DESCRIPTION=$(jq -r '.description' "$TEST_CONFIG")

# endregion
log_info "Running tests for ${NAME}"
run
log_info "Tests for ${NAME} completed"