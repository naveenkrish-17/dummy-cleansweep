#!/bin/bash

log() {
  local datetime=$(date '+%Y-%m-%d %H:%M:%S')
  local severity="$1"
  local message="$2"
  echo -e "${datetime} ${severity} ${message}"
}

log_info() {
  log "${GREEN}INFO${RESET}" "$1"
}

log_warning() {
  log "${YELLOW}WARNING${RESET}" "$1"
}

log_error() {
  log "${RED}ERROR${RESET}" "$1"
}
