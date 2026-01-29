#!/bin/bash
source "$(dirname "$0")/log.sh"

# Function to check if a Google Cloud Storage bucket exists and create it if it doesn't
create_bucket_if_not_exists() {
  local bucket_name=$1

  if [[ -z "$bucket_name" ]]; then
    log_error "Bucket name is required"
    return 1
  fi

  # Check if the bucket exists
  if gsutil -1 ls -b "gs://${bucket_name}" 2> /dev/null; then
    log_info "Bucket ${bucket_name} already exists."
  else
    # Create the bucket
    log_info "Creating bucket ${bucket_name}..."
    if gsutil -q mb -l EU "gs://${bucket_name}"; then
      log_info "Bucket ${bucket_name} created successfully."
    else
      log_error "Failed to create bucket ${bucket_name}."
      return 1
    fi
  fi
}

# Function to delete a Google Cloud Storage bucket if it exists
delete_bucket_if_exists() {
  local bucket_name=$1

  if [[ -z "$bucket_name" ]]; then
    log_error "Bucket name is required"
    return 1
  fi

  # Check if the bucket exists
  if gsutil -q ls -b "gs://${bucket_name}" 2> /dev/null; then
    # Delete the bucket
    log_info "Deleting bucket ${bucket_name}..."
    if gcloud -q storage rm --recursive "gs://${bucket_name}" > /dev/null 2>&1; then
      log_info "Bucket ${bucket_name} deleted successfully."
    else
      log_error "Failed to delete bucket ${bucket_name}."
      return 1
    fi
  else
    log_info "Bucket ${bucket_name} does not exist."
  fi
}

# Function to upload a file to a Google Cloud Storage bucket with a target filename
upload_file_to_bucket() {
  local bucket_name=$1
  local file_path=$2
  local target_filename=$3

  if [[ -z "$bucket_name" || -z "$file_path" || -z "$target_filename" ]]; then
    log_error "Bucket name, file path, and target filename are required"
    return 1
  fi

  if [[ ! -f "$file_path" ]]; then
    log_error "File $file_path does not exist"
    return 1
  fi

  log_info "Uploading file $file_path to bucket $bucket_name as $target_filename..."
  if gsutil -q cp "$file_path" "gs://${bucket_name}/${target_filename}"; then
    log_info "File $file_path uploaded successfully to bucket $bucket_name as $target_filename."
  else
    log_error "Failed to upload file $file_path to bucket $bucket_name as $target_filename."
    return 1
  fi
}

# Function to check if a file exists in a Google Cloud Storage bucket
file_exists_in_bucket() {
  local bucket_name=$1
  local target_filename=$2

  if [[ -z "$bucket_name" || -z "$target_filename" ]]; then
    log_error "Bucket name and target filename are required"
    return 1
  fi

  log_info "Checking if file $target_filename exists in bucket $bucket_name..."
  if gsutil ls "gs://${bucket_name}/${target_filename}" 2> /dev/null; then
    log_info "File $target_filename exists in bucket $bucket_name."
    return 0
  else
    log_info "File $target_filename does not exist in bucket $bucket_name."
    return 1
  fi
}
