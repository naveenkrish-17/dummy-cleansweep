#!/bin/bash
source "$(dirname "$0")/log.sh"

# Function to deploy a Cloud Run job
deploy_cloud_run_job() {
  local project_id=$1
  local region=$2
  local service_name=$3
  local image_uri=$4

  if [[ -z "$project_id" || -z "$region" || -z "$service_name" || -z "$image_uri" ]]; then
    log_error "Project ID, region, service name, and image URI are required"
    return 1
  fi

  # create temp config file
  TEMP_DIR=$(mktemp -d)
  if [[ $? -ne 0 ]]; then
    log_error "Failed to create temp directory"
    return 1
  fi
  TEMP_CONFIG_FILE="${TEMP_DIR}/${RUN_UUID}_config.yml"
  create_config $service_name $image_uri > $TEMP_CONFIG_FILE

  log_info "Temp config file created at $TEMP_CONFIG_FILE"

  log_info "Deploying Cloud Run job $service_name in project $project_id, region $region with image $image_uri..."

  gcloud -q run jobs replace $TEMP_CONFIG_FILE --project ${project_id} --region ${region} > /dev/null 2>&1
  if [[ $? -eq 0 ]]; then
    log_info "Cloud Run job $service_name deployed successfully."
  else
    log_error "Failed to deploy Cloud Run job $service_name."
    return 1
  fi
}

create_config() {
  local service_name=$1
  local image_uri=$2

  CONFIG_CONTENT="---
  apiVersion: run.googleapis.com/v1
  kind: Job
  metadata:
    name: ${service_name}
  spec:
    template:
      metadata:
        annotations:
          run.googleapis.com/vpc-access-egress: all-traffic
          run.googleapis.com/execution-environment: gen2
          run.googleapis.com/vpc-access-connector: projects/${PROJECT_ID}/locations/${REGION}/connectors/grp-cec-kosm-${REGION}
      spec:
        taskCount: 1
        template:
          spec:
            containers:
              - image: ${image_uri}
                command: python
                args:
                  - -m
                env:
                  - name: env_name
                    value: dev
                  - name: platform
                    value: kosmo
                  - name: azure_client_id
                    valueFrom:
                      secretKeyRef:
                        key: latest
                        name: AZURE_CLIENT_ID
                  - name: azure_client_secret
                    valueFrom:
                      secretKeyRef:
                        key: latest
                        name: AZURE_CLIENT_SECRET
                  - name: azure_tenant_id
                    valueFrom:
                      secretKeyRef:
                        key: latest
                        name: AZURE_TENANT_ID
                  - name: slack_bot_token
                    valueFrom:
                      secretKeyRef:
                        key: latest
                        name: CLEANSWEEP_SLACK_BOT_TOKEN
                  - name: env_id
                    value: ${RUN_UUID}
                  - name: azure_scope
                    value: api://032973cf-8723-49c8-8b43-98614afcee26/.default
                  - name: openai_api_base
                    value: https://apim-openai6t1p642f.azure-api.net
                  - name: openai_api_version
                    value: '2024-02-01'
                resources:
                  limits:
                    cpu: 4000m
                    memory: 16Gi
            maxRetries: 0
            timeoutSeconds: '36000'
            serviceAccountName: contentpipeline-etl-run@${PROJECT_ID}.iam.gserviceaccount.com
  "
  echo "$CONFIG_CONTENT"
}


# Function to delete a Cloud Run job
delete_cloud_run_job() {
  local project_id=$1
  local region=$2
  local service_name=$3

  if [[ -z "$project_id" || -z "$region" || -z "$service_name" ]]; then
    log_error "Project ID, region, and service name are required"
    return 1
  fi

  log_info "Deleting Cloud Run job $service_name in project $project_id, region $region..."

  gcloud run jobs delete "$service_name" \
    --project ${project_id} \
    --region "$region" \
    --quiet > /dev/null 2>&1

  if [[ $? -eq 0 ]]; then
    log_info "Cloud Run job $service_name deleted successfully."
  else
    log_error "Failed to delete Cloud Run job $service_name."
    return 1
  fi
}