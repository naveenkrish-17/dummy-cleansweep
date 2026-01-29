"""CleanSweep DAG.

Public articles from One Help UK
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from hashlib import sha1

from airflow.models import BaseOperator
from airflow.models.dag import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)
from airflow.utils.decorators import apply_defaults
from google.cloud.storage import Client


def check_file_exists(bucket_name: str, match_glob: str):
    """Check if any file exists in a specified Google Cloud Storage bucket that matches a given glob pattern.

    Args:
        bucket_name (str): The name of the Google Cloud Storage bucket.
        match_glob (str): The glob pattern to match files against.

    Returns:
        bool: True if any file matching the glob pattern exists in the bucket, False otherwise.

    """
    client = Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(match_glob=match_glob)
    return any(blobs)


def get_run_id(run_id: str, dag_id: str | None = None) -> str:
    """Generate a unique 7-character run ID based on the SHA-1 hash of the input string.

    Args:
        run_id (str): The run identifier.
        dag_id (str | None): The DAG identifier. If provided, it will be appended to the run_id.
            If not provided, only the run_id will be used.

    Returns:
        str: A 7-character hexadecimal string derived from the SHA-1 hash of the input.
    """

    value = run_id
    if dag_id:
        value = f"{run_id}-{dag_id}"

    return sha1(value.encode("utf-8")).hexdigest()[:7]


class RunIdOperator(BaseOperator):
    """RunIdOperator is a custom Airflow operator that logs a specified `run_id` during task execution.

    Attributes:
        template_fields (tuple): A tuple containing the names of attributes that should be templated by Airflow.

        run_id (str): The run identifier to be logged during task execution.
        **kwargs: Additional keyword arguments passed to the BaseOperator.
    """

    template_fields = ("run_id",)

    @apply_defaults
    def __init__(
        self,
        *,
        run_id: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.run_id = run_id

    def execute(self, context):
        """Execute the main logic of the task instance.

        Args:
            context (dict): The execution context provided by Airflow, containing
                            runtime information such as task instance (ti).

        Logs:
            Logs the `run_id` if the task instance (ti) is available in the context.
        """
        ti = context.get("ti")
        if ti is not None:
            ti.log.info(f"run_id: {self.run_id}")


PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
if PROJECT_ID is None:
    raise ValueError("GOOGLE_CLOUD_PROJECT environment variable was not set.")
REGION = "europe-west1"
JOB_NAME = "cleansweep-dev"

with DAG(
    "contentstack-public",
    description="Public articles from One Help UK",
    tags=[
        "data territory: GB",
        "load type: DELTA",
        "platform: em",
        "platform: kosmo",
        "classification: PUBLIC",
    ],
    default_args={
        "deferrable": False,
        "owner": "Gen AI Delivery",
        "email": "DL-GenAI@sky.uk",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    max_active_runs=1,
    default_view="graph",
    owner_links={"Gen AI Delivery": "mailto:DL-GenAI@sky.uk"},
    schedule_interval="30 1 * * *",
    catchup=False,
    start_date=datetime(2026, 1, 5),
    user_defined_macros={"get_run_id": get_run_id},
) as dag:

    log_run_id = RunIdOperator(
        task_id="log_run_id",
        run_id="{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
        dag=dag,
    )

    em_gb_load_and_clean_load = CloudRunExecuteJobOperator(
        task_id="em-gb-load-and-clean-load",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.transform"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "em"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {"name": "mapping", "value": "content_stack_mapping.json"},
                        {"name": "source_path", "value": "$.articles"},
                        {"name": "plugin", "value": "contentstack_public_plugin.py"},
                        {
                            "name": "source",
                            "value": '{"bucket": "skyuk-uk-lan-kosmo-content-stack-ENV", "extension": "json", "directory": "public", "use_run_id": false}',
                        },
                    ],
                }
            ]
        },
    )

    em_gb_load_and_clean_metadata = CloudRunExecuteJobOperator(
        task_id="em-gb-load-and-clean-metadata",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.metadata"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "em"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {"name": "plugin", "value": "contentstack_public_plugin.py"},
                        {
                            "name": "source",
                            "value": '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "curated"}',
                        },
                    ],
                }
            ]
        },
    )

    em_gb_load_and_clean_clean = CloudRunExecuteJobOperator(
        task_id="em-gb-load-and-clean-clean",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.clean"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "em"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {
                            "name": "source",
                            "value": '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "metadata"}',
                        },
                        {
                            "name": "rules",
                            "value": '[{"rule": "filter out non-searchable", "type": "filter_by_column", "column": "id", "operator": "not in", "value": ["blt4907f09ceba3135a", "blta5a34d8b4f0b4d01", "blt475bf968ff6c8682", "bltc447a246664ebe5c", "blt7f5509e7b3f6924b", "blt622ba69fc5c93f1f"]}, {"rule": "filter out blank Articles", "type": "filter_by_column", "column": "length", "operator": ">", "value": 0}, {"rule": "Remove ROI articles", "type": "filter_by_column", "column": "metadata_region", "operator": "in", "value": "GB"}, {"rule": "Remove -roi- articles slug contains", "type": "filter_by_column", "column": "metadata_slug", "operator": "not in", "value": "-roi-"}, {"rule": "Remove roi articles slug ending", "type": "remove_by_match", "column": "metadata_slug", "value": "^.*-roi$"}, {"rule": "Remove versioned and test articles slug contains", "type": "remove_by_match", "column": "metadata_slug", "value": "-(?:v|t)[0-9]+"}, {"rule": "Remove ab articles slug endings", "type": "remove_by_match", "column": "metadata_slug", "value": "^.*-(?:b|m1)$"}, {"rule": "Remove old articles slug ending", "type": "remove_by_match", "column": "metadata_slug", "value": "^.*-old$"}, {"rule": "Remove duplicate articles", "type": "remove_duplicates", "columns": ["metadata_slug"], "order_by": "metadata_modified", "order": "desc"}, {"rule": "Remove articles with redirect", "type": "filter_by_column", "column": "metadata_should_redirect", "operator": "=", "value": false}, {"rule": "Remove articles containing test", "type": "remove_by_match", "column": "title", "value": "test"}, {"rule": "Remove substrings from title", "type": "remove_substrings", "columns": ["title"], "substrings": ["((?:\\\\[?(?:redirect(?:ed)?|(?:un)?searchable)\\\\]?)|v\\\\d|(?:ab | - |\\\\[)?test\\\\]?|\\\\[\\\\])"]}, {"rule": "Replace carriage returns", "type": "replace_substrings", "columns": ["content"], "substrings": ["\\r"], "replacement": "\\n"}, {"rule": "Remove substrings from content", "type": "remove_substrings", "columns": ["content"], "substrings": ["&nbsp;", "-{2,}"]}, {"rule": "Replace additional spaces", "type": "replace_substrings", "columns": ["content"], "substrings": [" {2,}"], "replacement": " "}, {"rule": "Replace superfluous returns", "type": "replace_substrings", "columns": ["content"], "substrings": ["\\n{3,}"], "replacement": "\\n\\n"}, {"rule": "Remove ID&V articles", "type": "remove_by_match", "column": "title", "value": "ID&V"}, {"rule": "Remove Atlas articles", "type": "remove_by_match", "column": "title", "value": "Atlas"}, {"rule": "Remove PRIVATE articles", "type": "remove_by_match", "column": "content_type", "value": "PRIVATE"}, {"rule": "refactor markdown links", "type": "reference_to_inline", "column": "content"}]',
                        },
                    ],
                }
            ]
        },
    )

    em_gb_load_and_clean_check_new_file_produced = ShortCircuitOperator(
        task_id="em-gb-load-and-clean-check-new-file-produced",
        dag=dag,
        python_callable=check_file_exists,
        op_args=[
            "skygenai-uk-stg-em-contentstack-public-ir-dev",
            "cleaned/*_{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}.avro",
        ],
    )

    kosmo_gb_process_kosmo_chunk = CloudRunExecuteJobOperator(
        task_id="kosmo-gb-process-kosmo-chunk",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.chunk"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "kosmo"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {
                            "name": "source",
                            "value": '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "cleaned"}',
                        },
                    ],
                }
            ]
        },
    )

    kosmo_gb_process_kosmo_check_new_file_produced = ShortCircuitOperator(
        task_id="kosmo-gb-process-kosmo-check-new-file-produced",
        dag=dag,
        python_callable=check_file_exists,
        op_args=[
            "skygenai-uk-stg-kosmo-contentstack-public-ir-dev",
            "chunked/*_{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}.avro",
        ],
    )

    kosmo_gb_process_kosmo_embed = CloudRunExecuteJobOperator(
        task_id="kosmo-gb-process-kosmo-embed",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.embed"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "kosmo"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {
                            "name": "source",
                            "value": '{"bucket": "skygenai-uk-stg-kosmo-contentstack-public-ir-ENV", "directory": "chunked"}',
                        },
                    ],
                }
            ]
        },
    )

    em_gb_process_em_chunk_semantic_chunk = CloudRunExecuteJobOperator(
        task_id="em-gb-process-em-chunk-semantic-chunk",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.semantic.chunk"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "em"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {"name": "model", "value": "gpt-4o"},
                        {
                            "name": "source",
                            "value": '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "cleaned"}',
                        },
                        {
                            "name": "semantic",
                            "value": '{"cluster_config": {"eps": 0.1, "min_samples": 2}}',
                        },
                    ],
                }
            ]
        },
    )

    em_gb_process_em_chunk_semantic_cluster = CloudRunExecuteJobOperator(
        task_id="em-gb-process-em-chunk-semantic-cluster",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.semantic.cluster"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "em"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {"name": "model", "value": "gpt-4o"},
                        {
                            "name": "source",
                            "value": '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "semantic/chunked"}',
                        },
                        {
                            "name": "semantic",
                            "value": '{"cluster_config": {"eps": 0.1, "min_samples": 2}}',
                        },
                    ],
                }
            ]
        },
    )

    em_gb_process_em_chunk_semantic_merge = CloudRunExecuteJobOperator(
        task_id="em-gb-process-em-chunk-semantic-merge",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.semantic.merge"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "em"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {"name": "model", "value": "gpt-4o"},
                        {
                            "name": "source",
                            "value": '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "semantic/clustered"}',
                        },
                        {
                            "name": "semantic",
                            "value": '{"cluster_config": {"eps": 0.1, "min_samples": 2}}',
                        },
                    ],
                }
            ]
        },
    )

    em_gb_process_em_chunk_semantic_validate = CloudRunExecuteJobOperator(
        task_id="em-gb-process-em-chunk-semantic-validate",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.semantic.validate"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "em"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {"name": "model", "value": "gpt-4o"},
                        {
                            "name": "source",
                            "value": '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "semantic/merged"}',
                        },
                        {
                            "name": "semantic",
                            "value": '{"cluster_config": {"eps": 0.1, "min_samples": 2}}',
                        },
                    ],
                }
            ]
        },
    )

    em_gb_process_em_apply_watson_validation = CloudRunExecuteJobOperator(
        task_id="em-gb-process-em-apply-watson-validation",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.run"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "em"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {"name": "plugin", "value": "watson_validate.py"},
                        {
                            "name": "source",
                            "value": '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "chunked"}',
                        },
                    ],
                }
            ]
        },
    )

    em_gb_process_em_apply_language_validation = CloudRunExecuteJobOperator(
        task_id="em-gb-process-em-apply-language-validation",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.run"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "em"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {"name": "plugin", "value": "language_validate.py"},
                        {
                            "name": "source",
                            "value": '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "watson_fixed"}',
                        },
                    ],
                }
            ]
        },
    )

    em_gb_process_em_apply_URL_check_and_metadata_dedupe = CloudRunExecuteJobOperator(
        task_id="em-gb-process-em-apply-URL-check-and-metadata-dedupe",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.run"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "em"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {"name": "plugin", "value": "contentstack_public_plugin.py"},
                        {
                            "name": "source",
                            "value": '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "language_fixed"}',
                        },
                    ],
                }
            ]
        },
    )

    em_gb_process_em_apply_hallucination_check = CloudRunExecuteJobOperator(
        task_id="em-gb-process-em-apply-hallucination-check",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.run"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "em"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {"name": "plugin", "value": "hallucination_check.py"},
                        {
                            "name": "source",
                            "value": '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "url_check"}',
                        },
                    ],
                }
            ]
        },
    )

    em_gb_process_em_apply_2nd_hallucination_check = CloudRunExecuteJobOperator(
        task_id="em-gb-process-em-apply-2nd-hallucination-check",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.run"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "em"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {"name": "plugin", "value": "hallucination_check_2.py"},
                        {
                            "name": "source",
                            "value": '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "hallucination_checked"}',
                        },
                    ],
                }
            ]
        },
    )

    em_gb_process_em_embed = CloudRunExecuteJobOperator(
        task_id="em-gb-process-em-embed",
        project_id=PROJECT_ID,
        region=REGION,
        job_name=JOB_NAME,
        dag=dag,
        overrides={
            "container_overrides": [
                {
                    "args": ["-m", "app.embed"],
                    "env": [
                        {
                            "name": "run_id",
                            "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
                        },
                        {"name": "name", "value": "contentstack public"},
                        {
                            "name": "description",
                            "value": "Public articles from One Help UK",
                        },
                        {"name": "classification", "value": "PUBLIC"},
                        {"name": "language", "value": "en"},
                        {"name": "load_type", "value": "DELTA"},
                        {"name": "platform", "value": "em"},
                        {"name": "schedule", "value": "30 1 * * *"},
                        {"name": "metadata__data_owner", "value": "DL-GenAI@sky.uk"},
                        {
                            "name": "metadata__data_owner_name",
                            "value": "Gen AI Delivery",
                        },
                        {"name": "metadata__data_territory", "value": "GB"},
                        {"name": "metadata__includes_protected", "value": "false"},
                        {"name": "metadata__includes_critical", "value": "false"},
                        {
                            "name": "metadata__owners",
                            "value": '[{"name": "Gen AI Delivery", "email": "DL-GenAI@sky.uk"}]',
                        },
                        {"name": "model", "value": "text-embedding-3-large"},
                        {"name": "dimensions", "value": "2000"},
                        {
                            "name": "source",
                            "value": '{"bucket": "skygenai-uk-stg-em-contentstack-public-ir-ENV", "directory": "hallucination_checked_2"}',
                        },
                    ],
                }
            ]
        },
    )

    em_gb_load_and_clean_load >> em_gb_load_and_clean_metadata
    em_gb_load_and_clean_metadata >> em_gb_load_and_clean_clean
    em_gb_load_and_clean_clean >> em_gb_load_and_clean_check_new_file_produced
    em_gb_load_and_clean_check_new_file_produced >> kosmo_gb_process_kosmo_chunk
    kosmo_gb_process_kosmo_chunk >> kosmo_gb_process_kosmo_check_new_file_produced
    kosmo_gb_process_kosmo_check_new_file_produced >> kosmo_gb_process_kosmo_embed
    (
        em_gb_load_and_clean_check_new_file_produced
        >> em_gb_process_em_chunk_semantic_chunk
    )
    em_gb_process_em_chunk_semantic_chunk >> em_gb_process_em_chunk_semantic_cluster
    em_gb_process_em_chunk_semantic_cluster >> em_gb_process_em_chunk_semantic_merge
    em_gb_process_em_chunk_semantic_merge >> em_gb_process_em_chunk_semantic_validate
    em_gb_process_em_chunk_semantic_validate >> em_gb_process_em_apply_watson_validation
    (
        em_gb_process_em_apply_watson_validation
        >> em_gb_process_em_apply_language_validation
    )
    (
        em_gb_process_em_apply_language_validation
        >> em_gb_process_em_apply_URL_check_and_metadata_dedupe
    )
    (
        em_gb_process_em_apply_URL_check_and_metadata_dedupe
        >> em_gb_process_em_apply_hallucination_check
    )
    (
        em_gb_process_em_apply_hallucination_check
        >> em_gb_process_em_apply_2nd_hallucination_check
    )
    em_gb_process_em_apply_2nd_hallucination_check >> em_gb_process_em_embed
    log_run_id >> em_gb_load_and_clean_load
