"""Module for merging question and answer pairs into a single question and answer pair."""

import asyncio
import logging
import re
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from cleansweep_core.chunk.semantic.merge import process_merge_results
from pydantic import BaseModel, Field

from cleansweep._types import MergeConfig
from cleansweep.chunk.semantic._utils import embed_dataframe
from cleansweep.chunk.semantic.cluster import (
    add_cluster_to_dataframe,
    cluster_question_answer_pairs,
    drop_cluster_columns,
)
from cleansweep.chunk.semantic.create import QuestionAnswerBase
from cleansweep.exceptions import PipelineError
from cleansweep.prompts.utils import create_prompt
from cleansweep.utils.azure.api import create_messages, process_api_calls
from cleansweep.utils.azure.auth import AzureCredentials
from cleansweep.utils.openai.chat import chat_completion_async
from cleansweep.utils.openai.tool import create_function, tool_choice

logger = logging.getLogger(__name__)


class MergedQuestion(QuestionAnswerBase):
    """A representation of the merged question and answer pair."""

    source_ids: list[str] = Field(
        ...,
        description=(
            "A list of all the distinct chunk ids that have "
            "been used to compose the final qestion and answer pair."
        ),
    )
    sufficient_ids: list[str] = Field(
        ...,
        description=(
            (
                "A subset of unique source_ids - up to 3 different ids - providing the MINIMUM "
                "source questions that can cover the major part of the question and answer pair"
            )
        ),
    )


class MergeQuestionsResponse(BaseModel, arbitrary_types_allowed=True):
    """A represenation of the required response schema."""

    items: list[MergedQuestion] = Field(
        description="List of merged question and answer pairs"
    )


def _categorise_cluster_size(df: pd.DataFrame, max_cluster_size: int) -> pd.DataFrame:
    """Categorise the clusters based on the size.

    Args:
        df (DataFrame): The DataFrame containing the question-answer pairs.
        max_cluster_size (int): The maximum size of a cluster.

    Returns:
        DataFrame: The DataFrame with an additional 'cluster_category' column indicating
            the size of the cluster.

    """
    size_map = df.groupby("cluster_id").size().to_dict()
    df["cluster_category"] = df["cluster_id"].apply(
        lambda x: ("large" if size_map.get(x, 0) > max_cluster_size else "small")
    )

    return df


def merge_clusters(
    df: pd.DataFrame,
    config: MergeConfig,
    temperature: float | None = None,
) -> pd.DataFrame:
    """Merge clusters in a DataFrame.

    Each cluster is merged to create a smaller cluster of consolidated question-answer pairs.

    Args:
        df (pd.DataFrame): The DataFrame containing the clusters to be merged.
        config (MergeConfig): The configuration for merging clusters.
        temperature (float, Optional): The temperature to use for the model. Defaults to None.

    Returns:
        pd.DataFrame: The DataFrame with merged clusters.

    Raises:
        None

    """
    return asyncio.run(amerge_clusters(df, config, temperature=temperature))


async def amerge_clusters(
    df: pd.DataFrame,
    config: MergeConfig,
    temperature: float | None = None,
) -> pd.DataFrame:
    """Merge clusters in a DataFrame.

    Each cluster is merged to create a smaller cluster of consolidated question-answer pairs.

    Args:
        df (pd.DataFrame): The DataFrame containing the clusters to be merged.
        config (MergeConfig): The configuration for merging clusters.
        temperature (float, Optional): The temperature to use for the model. Defaults to None.

    Returns:
        pd.DataFrame: The DataFrame with merged clusters.

    Raises:
        None

    """
    if df.empty is True:
        return df
    kwargs = {"temperature": temperature} if temperature is not None else {}

    credentials = config.credentials
    if credentials is None:
        credentials = AzureCredentials()

    # create tasks to process each cluster
    tasks = (
        df.groupby("cluster_id")[["question", "answer", "question_id"]]
        .apply(
            lambda x: create_prompt(
                prompt_dir=config.prompt_dir,
                template_name=config.merge_prompt.template,
                prompt=config.merge_prompt.prompt,
                qnas=x.to_dict(orient="records"),
            )
        )
        .apply(lambda x: create_messages(user_input=x))
    ).to_list()

    # process the tasks
    responses = await process_api_calls(
        chat_completion_async,
        tasks,
        "chat",
        config.model,
        credentials=credentials,
        tools=config.tools,
        tool_choice=config.tool_choice,
        ignore_refusals=True,
        **kwargs,
    )

    clusters = {}
    records = []
    for _id, frame in df.groupby("cluster_id"):
        clusters[_id] = {
            "cluster_action": frame["cluster_action"].iloc[0],
            "cluster_category": frame["cluster_category"].iloc[0],
            "cluster_uuid": frame["cluster_uuid"].iloc[0],
        }

        records.append(
            frame.to_dict(orient="records"),
        )

    processed_results = process_merge_results(responses, records, list(clusters.keys()))
    output_df = pd.DataFrame(processed_results)
    if output_df.empty is True:
        return output_df

    # add in the static cluster information
    for _id, cluster_values in clusters.items():
        for key, value in cluster_values.items():
            output_df.loc[output_df["cluster_id"] == _id, key] = value

    return output_df


def _identify_static_clusters(df: pd.DataFrame) -> list[Any]:

    new_clus = "cluster_id"
    old_clus = "prev_cluster_id"

    if old_clus not in df.columns:
        return []

    cond = (
        (df.groupby(new_clus)[old_clus].nunique() == 1)
        & (df.groupby(new_clus)[old_clus].count() > 1)
        & (pd.to_numeric(df[old_clus], errors="coerce").astype(int).min() >= 0)
    )

    return list(df.groupby(new_clus)[old_clus].count()[cond].index)


def recursive_merge(
    df: pd.DataFrame,
    config: MergeConfig,
    temperature: float | None = None,
) -> pd.DataFrame:
    """Recursively merge clusters in a DataFrame.

    Each iteration re-clusters the DataFrame on a gradually more relaxed epsilon value, merging any
    new sub-clusters that are formed.

    Args:
        df (pd.DataFrame): The DataFrame containing one cluster of question-answer pairs.
        config (RecursiveMergeConfig): The configuration for recursive merging.
        temperature (float, Optional): The temperature to use for the model. Defaults to None.

    Returns:
        pd.DataFrame: The merged DataFrame with clusters.

    Raises:
        None

    """
    return asyncio.run(arecursive_merge(df, config, temperature=temperature))


async def arecursive_merge(
    df: pd.DataFrame,
    config: MergeConfig,
    temperature: float | None = None,
) -> pd.DataFrame:
    """Recursively merge clusters in a DataFrame.

    Each iteration re-clusters the DataFrame on a gradually more relaxed epsilon value, merging any
    new sub-clusters that are formed.

    Args:
        df (pd.DataFrame): The DataFrame containing one cluster of question-answer pairs.
        config (RecursiveMergeConfig): The configuration for recursive merging.
        temperature (float, Optional): The temperature to use for the model. Defaults to None.

    Returns:
        pd.DataFrame: The merged DataFrame with clusters.

    Raises:
        None

    """

    def _concat_and_dedupe(df: list[pd.DataFrame]) -> pd.DataFrame:
        frame = pd.concat(df)
        frame.drop_duplicates(subset=["question_id", "source_id"], inplace=True)
        frame.reset_index(drop=True, inplace=True)
        return frame

    now = datetime.now()
    processed_clusters = []
    output_frames = []
    credentials = config.credentials
    cluster_id = df.iloc[0]["cluster_id"]
    last_eps = 0.0
    max_eps_since_last_cluster = 0.1
    original_size = df["question_id"].nunique()

    if "root_cluster_id" not in df.columns:
        df["root_cluster_id"] = df["cluster_id"]

    if credentials is None:
        credentials = AzureCredentials()
    i = 1

    dbscan_config = deepcopy(config.recursive_merge.cluster_model_config)
    dbscan_config.eps = config.recursive_merge.min_cluster_distance
    while True:
        for cdef in config.recursive_merge.cluster_definitions:
            # if any records have no embedding, embed them
            if cdef.cluster_name not in df.columns:
                df = await embed_dataframe(
                    df,
                    cdef,
                    config.recursive_merge.embedder_type,
                    config.recursive_merge.embedding_model,
                    config.recursive_merge.token_limit,
                )
            elif df[df[cdef.cluster_name].isnull()].empty is False:
                embedded_frame = await embed_dataframe(
                    df[df[cdef.cluster_name].isnull()],
                    cdef,
                    config.recursive_merge.embedder_type,
                    config.recursive_merge.embedding_model,
                    config.recursive_merge.token_limit,
                )

                embedded_frame = drop_cluster_columns(embedded_frame)

                # frame.update(embedded_frame)
                df = df.merge(
                    embedded_frame[["question_id", cdef.cluster_name]],
                    how="left",
                    left_on="question_id",
                    right_on="question_id",
                    suffixes=("", "_r"),
                )

                df[cdef.cluster_name] = df[cdef.cluster_name].combine_first(
                    df[f"{cdef.cluster_name}_r"]
                )
                df.drop([f"{cdef.cluster_name}_r"], axis=1, inplace=True)

            df, _ = add_cluster_to_dataframe(df, cdef, config=dbscan_config)

        df = cluster_question_answer_pairs(
            df, config.recursive_merge.cluster_definitions
        )
        processed_frames = []  # frames processed this iteration
        unprocessed_frames = []  # frames not processed this iteration

        # split frame into clustered and unclustered
        ucd = df[df["cluster_id"] == "-1"]
        ucd["cluster_category"] = "unclustered"

        # default `is_sufficient`
        if "is_sufficient" not in ucd.columns:
            ucd["is_sufficient"] = True

        cd = df[df["cluster_id"] != "-1"]

        if cd.empty is True:
            logger.debug("Cluster %s iteration %s: No clusters found", cluster_id, i)
            await asyncio.sleep(0.01)
        else:

            # recategorise clusters
            cd = _categorise_cluster_size(cd, config.recursive_merge.max_cluster_size)

            # split on category
            scd = cd[cd["cluster_category"] == "small"]
            lcd = cd[cd["cluster_category"] == "large"]
            if scd.empty is True:
                logger.debug(
                    "Cluster %s iteration %s: No small clusters found", cluster_id, i
                )
                output_frames.append(lcd)
                output_frames.append(ucd)

            else:
                if lcd.empty is False:
                    unprocessed_frames.append(lcd)
                # process clusters
                for _, group in scd.groupby("cluster_id"):
                    cluster_signature = set(group["question_id"].sort_values().values)
                    if cluster_signature in processed_clusters:
                        unprocessed_frames.append(group)
                        await asyncio.sleep(0.01)
                        continue

                    root_cluster_id = ":".join(
                        group.iloc[0][["root_cluster_id", "cluster_id"]].to_list()
                    )

                    # do merge
                    pr_frame = await amerge_clusters(
                        group,
                        config,
                        temperature=temperature,
                    )
                    if pr_frame.empty is True:
                        processed_frames.append(group)
                        await asyncio.sleep(0.01)
                        continue

                    pr_frame["root_cluster_id"] = root_cluster_id

                    processed_frames.append(pr_frame)
                    processed_clusters.append(cluster_signature)

        # check for static clusters
        static_clusters = _identify_static_clusters(df)
        unchanged = len(static_clusters) == df["cluster_id"].nunique()

        if not unchanged:
            last_eps = dbscan_config.eps

        # prepare for next iteration
        dbscan_config.eps += config.recursive_merge.step_size
        final_frames = [ucd]
        if processed_frames:
            final_frames.extend(processed_frames)
        if unprocessed_frames:
            final_frames.extend(unprocessed_frames)

        df = _concat_and_dedupe(final_frames)
        df["prev_cluster_id"] = df["cluster_id"]

        # check for termination conditions
        if unchanged and (dbscan_config.eps - last_eps) > max_eps_since_last_cluster:
            logger.debug(
                "Cluster %s iteration %s: Static clusters found", cluster_id, i
            )
            output_frames.append(df)
            break

        if dbscan_config.eps >= config.recursive_merge.max_cluster_distance:
            logger.debug("Cluster %s iteration %s: Max epsilon reached", cluster_id, i)
            output_frames.append(df)
            break
        await asyncio.sleep(0.01)

    final_df = _concat_and_dedupe(output_frames)

    final_df["cluster_id"] = final_df.apply(
        lambda x: f"{x['root_cluster_id']}:{x['cluster_id']}", axis=1
    )
    final_df.drop("root_cluster_id", axis=1, inplace=True)
    logger.info(
        "Cluster %s merged %s questions to %s with eps %.2f in %s",
        cluster_id,
        original_size,
        final_df["question_id"].nunique(),
        last_eps,
        timedelta(seconds=int((datetime.now() - now).total_seconds())),
    )

    return final_df


def merge_question_answer_pairs(
    df: pd.DataFrame, config: MergeConfig, temperature: float | None = None
) -> pd.DataFrame:
    """Merge question-answer pairs in a DataFrame based on clustering.

    Args:
        df (pd.DataFrame): The DataFrame containing question-answer pairs.
        config (MergeConfig): The configuration for merging.
        temperature (float, Optional): The temperature to use for the model. Defaults to None.

    Returns:
        pd.DataFrame: The merged DataFrame.

    Raises:
        PipelineError: If no 'cluster_id' column is found in the DataFrame.

    """
    if config.credentials is None:
        config.credentials = AzureCredentials()

    if "cluster_id" not in df.columns:
        raise PipelineError("No cluster_id column found in DataFrame to merge")

    # split the dataframe into clustered and unclustered
    unclustered_df = df[df["cluster_id"] == "-1"]
    clustered_df = df[df["cluster_id"] != "-1"]

    # categorise the clusters based on the size
    unclustered_df["cluster_category"] = "unclustered"
    clustered_df = _categorise_cluster_size(
        clustered_df, config.recursive_merge.max_cluster_size
    )

    # add default `is_sufficient` to unclustered
    unclustered_df["is_sufficient"] = True

    # split clustered into small and large clusters
    small_clustered_df = clustered_df[clustered_df["cluster_category"] == "small"]
    large_clustered_df = clustered_df[clustered_df["cluster_category"] == "large"]
    large_clustered_df["root_cluster_id"] = large_clustered_df["cluster_id"]

    logger.info(
        "%s unclustered records, %s small clusters, %s large clusters",
        len(unclustered_df),
        len(small_clustered_df["cluster_id"].unique()),
        len(large_clustered_df["cluster_id"].unique()),
    )

    tasks = []

    config.tools = [
        create_function(
            "merge_questions",
            "Merge questions.",
            MergeQuestionsResponse,
            strict=False,
        )
    ]
    config.tool_choice = tool_choice("merge_questions")

    class TempFilter(logging.Filter):
        """A filter for the logger."""

        def filter(self, record) -> bool:
            """Exclude async future task exceptions."""
            if re.search(r"Task exception was never retrieved", record.getMessage()):
                return False
            return True

    with ThreadPoolExecutor() as executor:
        _logger = logging.getLogger("asyncio")
        _temp_filter = TempFilter()
        _logger.addFilter(_temp_filter)
        # process small clusters
        if not small_clustered_df.empty:
            tasks.append(
                executor.submit(
                    merge_clusters, small_clustered_df, config, temperature=temperature
                )
            )

        for _, group in large_clustered_df.groupby("root_cluster_id"):
            tasks.append(
                executor.submit(recursive_merge, group, config, temperature=temperature)
            )

        done, _ = wait(tasks, return_when=ALL_COMPLETED)

        results = [task.result() for task in done]
        _logger.removeFilter(_temp_filter)

    results.append(unclustered_df)
    return pd.concat(results)
