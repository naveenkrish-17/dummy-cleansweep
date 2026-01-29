"""Cluster question-answer pairs based on embeddings."""

import asyncio
import logging
from hashlib import md5
from pathlib import Path
from tempfile import TemporaryDirectory

import joblib
import networkx as nx
import numpy as np
import pandas as pd
from google.cloud.storage import Bucket
from sklearn.cluster import DBSCAN

import cleansweep.utils.google.storage as gcs
from cleansweep._types import ClusterDefinition, DBScanConfig, Deployment
from cleansweep.chunk.semantic._utils import embed_dataframe
from cleansweep.enumerations import EmbedderType
from cleansweep.exceptions import PipelineError

logger = logging.getLogger(__name__)


def add_cluster_to_dataframe(
    df: pd.DataFrame,
    definition: ClusterDefinition,
    model: DBSCAN | None = None,
    config: DBScanConfig | None = None,
) -> tuple[pd.DataFrame, DBSCAN]:
    """Add cluster labels to a DataFrame using the DBSCAN algorithm.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be clustered.
        definition (ClusterDefinition): An object containing the cluster name and filter name.
        model (DBSCAN, optional): An existing DBSCAN model. If None, a new model will be created
            using the provided config.
        config (DBScanConfig, optional): Configuration for creating a new DBSCAN model if model is
            None.

    Returns:
        (tuple[pd.DataFrame, DBSCAN]): A tuple containing the updated DataFrame with cluster labels
            and the DBSCAN model used.

    Raises:
        PipelineError: If there is an error during clustering and the number of samples is
            sufficient for clustering.

    """
    if model is None:
        model = create_new_dbscan(config)

    np_embeddings = np.array(df[definition.cluster_name].to_list())
    logger.debug("Converted %s to numpy array", definition.cluster_name)

    df[definition.cluster_filter_name] = -1
    if len(np_embeddings) > 0:

        try:
            df[definition.cluster_filter_name] = model.fit_predict(np_embeddings)
        except ValueError as exc:
            logger.error("Error clustering %s: %s", definition.cluster_name, exc)

        logger.debug("Clustered %s using DBSCAN", definition.cluster_name)

    return df, model


def drop_cluster_columns(
    df: pd.DataFrame, cluster_definitions: list[ClusterDefinition] | None = None
) -> pd.DataFrame:
    """Drop the cluster columns from the given DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame from which to drop the cluster columns.
        cluster_definitions (list[ClusterDefinition] | None, optional):
            The list of cluster definitions. Defaults to None.

    Returns:
        pd.DataFrame: The DataFrame with the cluster columns dropped.

    """
    if cluster_definitions is None:
        cluster_definitions = []

    drop_columns = []
    for definition in cluster_definitions:
        drop_columns.append(definition.cluster_name)
        drop_columns.append(definition.cluster_filter_name)

    for col in ["vector_id", "text_to_embed"]:
        if col in df.columns:
            drop_columns.append(col)

    # drop the cluster columns
    df.drop(
        drop_columns,
        axis=1,
        inplace=True,
    )

    return df


def _split_dataframe_into_clustered_and_unclustered(
    df: pd.DataFrame,
    cluster_definitions: list[ClusterDefinition],
    id_column: str | None = "question_id",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a DataFrame into clustered and unclustered DataFrames based on cluster definitions.

    Args:
        df (pd.DataFrame): The input DataFrame containing data to be split.
        cluster_definitions (list[ClusterDefinition]): A list of ClusterDefinition objects that
            define the clustering criteria.
        id_column (str | None, optional): The column name to use as the identifier. Defaults to
            "question_id".

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing two DataFrames:
            - clustered_df: DataFrame containing rows that are clustered.
            - unclustered_df: DataFrame containing rows that are unclustered.

    Raises:
        PipelineError: If any of the cluster columns defined in cluster_definitions are not found in
            the DataFrame.

    """
    if id_column is None:
        id_column = "question_id"

    if any(
        (definition.cluster_name not in df.columns)
        for definition in cluster_definitions
    ):
        raise PipelineError("Cluster columns not found in DataFrame")

    unclustered_df = df[df[cluster_definitions[0].cluster_filter_name] == -1]
    for definition in cluster_definitions[1:]:
        unclustered_df = unclustered_df[
            unclustered_df[definition.cluster_filter_name] == -1
        ]

    unclustered_df["cluster_id"] = "-1"

    logger.debug("%s unclustered documents", len(unclustered_df))

    clustered_df = df[~df[id_column].isin(unclustered_df[id_column])]
    logger.debug("%s documents to be clustered", len(clustered_df))

    return clustered_df, unclustered_df


def cluster_question_answer_pairs(
    df: pd.DataFrame,
    cluster_definitions: list[ClusterDefinition],
    id_column: str | None = "question_id",
) -> pd.DataFrame:
    """Clusters question-answer pairs based on the provided cluster definitions.

    Args:
        df (pd.DataFrame): The DataFrame containing the question-answer pairs.
        cluster_definitions (list[ClusterDefinition]): A list of ClusterDefinition objects
            defining the clusters to be created.
        id_column (str | None, optional): The column name to use as the identifier. Defaults to
            "question_id".

    Returns:
        pd.DataFrame: The DataFrame with an additional 'cluster_id' column indicating
            the cluster assignment for each question-answer pair.

    """
    if id_column is None:
        id_column = "question_id"

    # create a graph of all connected components
    g = nx.Graph()
    for definition in cluster_definitions:
        logger.debug("Adding edges for %s", definition.cluster_name)
        clusters = df.groupby(definition.cluster_filter_name)

        for cluster_id, cluster in clusters:
            if cluster_id == -1:
                continue
            _ids = cluster[id_column].to_list()
            i = 0
            while i < len(_ids) - 1:
                g.add_edge(_ids[i], _ids[i + 1])
                i += 1

    final_clusters = list(nx.connected_components(g))

    df["cluster_id"] = "-1"
    for i, cluster in enumerate(final_clusters):
        df.loc[df[id_column].isin(cluster), "cluster_id"] = str(i)
    logger.debug("%s clusters found", len(final_clusters))
    return df


def create_new_dbscan(config: DBScanConfig | None = None) -> DBSCAN:
    """Create a new DBSCAN model instance.

    This function initializes a new DBSCAN model using the provided configuration.
    If no configuration is provided, a default DBScanConfig instance is used.

    Args:
        config (DBScanConfig | None): The configuration for the DBSCAN model. If None, a default configuration is used.

    Returns:
        DBSCAN: A new instance of the DBSCAN model.

    """
    if config is None:
        config = DBScanConfig()
    model = DBSCAN(**config.model_dump())
    logger.debug("Created new model")
    return model


def _get_model_blob_name(cluster_definition: ClusterDefinition) -> Path:
    return Path("semantic", "models", cluster_definition.cluster_name).with_suffix(
        ".pkl.gz"
    )


def load_cluster_model(
    cluster_definition: ClusterDefinition,
    model_store: Bucket | None = None,
    config: DBScanConfig = DBScanConfig(),
) -> DBSCAN:
    """Load a DBSCAN clustering model from a storage bucket or create a new one if it doesn't exist.

    Args:
        cluster_definition (ClusterDefinition): The definition of the cluster to be loaded.
        model_store (Bucket | None, optional): The storage bucket where the model is stored.
            Defaults to None.
        config (DBScanConfig, optional): Configuration for the DBSCAN model. Defaults to a new
            instance of DBScanConfig.

    Returns:
        DBSCAN: The loaded or newly created DBSCAN model.

    """
    if model_store is None:
        return create_new_dbscan(config)

    model_blob_name = _get_model_blob_name(cluster_definition)
    model_blob = model_store.blob(model_blob_name.as_posix())

    if model_blob.exists():
        with TemporaryDirectory() as temp_dir:
            temp_file = Path(temp_dir).joinpath(model_blob_name.name)
            model_blob.download_to_filename(temp_file)
            model = joblib.load(temp_file)
        logger.debug("Loaded model from storage")
    else:
        model = create_new_dbscan(config)

    return model


def save_cluster_model(
    model: DBSCAN,
    cluster_definition: ClusterDefinition,
    model_store: Bucket,
) -> None:
    """Save a DBSCAN clustering model to a cloud storage bucket.

    This function serializes the given DBSCAN model and uploads it to the specified
    cloud storage bucket. The model is saved as a compressed pickle file (.pkl.gz)
    with a name derived from the cluster definition.

    Args:
        model (DBSCAN): The DBSCAN clustering model to be saved.
        cluster_definition (ClusterDefinition): An object containing the cluster's
            metadata, including the cluster name.
        model_store (Bucket): The cloud storage bucket where the model will be saved.

    Returns:
        None

    """
    with TemporaryDirectory() as temp_dir:

        model_file = (
            Path(temp_dir)
            .joinpath(cluster_definition.cluster_name)
            .with_suffix(".pkl.gz")
        )
        model_blob_name = _get_model_blob_name(cluster_definition)
        joblib.dump(model, model_file)
        model_blob = model_store.blob(model_blob_name.as_posix())
        model_blob.upload_from_filename(model_file)
    logger.debug("Saved model to storage")


def create_clustered_dataframe(
    df: pd.DataFrame,
    embedder_type: EmbedderType,
    model: Deployment,
    cluster_definitions: list[ClusterDefinition],
    token_limit: int | None = None,
    store: str | Bucket | None = None,
    cluster_config: DBScanConfig = DBScanConfig(),
) -> pd.DataFrame:
    """Create a clustered dataframe by embedding columns and adding cluster information.

    Args:
        df (pd.DataFrame): The input dataframe.
        embedder_type (EmbedderType): The type of embedder to use for embedding.
        model (Deployment): The deployment model to use for embedding.
        cluster_definitions (list[ClusterDefinition]): The list of cluster definitions.
        token_limit (int | None, optional): The token limit for embedding. Defaults to None.
        store (str | Bucket | None, optional): The model store to save the cluster model.
            Defaults to None.
        cluster_config (DBScanConfig, optional): The DBScan configuration. Defaults to
            DBScanConfig().

    Returns:
        pd.DataFrame: The final clustered dataframe.

    """
    return asyncio.run(
        acreate_clustered_dataframe(
            df,
            embedder_type,
            model,
            cluster_definitions,
            token_limit=token_limit,
            store=store,
            cluster_config=cluster_config,
        )
    )


async def acreate_clustered_dataframe(
    df: pd.DataFrame,
    embedder_type: EmbedderType,
    model: Deployment,
    cluster_definitions: list[ClusterDefinition],
    token_limit: int | None = None,
    store: str | Bucket | None = None,
    cluster_config: DBScanConfig = DBScanConfig(),
) -> pd.DataFrame:
    """Create a clustered dataframe by embedding columns and adding cluster information.

    Args:
        df (pd.DataFrame): The input dataframe.
        embedder_type (EmbedderType): The type of embedder to use for embedding.
        model (Deployment): The deployment model to use for embedding.
        cluster_definitions (list[ClusterDefinition]): The list of cluster definitions.
        token_limit (int | None, optional): The token limit for embedding. Defaults to None.
        store (str | Bucket | None, optional): The model store to save the cluster model.
            Defaults to None.
        cluster_config (DBScanConfig, optional): The DBScan configuration. Defaults to
            DBScanConfig().

    Returns:
        pd.DataFrame: The final clustered dataframe.

    """
    if not isinstance(store, Bucket) and store is not None:
        client = gcs.fs()
        store = client.get_bucket(store)

    logger.debug("Prep the DataFrame for embeddings")
    # Embed the question and answer columns and create question and q and a clusters
    for definition in cluster_definitions:
        df = await embed_dataframe(
            df, definition, embedder_type, model, token_limit=token_limit
        )

        # check for missing embeddings

        df[definition.cluster_name].apply(
            lambda x: None if all(val == 0.0 for val in x) else x
        )
        missing_embeddings = df[df[definition.cluster_name].isnull()]

        while missing_embeddings.empty is False:

            logger.info("Found missing embeddings, retrying")

            if not isinstance(missing_embeddings, pd.DataFrame):
                missing_embeddings = pd.DataFrame(missing_embeddings)

            embedded_frame = await embed_dataframe(
                missing_embeddings,
                definition,
                embedder_type,
                model,
                token_limit=0,  # embed docs individually!
            )

            df = df.merge(
                embedded_frame[["question_id", definition.cluster_name]],
                how="left",
                left_on="question_id",
                right_on="question_id",
                suffixes=("", "_r"),
            )

            df[definition.cluster_name] = df[definition.cluster_name].combine_first(
                df[f"{definition.cluster_name}_r"]
            )
            df.drop([f"{definition.cluster_name}_r"], axis=1, inplace=True)

        dbscan = create_new_dbscan(cluster_config)

        df, dbscan = add_cluster_to_dataframe(df, definition, model=dbscan)

    clustered_df, unclustered_df = _split_dataframe_into_clustered_and_unclustered(
        df, cluster_definitions
    )
    clustered_df = cluster_question_answer_pairs(clustered_df, cluster_definitions)

    logger.info("Unclustered records: %s", len(unclustered_df))
    logger.info("Clustered records: %s", len(clustered_df))
    logger.info("Unique clusters: %s", len(clustered_df["cluster_id"].unique()))

    final_df = pd.concat([unclustered_df, clustered_df], ignore_index=True)

    # add cluster uuids
    uuids = {}
    for _id, frame in final_df.groupby("cluster_id"):
        ids = sorted(frame["question_id"].unique().tolist())
        ids_str = "|".join(ids)
        cluster_id = md5(ids_str.encode("utf-8")).hexdigest()
        uuids[_id] = cluster_id

    final_df["cluster_uuid"] = final_df["cluster_id"].map(pd.Series(uuids))

    return drop_cluster_columns(final_df)
