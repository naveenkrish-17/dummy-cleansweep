"""Embed module for Cleansweep."""

import pandas as pd

from cleansweep import __app_name__
from cleansweep.core import (
    get_plugin_module,
    read_curated_file_to_dataframe,
    write_dict_to_newline_delimited_json_file,
)
from cleansweep.core.delta import delta_merge, delta_prepare
from cleansweep.core.fileio import (
    create_glob_pattern,
    create_target_file_path,
    write_dataframe_to_parquet_file,
)
from cleansweep.embed.embedding import (
    create_df_to_embed,
    create_embedding_file_content,
    create_embeddings,
    get_columns_to_embed,
)
from cleansweep.embed.model import EmbeddedChunk, EmQA
from cleansweep.embed.utils import (
    add_root_document_to_df,
    create_em_source_document,
    get_dedup_id,
    get_description,
    get_document,
    get_embedding_file_name,
    get_id,
    get_sufficient_references,
    is_public,
)
from cleansweep.enumerations import EmbedderType, LoadType
from cleansweep.exceptions import PipelineError
from cleansweep.hooks.hookimpl import get_plugin_manager
from cleansweep.model.network import raw_path
from cleansweep.settings.base import settings
from cleansweep.settings.embedding import EmbeddingSettings
from cleansweep.settings.files import InputFiles
from cleansweep.settings.load import load_settings
from cleansweep.utils.azure.utils import num_tokens_from_strings
from cleansweep.utils.dataframe import (
    aggregate_dataframe_by_columns,
    refactor_dataframe,
)
from cleansweep.utils.exceptions import error_handler, initialize_except_hook
from cleansweep.utils.logging import set_app_labels, setup_logging
from cleansweep.utils.slack import send_error_message

logger = setup_logging(__app_name__, settings.log_level, settings.dev_mode)
"""Logger for the module."""

initialize_except_hook(uncaught_hook=error_handler)


SETTINGS_EXCLUDE = {
    "model",
    "embedder_type",
    "plugin",
    "extra_columns_to_embed",
}
"""Settings fields to exclude when passing to the embed_documents method."""


def main():
    """Process embedding steps."""
    input_files = InputFiles()

    if input_files.config_file_uri is not None:
        logger.info("Config file: %s", input_files.config_file_uri)

    logger.info("Loading settings...")
    app = load_settings(EmbeddingSettings, input_files.config_file_uri)
    assert not isinstance(app.model, str), "Model must be a Deployment object."
    set_app_labels(app.labels)

    input_files.initialize_input_file_uri(app.source)

    logger.info("📖 Reading curated data file %s.", input_files.input_file)
    source_documents = read_curated_file_to_dataframe(input_files.input_file)

    # setup platform specific default variables
    columns_to_embed = []
    action_column = id_column = target_type = None

    if app.platform == "kosmo":
        columns_to_embed.append("chunk")
        target_type = EmbeddedChunk
    elif app.platform == "em":
        columns_to_embed.append("question")
        target_type = EmQA
        id_column = "question_id"

    if app.columns_to_embed:
        columns_to_embed = app.columns_to_embed

    documents, previous_embedded_documents = delta_prepare(
        source_documents,
        app.load_type,
        app.staging_bucket,
        match_glob=create_glob_pattern(
            app.output.directory or "embedded", extension=".parquet"
        ),
        force=app.force,
        action_column=action_column,
        id_column=id_column,
    )

    documents_df_embedded = None
    target = get_embedding_file_name(raw_path(input_files.input_file), "parquet")

    target_pq = create_target_file_path(
        file_name=target,
        extension=".parquet",
        bucket=app.output.bucket or app.staging_bucket,
        target_dir=app.output.directory or "embedded",
        run_id=app.run_id if app.output.use_run_id else None,
        prefix=app.output.prefix,
    )

    if target_type is None:
        raise ValueError(f"Invalid platform: {app.platform}")

    if documents is not None and not documents.empty:
        plugin_manager = None
        # execute the pre chunk hooks
        if app.plugin is not None:
            logger.info("🔌 Applying plugins...")
            plugin = get_plugin_module(app.plugin)
            plugin_manager = get_plugin_manager()
            plugin_manager.register(plugin)

            results = plugin_manager.hook.pre_embed(documents=documents)
            if results:
                documents = results[0]

        documents_df_to_embed = create_df_to_embed(
            documents,
            get_columns_to_embed(columns_to_embed, app.extra_columns_to_embed),
        )
        logger.info("Embedding model: %s", app.model.model)
        logger.info("Embedding %d documents.", len(documents_df_to_embed))
        documents_df_embedded = create_embeddings(
            documents_df_to_embed,
            app.embedder_type,
            app.model,
            **app.model_dump(exclude=SETTINGS_EXCLUDE),
        )

        if "embedding" not in documents_df_embedded.columns:
            documents_df_embedded["embedding"] = None

        logger.info("Adding root document to the embedded documents")
        documents_df_embedded = add_root_document_to_df(documents_df_embedded)

        # check for default embeddings, if so notify.
        default_embeddings = documents_df_embedded[
            documents_df_embedded.apply(
                lambda x: all(v == 0.0 for v in x["embedding"]), axis=1
            )
        ]

        if default_embeddings.empty is False:
            logger.warning(
                "Found %d default embeddings in the embedded documents. "
                "This may indicate an issue with the embeddings.",
                len(default_embeddings),
            )

            record_id = id_column or "chunk_id"

            ids = (
                default_embeddings[record_id]
                .unique()  # pyright: ignore[reportAttributeAccessIssue]
                .tolist()
            )
            logger.warning("Default embeddings found in the following records: %s", ids)
            message = f"Default embeddings found in the following records: \n{',\n'.join(ids[:10])}"
            if len(ids) > 10:
                message += "\n\n**See logs for additional missing embeddings.**"
            send_error_message(
                settings.default_channel,
                error=PipelineError(message),
            )

        # execute the post chunk hooks
        if app.plugin is not None:

            logger.info("🔌 Applying plugins...")
            if plugin_manager is None:
                logger.info("Loading embedding plugin...")
                plugin = get_plugin_module(app.plugin)
                plugin_manager = get_plugin_manager()
                plugin_manager.register(plugin)

            results = plugin_manager.hook.post_embed(documents=documents_df_embedded)
            if results:
                documents = results[0]
    elif app.load_type in [LoadType.DELTA, LoadType.INCREMENTAL]:
        # we could have only deletions so there are no records to process
        # but we still need to load the previous_df
        logger.info("No records to process. Loading previous_df.")
        documents_df_embedded = previous_embedded_documents
        previous_embedded_documents = None
    else:
        logger.info("No records to process. Exiting...")
        exit(0)

    assert documents_df_embedded is not None, "There are no records to process."
    final_documents = delta_merge(documents_df_embedded, previous_embedded_documents)

    logger.info("📝 Writing %d embeddings to %s", len(final_documents), target_pq)

    write_dataframe_to_parquet_file(final_documents, target_pq)

    # prep dataframe
    if app.platform == "em":

        final_documents = final_documents.drop_duplicates(
            subset=["question_uuid", "source_id"], keep="first"
        )

        # custom handling for is_sufficient - it needs to be unique or it will be reduced in the
        # aggregation. Convert to a tuple with the first value being the boolean and the second
        # being the uuid as this is unqiue for each row.
        final_documents["is_sufficient"] = final_documents.apply(
            lambda x: (
                x["is_sufficient"] if pd.notnull(x["is_sufficient"]) else True,
                x["question_uuid"],
            ),
            axis=1,
        )

        final_documents = aggregate_dataframe_by_columns(
            final_documents, ["question_id"]
        )

    mapping = {
        "answer": "answer",
        "article_id": "id",
        "dedup_id": get_dedup_id,
        "description": get_description,
        "document": get_document,
        "embedding": "embedding",
        "id": get_id,
        "is_public": is_public,
        "question": "question",
        "sufficient_references": get_sufficient_references,
        "references": create_em_source_document,
        "title": "title",
        "url": "metadata_url",
        "metadata_root_document_title": "metadata_root_document_title",
        "metadata_root_document_description": "metadata_root_document_description",
        "metadata_root_document_url": "metadata_root_document_url",
        "metadata_root_document_content_type": "metadata_root_document_content_type",
    }

    refactored_dataframe = refactor_dataframe(
        final_documents,
        mapping,
        **{
            "max_document_length": app.max_document_length,
            "len_func": (
                num_tokens_from_strings
                if app.embedder_type == EmbedderType.OPENAI
                else len
            ),
            "platform": app.platform,
        },
    )

    target_embeddings = create_target_file_path(
        file_name=target,
        extension=".nd.json",
        bucket=app.output.bucket or app.staging_bucket,
        target_dir=app.output.directory or "embedded",
        run_id=None,
        prefix=app.output.prefix,
    )

    logger.info(
        "Writing %s embeddings to %s", len(refactored_dataframe), target_embeddings
    )

    embedding_file_content, errors = create_embedding_file_content(
        refactored_dataframe, target_type
    )

    if errors:
        message_note = ""
        if len(errors) > 3:
            errors = errors[:3]
            message_note = (
                "\n\n**Additional errors were encountered during embedding. "
                "Please check the logs for more information.**"
            )

        send_error_message(
            settings.default_channel,
            error=PipelineError(
                f"Errors encountered during embedding: {errors}{message_note}",
            ),
        )

    write_dict_to_newline_delimited_json_file(embedding_file_content, target_embeddings)

    logger.info("Embedding complete")
    exit(0)
