"""Generate metadata for the documents."""

import asyncio
import logging
from typing import Sequence

import pandas as pd

from cleansweep._types import Deployment, StrPath
from cleansweep.flags import flag
from cleansweep.prompts.utils import create_prompt, row_to_prompt_kwargs
from cleansweep.settings.metadata import MetadataGenerationConfig
from cleansweep.utils.azure.api import process_api_calls
from cleansweep.utils.azure.auth import AzureCredentials
from cleansweep.utils.azure.utils import create_message
from cleansweep.utils.openai.chat import chat_completion_async

logger = logging.getLogger(__name__)
"""Logger for the metadata module"""


@flag("chunk_metadata", arg_pos=0)
def add_metadata_to_df(
    df: pd.DataFrame,
    metadata_configs: Sequence[MetadataGenerationConfig],
    prompt_dir: StrPath,
    model: Deployment,
    credentials: AzureCredentials | None = None,
    temperature: float | None = None,
    timeout: float | None = None,
) -> pd.DataFrame:
    """Add metadata to the documents dataframe.

    Args:
        df (pd.DataFrame): The dataframe containing the documents
        metadata_configs (Sequence[MetadataGenerationConfig]): The metadata generation
            configurations
        prompt_dir (StrPath): The directory containing the prompt templates
        model (Deployment): The deployment model to use for generating the metadata
        credentials (AzureCredentials, optional): The Azure credentials. Defaults to None.
        temperature (float, optional): The temperature for the model. Defaults to None.
        timeout (float, optional): The timeout for the model. Defaults to None.

    Returns:
        pd.DataFrame: The dataframe with the metadata added

    """
    if credentials is None:
        credentials = AzureCredentials()
    kwargs = {"temperature": temperature} if temperature is not None else {}

    logger.info("Adding metadata to documents...")

    for config in metadata_configs:

        logger.info(
            "Generating metadata for %s using %s prompt",
            config.output,
            config.prompt.name,
        )

        prompts = df.apply(
            lambda x, config=config: [
                create_message(
                    create_prompt(
                        prompt_dir,
                        template_name=config.prompt.template,
                        prompt=config.prompt.prompt,
                        **row_to_prompt_kwargs(x, config.prompt.variables),
                    ),
                    role="user",
                )
            ],
            axis=1,
        ).to_list()

        metadata_results = asyncio.run(
            process_api_calls(
                chat_completion_async,
                prompts,
                "chat",
                model,
                credentials=credentials,
                timeout=timeout,
                **kwargs,
            )
        )

        values = []
        for result in metadata_results:
            if result:
                values.append(result)
            else:
                values.append("")

        df[config.output] = values

    logger.info("Metadata generation complete")

    return df
