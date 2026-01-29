"""Semantic chunking module for generating question and answer pairs from articles."""

import asyncio
import logging
from typing import Tuple

import pandas as pd
from pandas.core.groupby.generic import SeriesGroupBy
from pydantic import BaseModel, Field

from cleansweep._types import Deployment, Prompt, StrPath
from cleansweep.model.question import QuestionAnswer, QuestionAnswerBase
from cleansweep.prompts.utils import create_prompt, row_to_prompt_kwargs
from cleansweep.utils.azure.api import process_api_calls
from cleansweep.utils.azure.auth import AzureCredentials
from cleansweep.utils.azure.utils import create_message, process_results
from cleansweep.utils.openai.chat import chat_completion_async
from cleansweep.utils.openai.tool import create_function, tool_choice

logger = logging.getLogger(__name__)


class SemanticChunkingResponse(BaseModel, arbitrary_types_allowed=True):
    """A represenation of the required response schema."""

    items: list[QuestionAnswerBase | None] = Field(
        description="List of question and answer pairs"
    )


def create_question_answer_pairs(
    articles: pd.DataFrame,
    prompt_dir: StrPath,
    qa_prompt: Prompt,
    model: Deployment,
    credentials: AzureCredentials | None = None,
    temperature: float | None = None,
) -> list[SemanticChunkingResponse]:
    """Generate question and answer pairs from a list of articles.

    Args:
        articles (DataFrame): The articles to generate question and answer pairs from.
        prompt_dir (StrPath): The directory containing the prompt templates.
        qa_prompt (Prompt): The prompt template for generating question and answer pairs.
        model (Deployment): The deployment model to use for generating the pairs.
        credentials (AzureCredentials, Optional): The Azure credentials. Defaults to None. If None,
            the default credentials are taken from the environment.
        temperature (float, Optional): The temperature to use for the model. Defaults to None.

    Returns:
        results (list[SemanticChunkingResponse]): A list of question and answer pairs generated
            from the articles.

    """
    return asyncio.run(
        acreate_question_answer_pairs(
            articles, prompt_dir, qa_prompt, model, credentials, temperature=temperature
        )
    )


async def acreate_question_answer_pairs(
    articles: pd.DataFrame,
    prompt_dir: StrPath,
    qa_prompt: Prompt,
    model: Deployment,
    credentials: AzureCredentials | None = None,
    temperature: float | None = None,
) -> list[SemanticChunkingResponse]:
    """Generate question and answer pairs from a list of articles.

    Args:
        articles (DataFrame): The articles to generate question and answer pairs from.
        prompt_dir (StrPath): The directory containing the prompt templates.
        qa_prompt (Prompt): The prompt template for generating question and answer pairs.
        model (Deployment): The deployment model to use for generating the pairs.
        credentials (AzureCredentials, Optional): The Azure credentials. Defaults to None. If None,
            the default credentials are taken from the environment.
        temperature (float, Optional): The temperature to use for the model. Defaults to None.

    Returns:
        results (list[SemanticChunkingResponse]): A list of question and answer pairs generated
            from the articles.

    """
    if credentials is None:
        credentials = AzureCredentials()

    kwargs = {"temperature": temperature} if temperature is not None else {}

    tasks = [
        [
            create_message(
                content=create_prompt(
                    prompt_dir,
                    template_name=qa_prompt.template,
                    prompt=qa_prompt.prompt,
                    **row_to_prompt_kwargs(row, qa_prompt.variables),
                ),
                role="user",
            )
        ]
        for _, row in articles.iterrows()
    ]

    results = await process_api_calls(
        chat_completion_async,
        tasks,
        "chat",
        model,
        credentials=credentials,
        tools=[
            create_function(
                "qa",
                "Generate question and answer pairs from a knowledge article.",
                SemanticChunkingResponse,
                strict=False,
            )
        ],
        tool_choice=tool_choice("qa"),
        ignore_refusals=True,
        **kwargs,
    )

    return process_results(
        results,  # pyright: ignore[reportArgumentType]
        QuestionAnswerBase,
        SemanticChunkingResponse,
    )


def create_question_answer_dataframe(
    df: pd.DataFrame | SeriesGroupBy, qa_pairs: list[SemanticChunkingResponse]
) -> Tuple[pd.DataFrame, pd.DataFrame | None]:
    """Create a DataFrame from question answer pairs.

    Args:
        df (Union[DataFrame, SeriesGroupBy]): DataFrame with articles
        qa_pairs (list[SemanticChunkingResponse]): List of question answer pairs

    Returns:
        kb_df (DataFrame): DataFrame with question answer pairs
        failed_row_df (DataFrame, Optional): DataFrame with failed

    """
    qa_objects = []
    failed_row = []
    for row, response in zip(df.iterrows(), qa_pairs):

        row = row[1]

        if response is None:
            logger.warning("No questions generated for id %s", row.get("id"))
            failed_row.append(row.get("id"))
            continue

        for qa_pair in response.items:
            if qa_pair is not None:
                qa_objects.append(
                    QuestionAnswer(
                        question=qa_pair.question,
                        answer=qa_pair.answer,
                        source_id=row.get("id"),
                    )
                )

    logger.debug("Created QuestionAnswer objects from DataFrame")

    kb_df = pd.DataFrame([pair.model_dump() for pair in qa_objects])

    logger.debug(
        "Created new DataFrame from QuestionAnswer objects with %s questions",
        len(kb_df),
    )

    failed_row_df = None
    if failed_row:
        failed_row_df = pd.DataFrame(failed_row, columns=["id"])

    return kb_df, failed_row_df
