"""Semantic chunking module for generating question and answer pairs from articles."""

import asyncio
import json
import logging
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ValidationError

from cleansweep._types import Deployment, Prompt, StrPath
from cleansweep.prompts.utils import create_prompt, row_to_prompt_kwargs
from cleansweep.utils.azure.api import create_messages, process_api_calls
from cleansweep.utils.azure.auth import AzureCredentials
from cleansweep.utils.openai.chat import chat_completion_async
from cleansweep.utils.openai.tool import create_function, tool_choice

logger = logging.getLogger(__name__)


class HallucinationCheck(BaseModel):
    """Model for hallucination check results."""

    question_id: str
    rating: Literal["intrinsic", "extrinsic", "consistent"]
    justification: str


def create_validation_dataframe(
    df: pd.DataFrame, prompt_dir: StrPath, question_prompt: Prompt
) -> pd.DataFrame:
    """Create a DataFrame for validation.

    Args:
        df (DataFrame): The DataFrame containing the questions.
        prompt_dir (StrPath): The path to the prompt directory.
        question_prompt (Prompt): The question prompt.

    Returns:
        DataFrame: The DataFrame containing the validation data.

    """
    kwargs_repo = df.apply(
        lambda x: row_to_prompt_kwargs(x, question_prompt.variables), axis=1
    )
    kwargs_df = pd.DataFrame(kwargs_repo.tolist())

    document_df = pd.DataFrame(
        [
            {"id": question_id, "document": question_df["document"].str.cat(sep="\n\n")}
            for question_id, question_df in kwargs_df.groupby("id")
        ]
    )

    kwargs_df.drop("document", axis=1, inplace=True)

    final_df = pd.merge(document_df, kwargs_df, on="id").drop_duplicates("id")

    final_df["validation_prompt"] = final_df.apply(
        lambda x: create_prompt(
            prompt_dir,
            template_name=question_prompt.template,
            prompt=question_prompt.prompt,
            **x.to_dict(),
        ),
        axis=1,
    ).tolist()
    return final_df


def validate_questions(
    df: pd.DataFrame,
    prompt_dir: StrPath,
    validation_prompt: Prompt,
    model: Deployment,
    credentials: AzureCredentials | None = None,
    temperature: float | None = None,
) -> pd.DataFrame:
    """Validate the questions using hallucination check.

    Args:
        df (DataFrame): The DataFrame containing the questions.
        prompt_dir (StrPath): The path to the prompt directory.
        validation_prompt (Prompt): The validation prompt.
        model (Deployment): The model to use.
        credentials (AzureCredentials, Optional): The Azure credentials. Defaults to None. If None,
            the default credentials are taken from the environment.
        temperature (float, Optional): The temperature to use for the model. Defaults to None.

    Returns:
        DataFrame: The DataFrame containing the validation results.

    """
    if credentials is None:
        credentials = AzureCredentials()
    kwargs = {"temperature": temperature} if temperature is not None else {}
    validation_df = create_validation_dataframe(df, prompt_dir, validation_prompt)

    tasks = (
        validation_df["validation_prompt"]
        .apply(lambda x: create_messages(user_input=x))
        .to_list()
    )

    results = asyncio.run(
        process_api_calls(
            chat_completion_async,
            tasks,
            "chat",
            model,
            credentials=credentials,
            tools=[
                create_function(
                    "validate",
                    "Validate that the answer is sourced from the document.",
                    HallucinationCheck,
                    strict=False,
                )
            ],
            tool_choice=tool_choice("validate"),
            ignore_refusals=True,
            **kwargs,
        )
    )

    serialised_results = []
    for result in results:
        if result:
            try:
                serialised_results.append(json.loads(result))
            except json.JSONDecodeError as e:
                logger.error("JSON decode error: %s", e)
                logger.debug("Result: %s", result)
                serialised_results.append(None)
        else:
            serialised_results.append(None)

    data = []
    for result, prompt in zip(
        serialised_results, validation_df["validation_prompt"].tolist()
    ):
        if result is None:
            logger.warning("Result is None.")
            continue

        try:
            dump = HallucinationCheck(**result).model_dump()
            dump["validation_prompt"] = prompt
            data.append(dump)
        except ValidationError as e:
            logger.error("Validation error: %s", e)
            logger.debug("Result: %s", result)

    return pd.merge(
        df,
        pd.DataFrame(data),
        left_on="question_id",
        right_on="question_id",
    )
