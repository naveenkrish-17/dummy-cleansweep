"""Module for translation of text using OpenAI models."""

import asyncio
import logging
from typing import Any, Sequence, Type, TypeVar

import pandas as pd
from pydantic import BaseModel, Field

from cleansweep._types import Deployment, Prompt, StrPath
from cleansweep.exceptions import TranslationError
from cleansweep.flags import flag
from cleansweep.iso.languages import Language
from cleansweep.prompts.utils import create_prompt, row_to_prompt_kwargs
from cleansweep.utils.azure.api import create_messages, process_api_calls
from cleansweep.utils.azure.auth import AzureCredentials
from cleansweep.utils.azure.utils import HasItems, Texts, batch_texts, process_results
from cleansweep.utils.openai.chat import chat_completion_async
from cleansweep.utils.openai.tool import create_function, tool_choice

logger = logging.getLogger(__name__)
"""Logger for the translation module."""


class TranslatedText(BaseModel):
    """A representation of the translated text."""

    text: str | None = Field(description="The translated text.")


class TranslationResponse(BaseModel):
    """A represenation of the required response schema."""

    items: list[TranslatedText | None] = Field(description="List of translated")


T = TypeVar("T", bound=HasItems)
R = TypeVar("R", bound=BaseModel)


@flag("translate", arg_pos=0)
def create_translated_df(
    df: pd.DataFrame,
    columns_to_translate: list[str] | str,
    source_language_column: str,
    target_language: Language,
    prompt_dir: StrPath,
    prompt: Prompt,
    model: Deployment,
    token_limit: int | None = None,
    temperature: float | None = None,
    timeout: int | None = None,
    credentials: AzureCredentials | None = None,
    chunk: bool = False,
    response_schema: Type[T] = TranslationResponse,
    response_type: Type[R] = TranslatedText,
) -> pd.DataFrame:
    """Translate the specified columns in a DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame to be translated.
        columns_to_translate (list[str] | str): The columns to be translated. Can be a list of
            column names or a single column name.
        source_language_column (str): The column in the DataFrame that contains the source
            language information.
        target_language (Language): The target language to translate the text to.
        prompt_dir (StrPath): The directory path where the translation prompt files are located.
        prompt (Prompt): The translation prompt to be used.
        model (Deployment): The deployment model to be used for translation.
        token_limit (int): The maximum number of tokens allowed in the translated text.
        temperature (float | None, optional): The temperature value for controlling the randomness
            of the translation. Defaults to None.
        timeout (int | None, optional): The maximum time limit for translation in seconds. Defaults
            to None.
        credentials (AzureCredentials | None, optional): The Azure credentials for authentication.
            Defaults to None.
        chunk (bool, optional): Whether to chunk the translation into smaller parts. Defaults to
            False.
        response_schema (Type[T], optional): The response schema for translation. Defaults to
            TranslationResponse.
        response_type (Type[BaseModel], optional): The response type for translation. Defaults to
            TranslatedText.

    Returns:
        pd.DataFrame: The translated DataFrame with additional columns for the translated text.

    Raises:
        ValueError: If the input DataFrame is empty or if any of the specified columns are not
            present in the DataFrame.
        ValueError: If the source language column is not found in the DataFrame.
        TranslationError: If there is an error during the translation process.

    """
    return asyncio.run(
        acreate_translated_df(
            df,
            columns_to_translate,
            source_language_column,
            target_language,
            prompt_dir,
            prompt,
            model,
            token_limit,
            temperature=temperature,
            timeout=timeout,
            credentials=credentials,
            chunk=chunk,
            response_schema=response_schema,
            response_type=response_type,
        )
    )


@flag("translate", arg_pos=0)
async def acreate_translated_df(
    df: pd.DataFrame,
    columns_to_translate: list[str] | str,
    source_language_column: str,
    target_language: Language,
    prompt_dir: StrPath,
    prompt: Prompt,
    model: Deployment,
    token_limit: int | None = None,
    temperature: float | None = None,
    timeout: int | None = None,
    credentials: AzureCredentials | None = None,
    chunk: bool = False,
    response_schema: Type[T] = TranslationResponse,
    response_type: Type[R] = TranslatedText,
) -> pd.DataFrame:
    """Translate the specified columns in a DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame to be translated.
        columns_to_translate (list[str] | str): The columns to be translated. Can be a list of
            column names or a single column name.
        source_language_column (str): The column in the DataFrame that contains the source
            language information.
        target_language (Language): The target language to translate the text to.
        prompt_dir (StrPath): The directory path where the translation prompt files are located.
        prompt (Prompt): The translation prompt to be used.
        model (Deployment): The deployment model to be used for translation.
        token_limit (int): The maximum number of tokens allowed in the translated text.
        temperature (float | None, optional): The temperature value for controlling the randomness
            of the translation. Defaults to None.
        timeout (int | None, optional): The maximum time limit for translation in seconds. Defaults
            to None.
        credentials (AzureCredentials | None, optional): The Azure credentials for authentication.
            Defaults to None.
        chunk (bool, optional): Whether to chunk the translation into smaller parts. Defaults to
            False.
        response_schema (Type[T], optional): The response schema for translation. Defaults to
            TranslationResponse.
        response_type (Type[BaseModel], optional): The response type for translation. Defaults to
            TranslatedText.

    Returns:
        pd.DataFrame: The translated DataFrame with additional columns for the translated text.

    Raises:
        ValueError: If the input DataFrame is empty or if any of the specified columns are not
            present in the DataFrame.
        ValueError: If the source language column is not found in the DataFrame.
        TranslationError: If there is an error during the translation process.

    """

    def set_output_columns(
        df: pd.DataFrame,
        columns_to_translate: list[str],
        language: Language,
        translations: list[dict[str, str]] | None = None,
    ) -> pd.DataFrame:
        """Transform the given DataFrame by translating specified columns and adding metadata.

        Args:
            df (pd.DataFrame): The DataFrame containing the data to be translated.
            columns_to_translate (list[str]): List of column names in the DataFrame that need to be
                translated.
            language (Language): The language to which the columns are being translated.
            translations (list[dict[str, str]] | None, optional): A list of dictionaries containing
                translations for the columns. Each dictionary should map column names or "text" to
                their translated values. Defaults to None.

        Returns:
            pd.DataFrame: A new DataFrame with the translated columns, original columns, and
                language metadata. The original columns specified in `columns_to_translate` are
                removed and replaced with:
                - "translated_{column}": The translated text for the column.
                - "original_{column}": The original text for the column.
                - "{column}_language": The language of the translation.

        """
        for column in columns_to_translate:

            _translations = []
            if translations:
                for tr in translations:
                    _tr = tr.get(column)
                    if _tr is None:
                        _tr = tr.get("text")
                    _translations.append(_tr)

            df[f"translated_{column}"] = _translations if _translations else df[column]
            df[f"original_{column}"] = df[column]
            df[f"{column}_language"] = language
            df.drop(column, axis=1, inplace=True)

        return df

    if df.empty:
        raise ValueError("Input DataFrame is empty")

    if not isinstance(columns_to_translate, list):
        columns_to_translate = [columns_to_translate]

    if any(column not in df.columns for column in columns_to_translate):
        raise ValueError(
            f"All columns ({columns_to_translate}) must be present in DataFrame"
        )

    if source_language_column not in df.columns:
        raise ValueError(f"Column {source_language_column} not found in DataFrame")

    dataframes_to_concat = []
    # for each group, translate the text
    for source_language, group in df.groupby(source_language_column):
        if not isinstance(source_language, Language):
            source_language = Language[str(source_language)]
        if source_language == target_language:
            logger.debug("%d records are already in the target language", len(group))

            group = set_output_columns(group, columns_to_translate, target_language)

        else:

            logger.debug(
                "Translating %d records from %s to %s using model %s",
                len(group),
                source_language.name,
                target_language.name,
                model.model,
            )

            try:
                translations = await translate(
                    group[columns_to_translate].to_dict(
                        orient="records"
                    ),  # pyright: ignore[reportArgumentType]
                    target_language,
                    source_language,
                    prompt_dir,
                    prompt,
                    model,
                    token_limit,
                    temperature=temperature,
                    timeout=timeout,
                    credentials=credentials,
                    chunk=chunk,
                    response_schema=response_schema,
                    response_type=response_type,
                )
            except TranslationError as e:
                logger.error(
                    "Error translating text from %s to %s: %s",
                    source_language.name,
                    target_language.name,
                    e,
                )

                group = set_output_columns(group, columns_to_translate, source_language)

            else:
                if len(translations) != len(group):
                    logger.error(
                        (
                            "Number of translations does not match number of documents, no "
                            "translations will be applied."
                        )
                    )

                    group = set_output_columns(
                        group, columns_to_translate, source_language
                    )

                else:
                    group = set_output_columns(
                        group, columns_to_translate, target_language, translations
                    )

        dataframes_to_concat.append(group)

    return pd.concat(dataframes_to_concat)


async def translate(
    to_translate: Sequence[Texts],
    target_language: Language,
    source_language: Language,
    prompt_dir: StrPath,
    prompt: Prompt,
    model: Deployment,
    token_limit: int | None = None,
    temperature: float | None = None,
    timeout: int | None = None,
    chunk: bool = False,
    credentials: AzureCredentials | None = None,
    response_schema: Type[T] = TranslationResponse,
    response_type: Type[R] = TranslatedText,
) -> list[dict[str, Any]]:
    """Translate a list of texts.

    Args:
        to_translate (list[Texts]): The list of texts to be translated.
        target_language (Language): The target language for translation.
        source_language (Language): The source language of the texts.
        prompt_dir (StrPath): The directory containing the prompt template.
        prompt (Prompt): The prompt object containing the template and variables.
        model (Deployment): The deployment model to be used for translation.
        token_limit (int): The maximum number of tokens allowed in the translation.
        temperature (float | None, optional): The temperature value for generating diverse
            translations. Defaults to None.
        timeout (int | None, optional): The timeout value for the translation process. Defaults to
            None.
        chunk (bool, optional): Whether to process the texts in chunks. Defaults to False.
        credentials (AzureCredentials | None, optional): The Azure credentials for authentication.
            Defaults to None.
        response_schema (Type[T], optional): The response schema for the translation API. Defaults
            to TranslationResponse.
        response_type (Type[R], optional): The response type for the translated text.
            Defaults to TranslatedText.

    Returns:
        list[R]: The list of translated texts.

    Raises:
        None

    Examples:
        None

    """
    if credentials is None:
        credentials = AzureCredentials()
    kwargs = {"temperature": temperature} if temperature is not None else {}

    texts = list(to_translate)
    if chunk:

        if token_limit is None:
            raise ValueError("Token limit must be specified when chunking")

        texts = batch_texts(to_translate, token_limit, model.model)

    tasks = [
        create_messages(
            user_input=create_prompt(
                prompt_dir,
                template_name=prompt.template,
                prompt=prompt.prompt,
                **row_to_prompt_kwargs(
                    {
                        "source_language": source_language,
                        "target_language": target_language,
                        "text": text,
                    },  # pyright: ignore[reportArgumentType]
                    prompt.variables,
                ),
            )
        )
        for text in texts
    ]

    results = await process_api_calls(
        chat_completion_async,
        tasks,
        "chat",
        model,
        credentials=credentials,
        timeout=timeout,
        tools=[
            create_function(
                "translated_texts",
                "Submit a list of translated texts.",
                response_schema,  # pyright: ignore[reportArgumentType]
                strict=False,
            )
        ],
        tool_choice=tool_choice("translated_texts"),
        ignore_refusals=True,
        **kwargs,
    )

    processed_results = process_results(results, response_type, response_schema)

    output = []
    for i, result in enumerate(processed_results):
        input_docs = texts[i] if i < len(texts) else None
        if not isinstance(input_docs, list):
            input_docs = [input_docs]

        if result is None:
            logger.debug("Error translating chunk %d", i)
            output.extend(input_docs)
            continue

        if len(result.items) != len(input_docs):
            logger.debug(
                "Number of translations does not match number of documents in chunk %d",
                i,
            )
            output.extend(input_docs)
            continue

        for j, translated_text in enumerate(result.items):

            if translated_text:
                output.append(translated_text.model_dump())
            else:
                docs = None
                if chunk:
                    docs = input_docs
                elif j < len(input_docs):
                    docs = input_docs[j]
                logger.debug("Error translating document %d in chunk %d", j, i)
                output.append(docs)

    return output
