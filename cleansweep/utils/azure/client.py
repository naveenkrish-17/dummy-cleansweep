"""Functions to get the OpenAI client."""

from cachetools import TTLCache, cached
from openai import DEFAULT_MAX_RETRIES, AsyncAzureOpenAI, AzureOpenAI

from .auth import AzureCredentials


@cached(cache=TTLCache(maxsize=1, ttl=1800))
def get_open_ai_client(
    credentials: AzureCredentials, max_retries: int | None = None
) -> AzureOpenAI:
    """Get the OpenAI client.

    Args:
        credentials (AzureCredentials): The Azure credentials.
        max_retries (int, Optional): The maximum number of retries.

    Returns:
        AzureOpenAI: An instance of the AzureOpenAI class with the API key and endpoint set.

    """
    if max_retries is None:
        max_retries = DEFAULT_MAX_RETRIES

    if credentials.openai_api_base is None:
        raise ValueError("The Azure endpoint is required")

    return AzureOpenAI(
        api_key=credentials.api_key.get_secret_value(),  # type: ignore
        azure_endpoint=credentials.openai_api_base,
        api_version=credentials.openai_api_version,
        max_retries=max_retries,
    )


@cached(cache=TTLCache(maxsize=1, ttl=1800))
def get_open_ai_client_async(
    credentials: AzureCredentials, max_retries: int | None = None
) -> AsyncAzureOpenAI:
    """Get the OpenAI client.

    Args:
        credentials (AzureCredentials): The Azure credentials.
        max_retries (int, Optional): The maximum number of retries.

    Returns:
        An instance of the AsyncAzureOpenAI client.

    """
    if max_retries is None:
        max_retries = DEFAULT_MAX_RETRIES

    if credentials.openai_api_base is None:
        raise ValueError("The Azure endpoint is required")

    return AsyncAzureOpenAI(
        api_key=credentials.api_key.get_secret_value(),  # type: ignore
        azure_endpoint=credentials.openai_api_base,
        api_version=credentials.openai_api_version,
        max_retries=max_retries,
    )
