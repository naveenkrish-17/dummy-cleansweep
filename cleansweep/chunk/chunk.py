"""Sub-module for chunking text into smaller parts for embedding."""

from importlib.util import find_spec
from pathlib import Path

import nltk
import pandas as pd

from cleansweep._types import ChunkingStrategy
from cleansweep.chunk.strategies import STRATEGIES

EXCLUDE_SETTINGS = {"paragraph_delimiter", "description", "name", "text_splitter"}
"""Settings to exclude from the strategy settings when passing to the text splitter."""

if find_spec("en_core_web_sm") is None:
    from spacy.cli.download import download as spacy_download

    spacy_download("en_core_web_sm")

if not Path.home().joinpath("nltk_data/tokenizers/punkt_tab").exists():
    nltk.download("punkt_tab")


def chunk_text(
    text: str,
    strategy: str,
    strategy_repository: dict[str, ChunkingStrategy] | None = None,
) -> list[str]:
    """Chunk the given text using the given strategy.

    Args:
        text (str): The text to chunk
        strategy (str): The strategy to use
        strategy_repository (dict, optional): The strategy repository. Defaults to None. The
            strategy repository contains the configuration for the strategies.

    Returns:
        list[str]: The chunks

    """
    if strategy_repository is None:
        strategy_repository = STRATEGIES

    if strategy not in strategy_repository:
        strategy = "default"

    _strategy = strategy_repository[strategy]

    assert (
        _strategy.text_splitter is not None
    ), f"Text splitter is not defined for {strategy}"

    splitter = _strategy.text_splitter(
        **_strategy.model_dump(exclude=EXCLUDE_SETTINGS, exclude_none=True)
    )
    return splitter.split_text(text)


def create_chunked_df(
    documents: pd.DataFrame,
    column_to_chunk: str,
    strategy: str,
    strategy_repository: dict | None = None,
) -> pd.DataFrame:
    """Create a DataFrame containing the chunked documents.

    Args:
        documents (DataFrame): The documents to chunk.
        column_to_chunk (str): The column containing the text to chunk.
        strategy (str): The strategy to use for chunking.
        strategy_repository (dict): The strategy repository.

    Returns:
        DataFrame: The chunked documents.

    """
    data = []
    for _, document in documents.iterrows():
        content = document[column_to_chunk]
        if not isinstance(content, str):
            raise ValueError("Document content is not a string")

        chunks = chunk_text(
            content,
            strategy=strategy,
            strategy_repository=strategy_repository,
        )

        for i, chunk in enumerate(chunks):
            d = document.to_dict()
            d["chunk"] = chunk
            d["chunk_id"] = f"{d['id']}-{i + 1}"
            data.append(d)

    return pd.DataFrame(data)
