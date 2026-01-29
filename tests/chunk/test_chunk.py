"""Test suite for the chunk module"""

import json
from pathlib import Path

import pandas as pd
import pytest

from cleansweep.chunk.chunk import chunk_text, create_chunked_df
from cleansweep.chunk.strategies import STRATEGIES
from cleansweep.chunk.utils import get_paragraph_delimiter

with (
    Path(__file__)
    .parent.joinpath("state_of_the_union.txt")
    .open("r", encoding="utf-8") as src
):
    TEXT = src.read()

with (
    Path(__file__).parent.joinpath("test_parameters.json").open("r", encoding="utf-8")
) as src:
    TEST_PARAMETERS = json.load(src)


def get_test_parameters(test_name: str) -> dict:
    """Pull the test parameters from the test_configs dictionary"""
    return TEST_PARAMETERS[test_name]


STRATEGY_REPOSITORY = STRATEGIES


class TestChunkText:
    """Test suite for the chunk_text function"""

    @pytest.mark.parametrize(
        "params",
        get_test_parameters("test_strategies"),
    )
    def test_strategies(self, params: dict):
        """Test the output of each strategy"""
        # set expected values
        strategy_config = STRATEGY_REPOSITORY[params["strategy"]]
        chunk_size = strategy_config.chunk_size

        # chunk data
        chunks = chunk_text(TEXT, params["strategy"])

        # assert the chunks
        assert all(len(chunk) <= chunk_size for chunk in chunks)
        assert chunks[0] == params["expected_chunk_0"]


class TestCreateChunkedDataFrame:
    """Test suite for the create_chunked_df function"""

    def test_func(self):
        """Test the create_chunked_df function"""
        # create a DataFrame with a single document
        documents = pd.DataFrame([{"id": 1, "content": TEXT}])
        df = create_chunked_df(documents, "content", "default", STRATEGY_REPOSITORY)

        # assert the DataFrame
        assert df.shape == (22, 4)
        assert df["chunk_id"].iloc[0] == "1-1"

    def test_error(self):
        """Test the create_chunked_df function with an invalid document"""
        # create a DataFrame with a single document
        documents = pd.DataFrame([{"id": 1, "content": 123}])

        # assert the ValueError
        with pytest.raises(ValueError):
            create_chunked_df(documents, "content", "default", STRATEGY_REPOSITORY)


class TestGetParagraphDelimiter:
    """Test suite for the get_paragraph_delimiter function"""

    @pytest.mark.parametrize(
        "strategy, repository, expected",
        [
            ("default", {"default": {"paragraph_delimiter": "\n\n"}}, "\n\n"),
            ("nonexistent", {"default": {"paragraph_delimiter": "\n\n"}}, "\n\n"),
            ("default", None, None),
            ("default", {"default": {}}, None),
        ],
    )
    def test_get_paragraph_delimiter(self, strategy, repository, expected):
        """Test the get_paragraph_delimiter function"""
        result = get_paragraph_delimiter(strategy, repository)
        assert result == expected
