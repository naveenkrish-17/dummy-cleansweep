"""Test suite for the metadata module."""

import pandas as pd
import pytest

from cleansweep._types import Prompt
from cleansweep.metadata import add_metadata_to_df
from cleansweep.settings.load import load_settings, settings
from cleansweep.settings.metadata import MetadataGenerationConfig, MetadataSettings

app = load_settings(MetadataSettings)


class TestAddMetadataToDf:
    """Test suite for the add_metadata_to_df function."""

    @pytest.fixture(scope="class", autouse=True)
    def mocl_api(self, class_mocker):
        """Mock the Azure API calls."""
        yield class_mocker.patch(
            "cleansweep.metadata.generate.process_api_calls",
            return_value=["This is a test", "This is a test", "This is a test"],
        )

    @pytest.mark.order(2)
    def test_add_metadata_to_df(self):
        """Test the add_metadata_to_df function."""

        df = pd.DataFrame(
            {
                "chunk": [
                    "This is the first document.",
                    "This is the second document.",
                    "This is the third document.",
                ],
                "metadata_language": ["en", "en", "en"],
                "title": ["Title", "Title", "Title"],
            }
        )
        original = app.feature.chunk_metadata
        app.feature.chunk_metadata = True

        config = MetadataGenerationConfig(
            prompt=Prompt(prompt="This is a test", name="description"),
            output="description",
        )

        result_df = add_metadata_to_df(
            df, [config], settings.prompts_template_dir, app.model
        )
        app.feature.chunk_metadata = original

        assert result_df["metadata_description"].tolist() == [
            "This is a test",
            "This is a test",
            "This is a test",
        ]

    @pytest.mark.order(3)
    def test_flag_enabled(self):
        """Test the add_metadata_to_df function."""

        df = pd.DataFrame(
            {
                "chunk": [
                    "This is the first document.",
                    "This is the second document.",
                    "This is the third document.",
                ],
                "metadata_language": ["en", "en", "en"],
                "title": ["Title", "Title", "Title"],
            }
        )
        original = app.feature.chunk_metadata
        app.feature.chunk_metadata = True

        config = MetadataGenerationConfig(
            prompt=Prompt(prompt="This is a test", name="description"),
            output="description",
        )

        result_df = add_metadata_to_df(
            df, [config], settings.prompts_template_dir, app.model
        )
        app.feature.chunk_metadata = original

        assert result_df["metadata_description"].tolist() == [
            "This is a test",
            "This is a test",
            "This is a test",
        ]

    @pytest.mark.order(4)
    def test_flag_disabled(self):
        """Test the add_metadata_to_df function."""

        df = pd.DataFrame(
            {
                "chunk": [
                    "This is the first document.",
                    "This is the second document.",
                    "This is the third document.",
                ],
                "metadata_language": ["en", "en", "en"],
                "title": ["Title", "Title", "Title"],
            }
        )
        original = app.feature.chunk_metadata
        app.feature.chunk_metadata = True

        config = MetadataGenerationConfig(
            prompt=Prompt(prompt="This is a test", name="description"),
            output="description",
        )

        result_df = add_metadata_to_df(
            df, [config], settings.prompts_template_dir, app.model
        )
        app.feature.chunk_metadata = original

        assert result_df.equals(df)
