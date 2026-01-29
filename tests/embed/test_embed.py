"""Test suite for the embed module."""

from freezegun import freeze_time

from cleansweep.embed.utils import get_embedding_file_name


class TestGetEmbeddingFileName:
    """Test suite for the get_embedding_file_name function."""

    @freeze_time("2021-01-01 00:00:00")
    def test_func(self):
        """Test the get_embedding_file_name function."""
        assert (
            get_embedding_file_name("test_2021-01-01-000000.ndjson")
            == "2021-01-01_00-00-00_embedded_2021-01-01_00-00-00.ndjson"
        )

    @freeze_time("2021-01-01 00:00:00")
    def test_extension(self):
        """Test the get_embedding_file_name function with an extension."""
        assert (
            get_embedding_file_name("test_2021-01-01-000000.ndjson", "nd.json")
            == "2021-01-01_00-00-00_embedded_2021-01-01_00-00-00.nd.json"
        )
        assert (
            get_embedding_file_name("test_2021-01-01-000000.ndjson", "parquet")
            == "2021-01-01_00-00-00_embedded_2021-01-01_00-00-00.parquet"
        )

    @freeze_time("2021-01-01 00:00:00")
    def test_no_source_time(self):
        """Test the get_embedding_file_name function with no source time in the input file name."""
        assert (
            get_embedding_file_name("test.ndjson")
            == "_embedded_2021-01-01_00-00-00.ndjson"
        )
