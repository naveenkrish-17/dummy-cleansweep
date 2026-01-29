"""Test suite for fileio module"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from cleansweep.core import (
    documents_to_dataframe,
    read_curated_file_to_dataframe,
    read_documents_file_to_dataframe,
    read_file_to_dict,
    write_dataframe_to_avro_file,
    write_dataframe_to_newline_delimited_json_file,
    write_dataframe_to_parquet_file,
)
from cleansweep.core.fileio import create_glob_pattern


def mock_read(*args, **kwargs):  # noqa pylint: disable=unused-argument
    """Mock read function"""
    return "Hello World!"


def mock_read_lines(*args, **kwargs):  # noqa pylint: disable=unused-argument
    """Mock read_lines function"""
    yield "Hello World!"


class TestDocumentsToDataFrame:
    """Test suite documents_to_dataframe function"""

    def test_documents_to_dataframe(self):
        """Test documents_to_dataframe method"""
        # test valid schema
        documents = [
            {
                "id": "1",
                "name": "test",
                "content": [{"data": ["test"]}],
                "metadata": {},
            },
        ]

        df, _ = documents_to_dataframe(documents)

        assert df.loc[0, "id"] == "1"
        assert df.loc[0, "title"] == "test"
        assert df.loc[0, "content"] == "test"

        # test invalid schema
        documents = [
            {
                "id": "1",
                "name": "test",
                "content": [{"data": "test"}],
            },
        ]

        _, errors = documents_to_dataframe(documents)

        assert len(errors) == 1


class TestReadDocumentsFileToDataframe:
    """Test suite read_documents_file_to_dataframe function"""

    def test_read_documents_file_to_dataframe(self, tmp_path):
        """Test read_documents_file_to_dataframe method"""
        path = tmp_path / "test.json"
        path.write_text(
            """[{"id": "1", "name": "test", "content": [{"data": ["test"]}], "metadata": {}}]"""
        )

        df, _ = read_documents_file_to_dataframe("file://" + str(path))

        assert df.loc[0, "id"] == "1"
        assert df.loc[0, "title"] == "test"
        assert df.loc[0, "content"] == "test"

        # not implemented file type
        with pytest.raises(NotImplementedError):
            read_documents_file_to_dataframe("file://test.txt")


class TestWriteDataframeToAvroFile:
    """Test suite write_dataframe_to_avro_file function"""

    @pytest.fixture(autouse=True, scope="class")
    def dataframe(self):
        """Test fixture for dataframe"""
        yield pd.DataFrame(
            [
                {
                    "id": "1",
                    "name": "test",
                    "content": "test",
                    "metadata_url": "https://test.com",
                }
            ]
        )

    def test_write_dataframe_to_avro_file(self, tmp_path, dataframe):
        """Test write_dataframe_to_avro_file method"""
        path = tmp_path / "test.avro"
        write_dataframe_to_avro_file(dataframe, "file://" + str(path))

        assert path.exists()

    def test_write_dataframe_to_gcs_avro_file(self, mocker, dataframe):
        """Test write_dataframe_to_avro_file method"""
        mock_write = mocker.patch("cleansweep.core.fileio.gcs_avro_write")

        write_dataframe_to_avro_file(dataframe, "gs://some_bucket/test.avro")

        mock_write.assert_called_once()

    def test_invalid_file_type(self, dataframe):
        """Test write_dataframe_to_avro_file method"""
        # not implemented file type
        with pytest.raises(NotImplementedError):
            write_dataframe_to_avro_file(dataframe, "file://test.txt")

    def test_invalid_url_type(self, dataframe):
        """Test write_dataframe_to_avro_file method"""
        # not implemented url type
        with pytest.raises(NotImplementedError):
            write_dataframe_to_avro_file(dataframe, "ftp://test.avro")


class TestWriteDataframeToParquetFile:
    """Test suite write_dataframe_to_parquet_file function"""

    @pytest.fixture(autouse=True, scope="class")
    def dataframe(self):
        """Test fixture for dataframe"""
        yield pd.DataFrame(
            [
                {
                    "id": "1",
                    "name": "test",
                    "content": "test",
                    "metadata_url": "https://test.com",
                }
            ]
        )

    def test_func(self, tmp_path, dataframe):
        """Test write_dataframe_to_parquet_file method"""
        path = tmp_path / "test.parquet"
        write_dataframe_to_parquet_file(dataframe, "file://" + str(path))

        assert path.exists()

    def test_write_dataframe_to_gcs_parquet_file(self, mocker, dataframe):
        """Test write_dataframe_to_parquet_file method"""
        mock_write = mocker.patch("cleansweep.core.fileio.gcs_parquet_write")

        write_dataframe_to_parquet_file(dataframe, "gs://some_bucket/test.parquet")

        mock_write.assert_called_once()

    def test_invalid_file_type(self, dataframe):
        """Test write_dataframe_to_parquet_file method"""
        # not implemented file type
        with pytest.raises(NotImplementedError):
            write_dataframe_to_parquet_file(dataframe, "file://test.txt")

    def test_invalid_url_type(self, dataframe):
        """Test write_dataframe_to_parquet_file method"""
        # not implemented url type
        with pytest.raises(NotImplementedError):
            write_dataframe_to_parquet_file(dataframe, "ftp://test.parquet")


class TestWriteDataframeToNewlineDelimitedJsonFile:
    """Test suite write_dataframe_to_newline_delimited_json_file function"""

    @pytest.fixture(autouse=True, scope="class")
    def dataframe(self):
        """Test fixture for dataframe"""
        yield pd.DataFrame(
            [
                {
                    "id": "1",
                    "name": "test",
                    "content": "test",
                    "metadata_url": "https://test.com",
                }
            ]
        )

    def test_func_json(self, tmp_path, dataframe):
        """Test write_dataframe_to_newline_delimited_json_file method"""
        path = tmp_path / "test.nd.json"
        write_dataframe_to_newline_delimited_json_file(dataframe, "file://" + str(path))

        assert path.exists()

    def test_func_ndjson(self, tmp_path, dataframe):
        """Test write_dataframe_to_newline_delimited_json_file method"""
        path = tmp_path / "test.ndjson"
        write_dataframe_to_newline_delimited_json_file(dataframe, "file://" + str(path))

        assert path.exists()

    def test_func_gs_ndjson(self, mocker, dataframe):
        """Test write_dataframe_to_newline_delimited_json_file method"""
        mock_write = mocker.patch("cleansweep.core.fileio.gcs.write")

        write_dataframe_to_newline_delimited_json_file(
            dataframe, "gs://some_bucket/test.ndjson"
        )

        mock_write.assert_called_once()

    def test_func_gs_json(self, mocker, dataframe):
        """Test write_dataframe_to_newline_delimited_json_file method"""
        mock_write = mocker.patch("cleansweep.core.fileio.gcs.write")

        write_dataframe_to_newline_delimited_json_file(
            dataframe, "gs://some_bucket/test.nd.json"
        )

        mock_write.assert_called_once()

    def test_invalid_file_type(self, dataframe):
        """Test write_dataframe_to_newline_delimited_json_file method"""
        # not implemented file type
        with pytest.raises(NotImplementedError):
            write_dataframe_to_newline_delimited_json_file(dataframe, "file://test.txt")

    def test_invalid_url_type(self, dataframe):
        """Test write_dataframe_to_newline_delimited_json_file method"""
        # not implemented url type
        with pytest.raises(NotImplementedError):
            write_dataframe_to_newline_delimited_json_file(
                dataframe, "ftp://test.ndjson"
            )


class TestReadCuratedFileToDataframe:
    """Test suite read_curated_file_to_dataframe function"""

    def test_read_curated_file_to_dataframe(self, tmp_path):
        """Test read_curated_file_to_dataframe method"""
        # prep an avro file
        path = tmp_path / "source.json"
        path.write_text(
            (
                """[{"id": "1", "name": "test", "content": [{"data": ["test"]}],"""
                """ "metadata": {"url": "https://test.com"}}]"""
            )
        )

        df, _ = read_documents_file_to_dataframe("file://" + str(path))

        path = tmp_path / "test.avro"
        write_dataframe_to_avro_file(df, "file://" + str(path))

        # test avro file
        df = read_curated_file_to_dataframe("file://" + str(path))

        assert df.loc[0, "id"] == "1"


class TestReadFileToDict:
    """Test suite read_file_to_dict function"""

    @pytest.mark.parametrize(
        "text, ext",
        [
            pytest.param(
                """{"id": "1", "name": "test", "content": [{"data": ["test"]}], "metadata": {}}""",
                "json",
                id="json",
            ),
            pytest.param(
                """id,name,content,metadata\n"1",test,"[{""data"": [""test""]}]",{}""",
                "csv",
                id="csv",
            ),
        ],
    )
    def test_read_file_to_dict(self, text, ext, tmp_path):
        """Test read_file_to_dict method"""
        # test json file
        path = tmp_path / f"test.{ext}"
        path.write_text(text)

        document = read_file_to_dict("file://" + str(path))[0]

        assert str(document["id"]) == "1"
        assert document["name"] == "test"
        assert document["content"][0]["data"] == ["test"]

    def test_invalid_json_path(self, tmp_path):
        """Test read_file_to_dict method with invalid path"""
        path = tmp_path / "test.json"
        path.write_text(
            """{"id": "1", "name": "test", "content": [{"data": ["test"]}], "metadata": {}}"""
        )

        # test failing path param
        with pytest.raises(ValueError):
            read_file_to_dict("file://" + str(path), "$.documents")

    def test_ndjson_with_path(self, tmp_path):
        """Test read_file_to_dict method with ndjson file with path"""
        # test ndjson file with path
        path = tmp_path / "test.json"
        path.write_text(
            (
                """{"documents": [{"id": "1", "name": "test", """
                """"content": [{"data": ["test"]}], "metadata": {}}]}"""
            )
        )

        documents = read_file_to_dict("file://" + str(path), "$.documents")

        assert len(documents) == 1

    def test_ndjson(self, tmp_path):
        """Test read_file_to_dict method with ndjson file"""
        path = tmp_path / "test.ndjson"
        path.write_text(
            (
                """{"id": "1", "name": "test", "content": [{"data": ["test"]}], "metadata": {}}\n"""
                """{"id": "2", "name": "test", "content": [{"data": ["test"]}], "metadata": {}}"""
            )
        )

        documents = read_file_to_dict("file://" + str(path))

        assert len(documents) == 2
        assert documents[1]["id"] == "2"

    def test_invalid_extension(self):
        """Test read_file_to_dict method with invalid path"""
        with pytest.raises(NotImplementedError):
            read_file_to_dict("file://test.txt")

    def test_invalid_url_type(self):
        """Test read_file_to_dict method with invalid path"""
        with pytest.raises(NotImplementedError):
            read_file_to_dict("ftp://test.json")

    def test_parquet(self, tmp_path):
        """Test read_file_to_dict method with parquet file"""
        path = tmp_path / "test.parquet"
        df = pd.DataFrame(
            [
                {
                    "id": "1",
                    "name": "test",
                    "content": [{"data": ["test"]}],
                }
            ]
        )
        table = pa.Table.from_pandas(df)
        pq.write_table(table, path)

        documents = read_file_to_dict("file://" + str(path))

        assert len(documents) == 1
        assert documents[0]["id"] == "1"


class TestCreateGlobPattern:
    @pytest.mark.parametrize(
        "directory, run_id, prefix, extension, file_name, expected",
        [
            # All parameters are None.
            (None, None, None, None, None, "*"),
            # Only directory provided and without a trailing slash.
            ("foo", None, None, None, None, "foo/*"),
            # Directory already ends with a slash.
            ("foo/", None, None, None, None, "foo/*"),
            # Only file name provided (other parameters default).
            (None, None, None, None, "data", "data"),
            # Prefix provided without trailing underscore combined with file name.
            (None, None, "bar", None, "file", "bar_file"),
            # Prefix provided with trailing underscore (should remain unchanged).
            (None, None, "bar_", None, "file", "bar_file"),
            # Run ID provided along with a file name.
            (None, "123", None, None, "file", "file_123"),
            # All parameters provided: directory missing slash, prefix missing underscore, extension missing dot.
            ("dir", "r1", "pre", "txt", "file", "dir/pre_file_r1.txt"),
            # Directory with slash, run ID provided as an empty string, prefix provided, extension already starts with a dot.
            ("dir/", "", "pre", ".py", "file", "dir/pre_file_.py"),
            # Edge case: extension provided as an empty string (non-None) results in an extra dot.
            (None, None, None, "", None, "*."),
        ],
    )
    def test_create_glob_pattern(
        self, directory, run_id, prefix, extension, file_name, expected
    ):
        result = create_glob_pattern(directory, run_id, prefix, extension, file_name)
        assert result == expected
