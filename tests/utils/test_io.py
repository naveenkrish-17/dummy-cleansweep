"""Test suite for the io module."""

import fastavro
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from cleansweep.utils.io import (
    avro_read,
    avro_read_lines,
    avro_write,
    gcs_avro_read,
    gcs_avro_read_lines,
    gcs_avro_write,
    gcs_parquet_read,
    gcs_parquet_read_lines,
    gcs_parquet_write,
    gcs_to_temp,
    parquet_read,
    parquet_read_lines,
)


class TestGCSToTemp:
    """Test suite for the gcs_to_temp function."""

    def test_gcs_to_temp(self, mocker):
        """Test that gcs_to_temp downloads a file from Google Cloud Storage and writes it to a
        temporary file.
        """
        mocker.patch("cleansweep.utils.io.gcs.download", return_value=None)

        url = "gs://bucket/path/to/file.txt"

        result = gcs_to_temp(url)

        assert result.name == "file.txt"


class TestAvroRead:
    """Test suite for the avro_read function."""

    def test_read(self, tmp_path):
        """Test that avro_read reads an Avro file correctly."""
        # prep an avro file
        path = tmp_path / "test.avro"

        schema = fastavro.parse_schema(
            {
                "type": "record",
                "name": "test",
                "fields": [{"name": "field1", "type": "string"}],
            }
        )

        with open(path, "wb") as f:
            fastavro.writer(f, schema, [{"field1": "value1"}, {"field1": "value2"}])

        # test avro_read
        result = avro_read(str(path))

        assert result == [{"field1": "value1"}, {"field1": "value2"}]


class TestGcsAvroRead:
    """Test suite for the gcs_avro_read function."""

    def test_read(self, mocker, tmp_path):
        """Test that gcs_avro_read reads an Avro file from Google Cloud Storage correctly."""
        # prep an avro file
        path = tmp_path / "test.avro"

        schema = fastavro.parse_schema(
            {
                "type": "record",
                "name": "test",
                "fields": [{"name": "field1", "type": "string"}],
            }
        )

        with open(path, "wb") as f:
            fastavro.writer(f, schema, [{"field1": "value1"}, {"field1": "value2"}])

        mocker.patch("cleansweep.utils.io.gcs_to_temp", return_value=path)

        # test avro_read
        result = gcs_avro_read(str(path))

        assert result == [{"field1": "value1"}, {"field1": "value2"}]


class TestParquetRead:
    """Test suite for the parquet_read function."""

    def test_read(self, tmp_path):
        """Test that parquet_read reads a Parquet file correctly."""
        # prep a parquet file
        path = tmp_path / "test.parquet"

        df = pd.DataFrame({"field1": ["value1", "value2"]})
        table = pa.Table.from_pandas(df)
        pq.write_table(table, path)

        # test parquet_read
        result = parquet_read(str(path))

        assert result == table


class TestGcsParquetRead:
    """Test suite for the gcs_parquet_read function."""

    def test_read(self, mocker, tmp_path):
        """Test that gcs_parquet_read reads a Parquet file correctly."""
        # prep a parquet file
        path = tmp_path / "test.parquet"

        df = pd.DataFrame({"field1": ["value1", "value2"]})
        table = pa.Table.from_pandas(df)
        pq.write_table(table, path)

        mocker.patch("cleansweep.utils.io.gcs_to_temp", return_value=path)

        # test parquet_read
        result = gcs_parquet_read(str(path))

        assert result == table


class TestAvroReadLines:
    """Test suite for the avro_read_lines function."""

    def test_read(self, tmp_path):
        """Test that avro_read_lines reads an Avro file correctly."""
        # prep an avro file
        path = tmp_path / "test.avro"

        schema = fastavro.parse_schema(
            {
                "type": "record",
                "name": "test",
                "fields": [{"name": "field1", "type": "string"}],
            }
        )

        with open(path, "wb") as f:
            fastavro.writer(f, schema, [{"field1": "value1"}, {"field1": "value2"}])

        # test avro_read
        result = list(avro_read_lines(str(path)))

        assert result == [{"field1": "value1"}, {"field1": "value2"}]


class TestGcsAvroReadLines:
    """Test suite for the gcs_avro_read_lines function."""

    def test_read(self, mocker, tmp_path):
        """Test that avro_read_lines reads an Avro file correctly."""
        # prep an avro file
        path = tmp_path / "test.avro"

        schema = fastavro.parse_schema(
            {
                "type": "record",
                "name": "test",
                "fields": [{"name": "field1", "type": "string"}],
            }
        )

        with open(path, "wb") as f:
            fastavro.writer(f, schema, [{"field1": "value1"}, {"field1": "value2"}])

        mocker.patch("cleansweep.utils.io.gcs_to_temp", return_value=path)

        # test avro_read
        result = list(gcs_avro_read_lines(str(path)))

        assert result == [{"field1": "value1"}, {"field1": "value2"}]


class TestParquetReadLines:
    """Test suite for the parquet_read_lines function."""

    def test_read(self, tmp_path):
        """Test that parquet_read_lines reads a Parquet file correctly."""
        # prep a parquet file
        path = tmp_path / "test.parquet"

        df = pd.DataFrame({"field1": ["value1", "value2"]})
        table = pa.Table.from_pandas(df)
        pq.write_table(table, path)

        # test parquet_read
        result = list(parquet_read_lines(str(path)))

        assert result == [{"field1": "value1"}, {"field1": "value2"}]


class TestGcsParquetReadLines:
    """Test suite for the gcs_parquet_read_lines function."""

    def test_read(self, mocker, tmp_path):
        """Test that gcs_parquet_read_lines reads a Parquet file correctly."""
        # prep a parquet file
        path = tmp_path / "test.parquet"

        df = pd.DataFrame({"field1": ["value1", "value2"]})
        table = pa.Table.from_pandas(df)
        pq.write_table(table, path)

        mocker.patch("cleansweep.utils.io.gcs_to_temp", return_value=path)

        # test parquet_read
        result = list(gcs_parquet_read_lines(str(path)))

        assert result == [{"field1": "value1"}, {"field1": "value2"}]


class TestAvroWrite:
    """Test suite for the avro_write function."""

    def test_avro_write(self, tmp_path):
        """Test that avro_write writes an Avro file correctly."""
        # prep a parquet file
        path = tmp_path / "test.avro"

        schema = {
            "type": "record",
            "name": "test",
            "fields": [{"name": "field1", "type": "string"}],
        }

        data = [{"field1": "value1"}, {"field1": "value2"}]

        # test avro_write
        avro_write(str(path), data, schema)

        with open(path, "rb") as f:
            result = list(fastavro.reader(f))

        assert result == data


class TestGcsAvroWrite:
    """Test suite for the gcs_avro_write function."""

    def test_write(self, mocker, tmp_path):
        """Test that gcs_avro_write writes an Avro file correctly."""
        # prep a parquet file
        path = tmp_path / "test.avro"

        schema = {
            "type": "record",
            "name": "test",
            "fields": [{"name": "field1", "type": "string"}],
        }

        data = [{"field1": "value1"}, {"field1": "value2"}]

        mock_upload = mocker.MagicMock()

        mocker.patch("cleansweep.utils.io.gcs.upload", mock_upload)

        # test avro_write
        gcs_avro_write(str(path), data, schema)
        assert mock_upload.called is True


class TestGcsParquetWrite:
    """Test suite for the gcs_parquet_write function."""

    def test_write(self, mocker, tmp_path):
        """Test that gcs_parquet_write writes an Parquet file correctly."""
        # prep a parquet file
        path = tmp_path / "test.avro"

        data = [{"field1": "value1"}, {"field1": "value2"}]

        mock_upload = mocker.MagicMock()

        mocker.patch("cleansweep.utils.io.gcs.upload", mock_upload)

        # test avro_write
        gcs_parquet_write(str(path), data)
        assert mock_upload.called is True
