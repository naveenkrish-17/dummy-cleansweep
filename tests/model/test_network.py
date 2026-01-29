"""Tests for the network module."""

from pathlib import Path

import pytest
from pydantic import BaseModel

from cleansweep.model.network import (
    CloudStorageUrl,
    FileUrl,
    FtpUrl,
    HttpUrl,
    Url,
    convert_to_url,
    file_type,
    isurlinstance,
    raw_path,
)


class TestCloudStorageUrl:
    """Test suite for the CloudStorageUrl class."""

    class Model(BaseModel):
        """Test model for the CloudStorageUrl class."""

        url: CloudStorageUrl

    def test_cloud_storage_url_valid(self):
        """Test valid cloud_storage URLs."""
        valid_urls = [
            "gs://example_bucket/folder/file.txt",
            "gs://example_bucket/file.txt",
        ]
        for url in valid_urls:
            doc_url = CloudStorageUrl(url)
            assert url in str(doc_url)
            assert (
                doc_url.scheme  # pylint: disable=no-member
                == url.split(":", maxsplit=1)[0]
            )
            assert doc_url.host == "example_bucket"  # pylint: disable=no-member

    def test_cloud_storage_url_invalid_scheme(self):
        """Test invalid cloud_storage URL schemes."""
        with pytest.raises(ValueError):
            self.Model(url="file://example.com")

    def test_cloud_storage_url_missing_host(self):
        """Test cloud_storage URL without a host."""
        with pytest.raises(ValueError):
            self.Model(url="https://")


class TestFtpUrl:
    """Test suite for the FtpUrl class."""

    class Model(BaseModel):
        """Test model for the FtpUrl class."""

        url: FtpUrl

    def test_ftp_url_valid(self):
        """Test valid ftp URLs."""
        valid_urls = [
            "ftp://example.com",
            "sftp://example.com",
        ]
        for url in valid_urls:
            doc_url = FtpUrl(url)
            assert url in str(doc_url)
            assert (
                doc_url.scheme  # pylint: disable=no-member
                == url.split(":", maxsplit=1)[0]
            )
            assert doc_url.host == "example.com"  # pylint: disable=no-member

    def test_ftp_url_invalid_scheme(self):
        """Test invalid ftp URL schemes."""
        with pytest.raises(ValueError):
            self.Model(url="file://example.com")

    def test_ftp_url_missing_host(self):
        """Test ftp URL without a host."""
        with pytest.raises(ValueError):
            self.Model(url="https://")


class TestIsUrlInstance:
    """Test suite for the isurlinstance function."""

    def test_isurlinstance_cloud_storage_url(self):
        """Test that isurlinstance correctly identifies a CloudStorageUrl."""
        url = "gs://bucket/path/to/file"
        assert isurlinstance(url, CloudStorageUrl) is True
        assert isurlinstance(url, FileUrl) is False

    def test_isurlinstance_file_url(self):
        """Test that isurlinstance correctly identifies a FileUrl."""
        url = "file:///path/to/file"
        assert isurlinstance(url, FileUrl) is True
        assert isurlinstance(url, FtpUrl) is False

    def test_isurlinstance_ftp_url(self):
        """Test that isurlinstance correctly identifies an FtpUrl."""
        url = "ftp://ftp.example.com/path/to/file"
        assert isurlinstance(url, FtpUrl) is True
        assert isurlinstance(url, HttpUrl) is False

    def test_isurlinstance_http_url(self):
        """Test that isurlinstance correctly identifies an HttpUrl."""
        url = "https://example.com/path/to/file"
        assert isurlinstance(url, HttpUrl) is True
        assert isurlinstance(url, CloudStorageUrl) is False


class TestConvertToUrl:
    """Test suite for the convert_to_url function."""

    def testget_path_string(self):
        """Test that convert_to_url correctly converts a string to a Url."""
        url = convert_to_url("gs://bucket/path/to/file")
        assert isinstance(url, Url)
        assert url.scheme == "gs"
        assert url.host == "bucket"
        assert url.path == "/path/to/file"

    def testget_path_cloud_storage_url(self):
        """Test that convert_to_url correctly handles a CloudStorageUrl."""
        input_url = "gs://bucket/path/to/file"
        url = convert_to_url(input_url)
        assert isinstance(url, Url)
        assert url.scheme == "gs"
        assert url.host == "bucket"
        assert url.path == "/path/to/file"

    def testget_path_file_url(self):
        """Test that convert_to_url correctly handles a FileUrl."""
        input_url = "file:///path/to/file"
        url = convert_to_url(input_url)
        assert isinstance(url, Url)
        assert url.scheme == "file"
        assert url.host is None
        assert url.path == "/path/to/file"

    def testget_path_ftp_url(self):
        """Test that convert_to_url correctly handles an FtpUrl."""
        input_url = "ftp://ftp.example.com/path/to/file"
        url = convert_to_url(input_url)
        assert isinstance(url, Url)
        assert url.scheme == "ftp"
        assert url.host == "ftp.example.com"
        assert url.path == "/path/to/file"

    def testget_path_http_url(self):
        """Test that convert_to_url correctly handles an HttpUrl."""
        input_url = "http://example.com/path/to/file"
        url = convert_to_url(input_url)
        assert isinstance(url, Url)
        assert url.scheme == "http"
        assert url.host == "example.com"
        assert url.path == "/path/to/file"


class TestFileType:
    """Test suite for the file_type function."""

    def testfile_type_cloud_storage_url(self):
        """Test that file_type correctly gets the file type of a CloudStorageUrl."""
        url = convert_to_url("gs://bucket/path/to/file.txt")
        result = file_type(url)
        assert result == Path(url.path).suffix

    def testfile_type_file_url(self):
        """Test that file_type correctly gets the file type of a FileUrl."""
        url = convert_to_url("file:///path/to/file.txt")
        result = file_type(url)
        assert result == Path(url.path).suffix

    def testfile_type_ftp_url(self):
        """Test that file_type correctly gets the file type of an FtpUrl."""
        url = convert_to_url("ftp://ftp.example.com/path/to/file.txt")
        result = file_type(url)
        assert result == Path(url.path).suffix

    def testfile_type_http_url(self):
        """Test that file_type correctly gets the file type of an HttpUrl."""
        url = convert_to_url("http://example.com/path/to/file.txt")
        result = file_type(url)
        assert result == Path(url.path).suffix


class TestRawPath:
    """Test suite for the raw_path function."""

    def test_raw_path_file_url(self):
        """Test that raw_path correctly gets the raw path of a FileUrl."""
        url = convert_to_url("file:///path/to/file.txt")
        result = raw_path(url)
        assert result == "/path/to/file.txt"

        url = convert_to_url("file://path/to/file.txt")
        result = raw_path(url)
        assert result == "/path/to/file.txt"

    def test_raw_path_cloud_storage_url(self):
        """Test that raw_path correctly gets the raw path of a CloudStorageUrl."""
        url = convert_to_url("gs://bucket/path/to/file.txt")
        result = raw_path(url)
        assert result == "bucket/path/to/file.txt"
