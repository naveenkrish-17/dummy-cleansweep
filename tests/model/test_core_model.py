"""Tests for the core models.
"""

from datetime import datetime

from cleansweep.enumerations import Classification, ContentType
from cleansweep.model.core import ContentModel, DocumentModel, MetadataModel, TableModel


class TestMetadataModel:
    """Test suite for the metadata model."""

    def test_metadata_model(self):
        """Tests that the metadata model is created correctly and accepts additional fields."""
        test_config = {
            "created": "2021-01-01T00:00:00",
            "modified": "2021-01-01T00:00:00",
            "tags": ["test"],
            "access_groups": ["test"],
            "classification": "PROTECTED",
            "keywords": ["test", "test2"],
        }
        metadata = MetadataModel(**test_config)
        assert metadata.keywords == ["test", "test2"]
        assert metadata.access_groups == ["test"]
        assert metadata.classification == Classification.PROTECTED
        assert metadata.created == datetime(2021, 1, 1)
        assert metadata.modified == datetime(2021, 1, 1)
        assert metadata.tags == ["test"]


class TestContentModel:
    """Test suite for the content model."""

    def test_content_model_minimum(self):
        """Test that the content model is created correctly with the minimum required fields."""
        content = ContentModel(data=["test_data"])
        assert content.title is None
        assert content.data == ["test_data"]
        assert content.metadata is None

    def test_content_raw(self):
        """Test `content_raw` method"""
        content = ContentModel(data=["test_data"], title="test_title")
        assert content.content_raw == "test_data"

        content = ContentModel(data=["test_data"])
        assert content.content_raw == "test_data"

    def test_content_type(self):
        """Test `content_type` method"""
        content = ContentModel(data=["test_data"], title="test_title")
        assert content.content_type == ContentType.PUBLIC

        content = ContentModel(
            data=["test_data"],
            title="test_title",
            metadata=MetadataModel(access_groups=[]),
        )
        assert content.content_type == ContentType.PUBLIC

        content = ContentModel(
            data=["test_data"],
            title="test_title",
            metadata=MetadataModel(access_groups=["test"]),
        )
        assert content.content_type == ContentType.PRIVATE


class TestDocumentModel:
    """Test suite for the document model."""

    def test_document_model(self):
        """Test that the document model is created correctly."""
        metadata = MetadataModel(key="test_key", value="test_value")
        content = ContentModel(data=["test_data"])
        document = DocumentModel(name="test_name", metadata=metadata, content=[content])

        assert document.name == "test_name"
        assert isinstance(document.id, str)
        assert document.metadata == metadata
        assert document.content == [content]

    def test_content_raw(self):
        """Test `content_raw` method"""
        metadata = MetadataModel(key="test_key", value="test_value")
        content = ContentModel(data=["test_data"], title="test_title")
        document = DocumentModel(name="test_name", metadata=metadata, content=[content])
        assert document.content_raw == "test_data"

        content = ContentModel(data=["test_data"])
        document = DocumentModel(name="test_name", metadata=metadata, content=[content])
        assert document.content_raw == "test_data"

    def test_content_type(self):
        """Test `content_type` method"""
        metadata = MetadataModel(
            key="test_key", value="test_value", classification="PUBLIC"
        )
        content = ContentModel(data=["test_data"], title="test_title")
        document = DocumentModel(name="test_name", metadata=metadata, content=[content])
        assert document.content_type == ContentType.PUBLIC

        metadata = MetadataModel(
            key="test_key",
            value="test_value",
            access_groups=["test"],
            classification="PUBLIC",
        )
        content = ContentModel(data=["test_data"], title="test_title")
        document = DocumentModel(name="test_name", metadata=metadata, content=[content])
        assert document.content_type == ContentType.PRIVATE

        metadata = MetadataModel(
            key="test_key",
            value="test_value",
            access_groups=[],
            classification="PUBLIC",
        )
        content_metadata = MetadataModel(access_groups=["test"])
        content = ContentModel(
            data=["test_data"], title="test_title", metadata=content_metadata
        )
        document = DocumentModel(name="test_name", metadata=metadata, content=[content])
        assert document.content_type == ContentType.MIXED

    def test_md5(self):
        """Test `md5` property"""
        metadata = MetadataModel(key="test_key", value="test_value")
        content = ContentModel(data=["test_data"], title="test_title")
        document = DocumentModel(name="test_name", metadata=metadata, content=[content])
        assert document.md5 is not None

        document.md5 = "test_md5"
        assert document.md5 == "test_md5"


class TestTableModel:
    """Test suite for the table model."""

    def test_table_model(self):
        """Test that the table model is created correctly."""
        metadata = MetadataModel(key="test_key", value="test_value")
        table = TableModel(columns=["column1"], rows=[["test_data"]], metadata=metadata)

        assert table.columns == ["column1"]
        assert table.rows == [["test_data"]]
        assert table.metadata == metadata

    def test_content_raw(self):
        """Test `content_raw` method"""
        table = TableModel(columns=["column1"], rows=[["test_data"]])
        assert table.content_raw == "['column1']\n[['test_data']]"

        table = TableModel(
            columns=["column1", "column2"],
            rows=[["test_data", "more data"], ["second row", "more data"]],
            title="test_title",
        )
        assert table.content_raw == (
            "['column1', 'column2']\n[['test_data', 'more data'], "
            "['second row', 'more data']]"
        )

    def test_content_type(self):
        """Test `content_type` method"""
        metadata = MetadataModel(key="test_key", value="test_value")
        table = TableModel(columns=["column1"], rows=[["test_data"]], metadata=metadata)
        assert table.content_type == ContentType.PUBLIC

        metadata = MetadataModel(
            key="test_key", value="test_value", access_groups=["test"]
        )
        table = TableModel(columns=["column1"], rows=[["test_data"]], metadata=metadata)
        assert table.content_type == ContentType.PRIVATE
