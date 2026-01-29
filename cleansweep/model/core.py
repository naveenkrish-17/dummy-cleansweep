"""The `cleansweep.model.core` module contains the target data model for the application."""

__all__ = ["ContentModel", "DocumentModel", "MetadataModel", "TableModel", "Defaults"]

import hashlib
import warnings
from datetime import datetime
from typing import Any, Literal, Optional, Union
from uuid import uuid4

import bs4
import html2text
import pytz
from bs4 import MarkupResemblesLocatorWarning
from pydantic import BaseModel, computed_field, model_validator

from cleansweep.enumerations import Classification, ContentType, DocumentType
from cleansweep.iso.languages import Language
from cleansweep.iso.regions import Country
from cleansweep.model.network import CloudStorageUrl, FtpUrl, HttpUrl

__pdoc__ = {
    "DocumentModel.model_computed_fields": False,
    "DocumentModel.model_config": False,
    "DocumentModel.model_fields": False,
    "MetadataModel.model_computed_fields": False,
    "MetadataModel.model_config": False,
    "MetadataModel.model_fields": False,
    "TableModel.model_computed_fields": False,
    "TableModel.model_config": False,
    "TableModel.model_fields": False,
    "ContentModel.model_computed_fields": False,
    "ContentModel.model_config": False,
    "ContentModel.model_fields": False,
}

warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning, module="bs4")


class Defaults:
    """Default values for the metadata."""

    language = (
        Language.English  # pyright: ignore[reportAttributeAccessIssue] # pylint: disable=no-member
    )
    classification = Classification.PROTECTED
    document_type = DocumentType.KNOWLEDGE
    delimiter: str | None = None


class MetadataModel(BaseModel, extra="allow"):  # pylint: disable=too-few-public-methods
    """Metadata for a document.

    Additional fields are allowed.
    """

    created: Optional[datetime] = datetime(1900, 1, 1, 0, 0, 0, 0, tzinfo=pytz.utc)
    modified: Optional[datetime] = datetime(1900, 1, 1, 0, 0, 0, 0, tzinfo=pytz.utc)
    expiry: Optional[datetime] = datetime(
        2260, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc
    )  # pandas max datetime is 2262-04-11 23:47:16.854775807
    tags: Optional[list[str]] = []
    access_groups: Optional[list[str]] = []
    classification: Optional[Classification] = None
    description: Optional[str] = None
    language: Optional[Language] = None
    type: Optional[DocumentType] = None
    region: Optional[list[Country]] = None
    url: Optional[Union[CloudStorageUrl, FtpUrl, HttpUrl]] = None

    root_document_id: Optional[str] = None

    def model_post_init(self, _context: Any) -> None:
        """Post-initialization hook for the model."""
        if self.language is None:
            self.language = Defaults.language

        if self.classification is None:
            self.classification = Defaults.classification

        if self.type is None:
            self.type = Defaults.document_type


class TableModel(BaseModel):
    """Table for a document."""

    title: Optional[str] = None
    columns: list[str] = []
    rows: list[list[str]]
    metadata: Optional[MetadataModel] = None

    @computed_field
    @property
    def content_full(self) -> str:
        """Full content string by combining the title and raw content with appropriate delimiters.

        Returns:
            str: The full content string with title and raw content, separated by delimiters and
                paragraph breaks.

        """
        content = []
        delimiter = Defaults.delimiter
        if self.title:
            content.append("\n")  # paragraph break
            if delimiter:
                content.append(f"{delimiter} # {self.title}")
            else:
                content.append(f"# {self.title}")
        elif delimiter:
            content.append("\n")  # paragraph break
            content.append(delimiter)
        content.append(self.content_raw)
        return "\n".join(content)

    @computed_field
    @property
    def content_raw(self) -> str:
        """Raw content string representation of the object's columns and rows.

        Returns:
            str: A string with each element of columns and rows converted to a string,
                 separated by newlines. If an element is None, it is replaced with an empty string.

        """
        content = [
            "" if structure is None else str(structure)
            for structure in [self.columns, self.rows]
        ]
        return "\n".join(content)

    @computed_field
    @property
    def html_content(self) -> str:
        """Converts the full HTML content to plain text.

        This method uses the html2text library to convert the HTML content stored
        in `self.content_raw` to plain text while ignoring any links present in
        the HTML.

        Returns:
            str: The plain text representation of the HTML content.

        """
        h = html2text.HTML2Text()
        h.ignore_links = True
        return h.handle(self.content_raw)

    @computed_field
    @property
    def content_type(self) -> ContentType:
        """Flag to indicate if the document requires access control."""
        return _get_content_type(self)


class ContentModel(BaseModel):  # pylint: disable=too-few-public-methods
    """Content for a document."""

    title: Optional[str] = None
    data: list[Union[str, "ContentModel", TableModel]]
    metadata: Optional[MetadataModel] = None

    @computed_field
    @property
    def content_raw(self) -> str:
        """Raw content string.

        Returns:
            str: A string containing concatenated content from `self.data`, separated by newline
                characters.

        """
        content = []
        for item in self.data:  # pylint: disable=not-an-iterable
            if isinstance(item, str):
                content.append(item)

            elif isinstance(item, (ContentModel, TableModel)):
                content.append(item.content_raw)

        return "\n".join(content)

    @computed_field
    @property
    def content_full(self) -> str:
        """Full content as a single string.

        This method constructs the content by iterating over the data items and
        appending them to a list. It handles different types of items, including
        strings and instances of `ContentModel` or `TableModel`. The content is
        then joined into a single string with newline characters.

        Returns:
            str: The full content as a single string.

        """
        h = html2text.HTML2Text()
        h.ignore_links = True
        is_html = False
        if bs4.BeautifulSoup(self.content_raw, "html.parser").find_all():
            is_html = True

        content = []
        delimiter = Defaults.delimiter
        if self.title:
            content.append("\n")  # paragraph break
            if delimiter:
                content.append(f"{delimiter} # {self.title}")
            else:
                content.append(f"# {self.title}")
        elif delimiter:
            content.append("\n")  # paragraph break
            content.append(delimiter)

        for item in self.data:  # pylint: disable=not-an-iterable
            if isinstance(item, str):
                content.append(h.handle(item) if is_html else item)

            elif isinstance(item, (ContentModel, TableModel)):
                content.append(item.content_full)

        return "\n".join(content)

    @computed_field
    @property
    def html_content(self) -> str:
        """Converts the full HTML content to plain text.

        This method uses the html2text library to convert the HTML content stored
        in `self.content_raw` to plain text while ignoring any links present in
        the HTML.

        Returns:
            str: The plain text representation of the HTML content.

        """
        h = html2text.HTML2Text()
        h.ignore_links = True
        is_html = False
        if bs4.BeautifulSoup(self.content_raw, "html.parser").find_all():
            is_html = True

        content = []
        for item in self.data:  # pylint: disable=not-an-iterable
            if isinstance(item, str):
                content.append(item)

            elif isinstance(item, (ContentModel, TableModel)):
                content.append(item.content_raw)

        output = "\n".join(content)
        return h.handle(output) if is_html else output

    @computed_field
    @property
    def content_type(self) -> ContentType:
        """Flag to indicate if the document requires access control."""
        return _get_content_type(self, "data")


class DocumentModel(BaseModel):  # pylint: disable=too-few-public-methods
    """Represents a document."""

    name: str
    id: str = str(uuid4())
    metadata: MetadataModel = MetadataModel()
    content: list[Union[ContentModel, TableModel]]
    md5: Optional[str] = None
    """The MD5 hash of the document."""
    action: Optional[Literal["D", "I", "U", "N", None]] = None
    """The action associated with the document. Can be one of the following values: "D" (delete),
    "I" (insert), "U" (update), "N" (no action), or None.
    """
    metadata_md5: Optional[str] = None
    """The MD5 hash of the metadata."""
    metadata_is_modified: Optional[bool] = None
    content_md5: Optional[str] = None
    """The MD5 hash of the content."""
    content_is_modified: Optional[bool] = None

    @computed_field
    @property
    def content_full(self) -> str:
        """Full content string.

        Returns:
            str: The full content string with title and raw content.

        """
        return "\n".join([item.content_full for item in self.content])

    @computed_field
    @property
    def content_raw(self) -> str:
        """Return the raw content of the document.

        Processes the raw content of the items in the content list by joining them into a single
        string, parsing it as HTML, and removing all attributes from the tags except for 'src' in
        'img' tags and 'href' in 'a' tags.

        Returns:
            str: The processed HTML content as a string with attributes removed from tags.

        """
        content = [item.content_raw for item in self.content]
        text = "\n".join(content)

        soup = bs4.BeautifulSoup(text, "html.parser")

        # Iterate through all tags and remove their attributes
        for tag in soup.find_all(True):
            if not hasattr(tag, "attrs"):
                continue

            if (
                "src" in tag.attrs  # pyright: ignore[reportAttributeAccessIssue]
                and tag.name == "img"  # pyright: ignore[reportAttributeAccessIssue]
            ) or (  # pyright: ignore[reportAttributeAccessIssue]
                "href" in tag.attrs  # pyright: ignore[reportAttributeAccessIssue]
                and tag.name == "a"  # pyright: ignore[reportAttributeAccessIssue]
            ):
                # keep image sources and anchors
                continue

            tag.attrs = {}  # pyright: ignore[reportAttributeAccessIssue]

        return str(soup)

    @computed_field
    @property
    def html_content(self) -> str:
        """Converts the full HTML content to plain text.

        This method uses the html2text library to convert the HTML content stored
        in `self.content_raw` to plain text while ignoring any links present in
        the HTML.

        Returns:
            str: The plain text representation of the HTML content.

        """
        content = [item.html_content for item in self.content]
        return "\n".join(content)

    @computed_field
    @property
    def length_full(self) -> int:
        """Length of the full content.

        Returns:
            int: The length of the full content string.

        """
        return len(self.content_full)

    @computed_field
    @property
    def length_raw(self) -> int:
        """Length of the raw content.

        Returns:
            int: The length of the raw content string.

        """
        return len(self.content_raw)

    @computed_field
    @property
    def length_html(self) -> int:
        """Length of the HTML content.

        Returns:
            int: The length of the HTML content string.

        """
        return len(self.html_content)

    @computed_field
    @property
    def content_type(self) -> ContentType:
        """Flag to indicate if the document requires access control.

        Returns
            ContentType: The content type of the document.

        """
        if self.metadata.classification != Classification.PUBLIC:
            return ContentType.PRIVATE

        return _get_content_type(self)

    @model_validator(mode="after")
    def set_md5(self):
        """Set the MD5 hash of the document.

        Returns
            DocumentModel: The updated DocumentModel instance.

        """
        if self.md5 is None:
            self.md5 = hashlib.md5(self.model_dump_json().encode("utf-8")).hexdigest()
        if self.metadata_md5 is None:
            self.metadata_md5 = hashlib.md5(
                self.metadata.model_dump_json().encode("utf-8")
            ).hexdigest()
        if self.content_md5 is None:
            self.content_md5 = hashlib.md5(self.content_raw.encode("utf-8")).hexdigest()
        return self


def _get_content_type(
    model: DocumentModel | ContentModel | TableModel,
    attr: Literal["content", "data"] | None = None,
) -> ContentType:
    """Get the content type of the model.

    Args:
        model (Union[ContentModel,TableModel]): The model to check.
        attr (str): The attribute to check.

    """
    if (
        model.metadata is not None
        and model.metadata.access_groups is not None
        and len(model.metadata.access_groups) > 0
    ):
        return ContentType.PRIVATE

    if attr is None:
        attr = "content"

    if hasattr(model, attr):
        for item in getattr(model, attr):
            if (
                hasattr(item, "content_type")
                and item.content_type != ContentType.PUBLIC
            ):
                return ContentType.MIXED

    return ContentType.PUBLIC
