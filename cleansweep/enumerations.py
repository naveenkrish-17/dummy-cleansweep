"""Module for enumerations used in the app."""

from enum import Enum


class ServiceLevel(Enum):
    """Enumeration of the service levels for a document."""

    MISSION_CRITICAL = "mission critical"
    BUSINESS_CRITICAL = "business critical"
    BUSINESS_IMPORTANT = "business important"
    BUSINESS_SUPPORT = "business support"


class TextSplitter(Enum):
    """Enumerations for text splitter."""

    RECURSIVE = "recursive"
    NLTK = "nltk"
    SPACY = "spacy"
    HTML = "html"


class EmbedderType(Enum):
    """Enumeration of the types of embedders that can be used to embed documents."""

    VERTEX = "vertex"
    OPENAI = "openai"


class Classification(Enum):
    """The classification of a document."""

    PUBLIC = "PUBLIC"
    NOT_PROTECTED = "NOT_PROTECTED"
    PROTECTED = "PROTECTED"
    CRITICAL = "CRITICAL"


class DocumentType(Enum):
    """The type of document."""

    KNOWLEDGE = "KNOWLEDGE"
    NEWS = "NEWS"


class ContentType(Enum):
    """The type of content."""

    PRIVATE = 0
    PUBLIC = 1
    MIXED = 2


class RuleType(Enum):
    """Enumeration of the types of rules that can be applied to a document."""

    REPLACE_SUBSTRINGS = "replace_substrings"
    REMOVE_SUBSTRINGS = "remove_substrings"
    REMOVE_NULL_OR_EMPTY = "remove_null_or_empty"
    FILTER_BY_DATE_RANGE = "filter_by_date_range"
    EXCLUDE_BY_DATE_RANGE = "exclude_by_date_range"
    FILTER_BY_COLUMNS = "filter_by_columns"
    FILTER_BY_COLUMN = "filter_by_column"
    FILTER_BY_MATCH = "filter_by_match"
    DOCUMENTS_CLEAN = "documents_clean"
    REMOVE_BY_MATCH = "remove_by_match"
    REMOVE_DUPLICATES = "remove_duplicates"
    REFERENCE_TO_INLINE = "reference_to_inline"


class LoadType(Enum):
    """Enumeration of the types of loads that can be performed on a dataset."""

    FULL = "FULL"
    DELTA = "DELTA"
    INCREMENTAL = "INCREMENTAL"
