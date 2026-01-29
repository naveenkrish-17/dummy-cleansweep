"""Model definitions for the EM API."""

__all__ = [
    "EmSourceDocument",
    "EmQAMetadata",
    "EmQA",
    "EmbeddedChunk",
    "EmbeddedChunkMetadata",
    "SourceDocument",
]

from pydantic import BaseModel, Field, HttpUrl


class SourceDocument(BaseModel):
    """Metadata for a source document."""

    title: str = Field(description="Title of the source document")
    description: str = Field(description="Summary of the source document")
    url: HttpUrl = Field(description="URL of the source document")
    is_public: bool | None = Field(
        default=None, description="True if the source document is public"
    )


class EmbeddedChunkMetadata(SourceDocument):
    """Metadata for a chunk of text with its embedding (inherits from SourceDocument)."""

    dedup_id: str | None = Field(
        default=None, description="Identifier used for deduplication"
    )
    root_source: SourceDocument | None = Field(
        default=None,
        description="Metadata for the root source document",
    )


class EmbeddedChunk(BaseModel):
    """A chunk of text with its embedding and metadata."""

    id: str = Field(description="Unique identifier for this embedding")
    metadata: EmbeddedChunkMetadata = Field(
        description="The chunk's flat metadata with an optional `dedup_id` value"
    )
    document: str = Field(description="The content of the chunk")
    embedding: list[float] = Field(description="The embedding vector of the chunk")


class EmSourceDocument(SourceDocument):
    """Metadata for the source document used in the EM API."""

    article_id: str = Field(description="Unique identifier for the source document")
    root_source: SourceDocument | None = Field(
        default=None,
        description="Metadata for the root source document",
    )


class EmQAMetadata(BaseModel):
    """Metadata for a question-answer pair."""

    question: str = Field(description="Question to be answered")
    answer: str = Field(description="Answer to the question")
    sufficient_references: list[int] = Field(
        description=(
            "List of sources that contain all of the QA's information expressed as a list "
            "of indices into `references`"
        )
    )
    references: list[EmSourceDocument] = Field(
        description="Complete list of references for the sources of the QA pair"
    )
    dedup_id: str | None = Field(
        default=None, description="Identifier used for deduplication"
    )


class EmQA(BaseModel):
    """Question-Answer pair for the EM API."""

    id: str = Field(description="Unique identifier for this QA pair")
    metadata: EmQAMetadata = Field(
        description="The chunk's flat metadata with an optional `dedup_id` value"
    )
    # document: str = Field(description="The content of the embedded document")
    embedding: list[float] = Field(description="The embedding vector of the QA pair")
