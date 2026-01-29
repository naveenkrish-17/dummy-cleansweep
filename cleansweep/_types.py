"""Common type aliases for the Cleansweep package."""

from datetime import datetime, timedelta
from os import PathLike
from typing import (
    Any,
    Callable,
    Dict,
    Literal,
    Optional,
    Protocol,
    Tuple,
    TypeAlias,
    TypeVar,
)

import pandas as pd
from google.cloud.storage.bucket import Bucket
from openai.types.chat.chat_completion_named_tool_choice_param import (
    ChatCompletionNamedToolChoiceParam,
)
from openai.types.chat.chat_completion_tool_param import ChatCompletionToolParam
from pydantic import BaseModel, ValidationInfo, computed_field, field_validator

from cleansweep.chunk.utils import get_text_splitter
from cleansweep.enumerations import EmbedderType
from cleansweep.iso.languages import Language
from cleansweep.utils.azure.auth import AzureCredentials

Platform: TypeAlias = Literal["kosmo", "em"]


class PromptVariable(BaseModel):
    """A variable for a prompt."""

    name: Optional[str]
    """The name of the variable."""

    value: Optional[str]
    """The value of the variable."""


class Prompt(BaseModel):
    """A generic prompt schema."""

    prompt: Optional[str] = None
    """The prompt for the model."""

    template: Optional[str] = None
    """The template for the prompt."""

    name: str
    """The name of the prompt."""

    variables: list[PromptVariable] = []

    @property
    def variable_mapping(self) -> list[dict[str, str]]:
        """Get the variable mapping."""
        return [var.model_dump() for var in self.variables]


class Deployment(BaseModel):
    """A deployment configuration."""

    name: str
    """The name of the deployment."""
    tpm: int
    """The Tokens Per Minute (TPM) for the deployment."""
    model: str
    """The model for the deployment."""
    dimensions: Optional[int] = None
    (
        """The maximum dimensions returned by a text embedding model. """
        """*** Only used for text embeddings. ***"""
    )
    accepts_dimensions: bool = True
    """Whether the deployment accepts dimensions. *** Only used for text embeddings. ***"""


Token: TypeAlias = Tuple[str, bool | str | int | float | list[bool | str | int | float]]
"""Each Token is a tuple containing a JSON Path and the value of the element in the document."""

StrPath: TypeAlias = str | PathLike[str]

DataframeTypes: TypeAlias = (
    datetime
    | timedelta
    | str
    | int
    | float
    | bool
    | list[datetime | str | int | float | bool]
)


class ClusterDefinition(BaseModel):
    """A definition of a cluster."""

    columns_to_embed: list[str]

    @computed_field
    @property
    def cluster_name(self) -> str:
        """The name of the cluster column."""
        return f"cluster_{'_'.join(self.columns_to_embed)}"

    @computed_field
    @property
    def cluster_filter_name(self) -> str:
        """The name of the cluster filter column."""
        return f"{self.cluster_name}_filter"


StrDict_co = TypeVar(
    "StrDict_co", str, bytes, Dict[str, str | int | float | bool], covariant=True
)


class HasStr(Protocol[StrDict_co]):
    """Protocol for objects with a __str__ method."""

    def __str__(self) -> str: ...


Texts: TypeAlias = str | bytes | Dict[str, str | int | float | bool] | HasStr
"""A type alias for text data."""

# Define a TypeVar for the key type and value type
K = TypeVar("K")
V = TypeVar("V")


class HasGet(Protocol[K, V]):  # pyright: ignore[reportInvalidTypeVarUse]
    """Protocol for objects with a get method."""

    def get(  # pylint: disable=C0116 # noqa: D102
        self, key: K, default: Optional[V] = None
    ) -> Optional[V]: ...

    def __getitem__(self, key: K) -> V: ...


class HasIn(Protocol[K]):  # pyright: ignore[reportInvalidTypeVarUse]
    """Protocol for objects with a __contains__ method."""

    def __contains__(self, key: K) -> bool: ...


class HasGetAndIn(HasGet[K, V], HasIn[K]):  # pyright: ignore[reportInvalidTypeVarUse]
    """Protocol for objects with a get and __contains__ method."""


SeriesLike: TypeAlias = pd.Series | HasGetAndIn[str, Any] | dict[str, Any]
"""A type alias for a Pandas Series-like object."""


class DBScanConfig(BaseModel):
    """Configuration for the DBScan clustering algorithm."""

    eps: float = 0.5
    min_samples: int = 5
    metric: str | Callable = "euclidean"
    metric_params: dict | None = None
    algorithm: Literal["auto", "ball_tree", "kd_tree", "brute"] = "auto"
    leaf_size: int = 30
    p: float | None = None
    n_jobs: int | None = None


class RecursiveMergeSettings(BaseModel):
    """Settings for recursive merge clustering."""

    max_cluster_size: int = 5
    """The maximum number of questions to cluster together"""
    max_cluster_distance: float = 0.25
    """The maximum distance between questions to cluster together"""
    min_cluster_distance: float = 0.001
    """The minimum distance between questions to cluster together"""
    step_size: float = 0.001
    """The step size increase for recursive merge clustering"""


class RecursiveMergeConfig(RecursiveMergeSettings, arbitrary_types_allowed=True):
    """Configuration for recursive merging of clusters."""

    cluster_model_config: DBScanConfig = DBScanConfig()
    store: Bucket | None = None
    cluster_definitions: list[ClusterDefinition]
    embedder_type: EmbedderType
    embedding_model: Deployment
    token_limit: int | None = None


class TranslationConfig(BaseModel, arbitrary_types_allowed=True):
    """Configuration for translating clusters."""

    prompt: Prompt
    model: Deployment
    target_language: Language
    token_limit: int | None = None
    temperature: float = 0.0


class MergeConfig(BaseModel, arbitrary_types_allowed=True):
    """Configuration for merging clusters."""

    prompt_dir: StrPath
    merge_prompt: Prompt
    model: Deployment
    credentials: AzureCredentials | None = None
    tool_choice: ChatCompletionNamedToolChoiceParam | None = None
    tools: list[ChatCompletionToolParam] | None = None

    recursive_merge: RecursiveMergeConfig
    """Configuration for recursive merging of clusters."""

    translation_config: TranslationConfig


class ChunkingStrategy(BaseModel, extra="allow"):
    """Settings for chunking strategy.

    Allowing extra fields for flexibility.
    """

    name: str
    paragraph_delimiter: Optional[str] = None
    """A delimiter to use for splitting paragraphs, this is added to the content of documents
    in the TDM.
    """
    separators: Optional[list[str]] = None
    chunk_size: Optional[int] = None
    chunk_overlap: Optional[int] = None
    keep_separator: Optional[bool] = None
    add_start_index: Optional[bool] = None
    strip_whitespace: Optional[bool] = None
    text_splitter: Optional[Callable] = None

    @field_validator("text_splitter", mode="before")
    @classmethod
    def get_text_splitter(
        cls, value: Any, info: ValidationInfo  # pylint: disable=unused-argument
    ) -> Callable:
        """Get the text splitter for the chunking strategy.

        Args:
            value (Any): The value to convert.
            info (ValidationInfo): The validation information.

        Returns:
            object: The text splitter.

        """
        return get_text_splitter(value)
