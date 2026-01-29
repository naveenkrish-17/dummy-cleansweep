"""Settings for chunking."""

from enum import Enum
from typing import Optional

from pydantic import BaseModel, ValidationInfo, field_validator

from cleansweep._types import (
    ChunkingStrategy,
    ClusterDefinition,
    DBScanConfig,
    Prompt,
    RecursiveMergeSettings,
)
from cleansweep.chunk.strategies import STRATEGIES
from cleansweep.deployments.deployments import DEPLOYMENTS
from cleansweep.enumerations import EmbedderType
from cleansweep.prompts import PROMPTS
from cleansweep.settings._types import SourceObject
from cleansweep.settings.app import AppSettings, Model


class SemanticMergeSource(BaseModel):
    """Settings for the source of the semantic merge."""

    source_dir: Optional[str] = "cleaned"
    source_extension: Optional[str] = "avro"
    source_prefix: Optional[str] = None


class SemanticChunkingSettings(BaseModel, arbitrary_types_allowed=True):
    """Settings for semantic chunking."""

    model: Model = DEPLOYMENTS.get_by_model(
        "gpt-4o"
    )  # pyright: ignore[reportAssignmentType]

    embedding_model: Model = DEPLOYMENTS.get_by_model(
        "text-embedding-ada-002"
    )  # pyright: ignore[reportAssignmentType]
    embedder_type: EmbedderType = EmbedderType.OPENAI
    """The provider of the embedding model"""
    token_limit: int = 4000
    """Used to limit the number of tokens in the text to embed when calling the OpenAI API"""

    qa_prompt: Prompt = PROMPTS["semantic_chunk"]
    merge_prompt: Prompt = PROMPTS["merge_questions"]
    validation_prompt: Prompt = PROMPTS["qna_validation"]

    recursive_merge: RecursiveMergeSettings = RecursiveMergeSettings()

    cluster_config: DBScanConfig = DBScanConfig(eps=0.25, min_samples=2)

    cluster_definitions: list[ClusterDefinition] = [
        ClusterDefinition(columns_to_embed=["question"])
    ]

    merge_source: SemanticMergeSource = SemanticMergeSource()

    @field_validator("qa_prompt", "merge_prompt", "validation_prompt")
    @classmethod
    def get_prompt(
        cls, value: Prompt, info: ValidationInfo  # pylint: disable=unused-argument
    ) -> Prompt:
        """Get the prompt from the value provided in the config.

        If the prompt provided is a custom prompt return it, if it is a default prompt then get it
        from the PROMPTS dictionary.

        Args:
            value (Prompt): The prompt value.
            info (ValidationInfo): The validation information.

        Returns:
            Prompt: The prompt.

        """
        if value.name in PROMPTS:
            return PROMPTS[value.name]

        return value


class ChunkSettings(AppSettings, arbitrary_types_allowed=True):
    """Settings for chunking."""

    strategy: Optional[ChunkingStrategy] = None
    force: bool = False
    """Force chunking of all documents - useful for re-chunking all documents"""

    # semantic chunking
    semantic: SemanticChunkingSettings = SemanticChunkingSettings()

    source: SourceObject = SourceObject(directory="cleaned", extension="avro")

    @property
    def strategy_settings(self) -> tuple[str, dict[str, ChunkingStrategy]]:
        """Get the strategy name and repository for the chunking strategy.

        Uses the chunk and embed settings.

        Returns
            tuple[str, dict]: The strategy name and repository.

        """
        strategy = "default"
        strategy_repository = None
        if self.strategy is not None:
            strategy = self.strategy.name
            strategy_repository = STRATEGIES
            if strategy not in strategy_repository:
                strategy_repository = {strategy: self.strategy}

        if strategy is None:
            strategy = "default"

        if strategy_repository is None:
            strategy_repository = STRATEGIES

        return strategy, strategy_repository

    def load(self, **kwargs) -> None:
        """Load the chunk settings.

        Args:
            **kwargs: Keyword arguments containing the chunk settings.

        """
        super().load(**kwargs)

        chunk_settings = kwargs.get("steps", kwargs).get("chunk", {})

        for key, value in chunk_settings.items():

            if not hasattr(self, key):
                continue

            if isinstance(getattr(self, key), BaseModel):
                setattr(self, key, type(getattr(self, key)).model_validate(value))
            elif isinstance(getattr(self, key), Enum):
                setattr(self, key, type(getattr(self, key))(value))
            else:
                setattr(self, key, value)

        # validate the source bucket
        self.validate_source_bucket()
