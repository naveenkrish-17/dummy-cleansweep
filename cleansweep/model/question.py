"""Question model."""

import hashlib
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, PrivateAttr, computed_field


class QuestionAnswerBase(BaseModel):
    """Base model for a Question and Answer pair."""

    question: str = Field(
        description="One of the customer queries that the article can help with."
    )
    answer: str = Field(
        description=(
            "Comprehensive response to the question in the voice of a helpful Sky assistant, "
            "all richly styled using Markdown. Provide self-contained and accurate information, "
            "useful links, images and videos, as well as considerate advice, without referencing "
            "the surrounding context of the source article. Highlight key terms and facts in bold "
            "and break down more involved information into bullet lists, tables and headings."
        )
    )

    _uuid: str = PrivateAttr()

    @computed_field
    @property
    def question_id(self) -> str:
        """Return unique id for the question."""
        return hashlib.md5(
            f"question: {self.question}|answer: {self.answer}".encode("utf-8")
        ).hexdigest()

    @computed_field
    @property
    def question_uuid(self) -> str:
        """Return the UUID of the question."""
        return self._uuid

    def pretty_repr(self) -> str:
        """Return a pretty representation of the question and answer."""
        ul = len(self.question) * "-"
        return f"\033[1m{self.question}\033[0m\n{ul}\n\n{self.answer}"

    def pretty_print(self) -> None:
        """Print a pretty representation of the question and answer."""
        return print(self.pretty_repr())

    def model_post_init(self, _context: Any):
        """Post init hook to set the UUID."""
        self._uuid = uuid4().hex


class QuestionAnswer(QuestionAnswerBase):
    """Question and Answer pair."""

    source_id: Optional[str]
