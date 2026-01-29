"""Translation settings for the application."""

from enum import Enum

from pydantic import BaseModel

from cleansweep._types import Prompt
from cleansweep.iso.languages import Language
from cleansweep.prompts import PROMPTS
from cleansweep.settings._types import SourceObject
from cleansweep.settings.app import AppSettings


class TranslationSettings(AppSettings):
    """Settings for translation."""

    target_language: Language = (
        Language.English  # pylint: disable=no-member # pyright: ignore[reportAttributeAccessIssue]
    )
    """The target language for translation"""
    token_limit: int = 3000
    """Used to limit the number of tokens in the text to embed when calling the OpenAI API"""
    temperature: float = 0
    """The temperature for the translation"""

    prompt: Prompt = PROMPTS["translation"]

    fields_to_translate: list[str] = ["chunk"]
    """The fields to translate"""
    force: bool = False

    source: SourceObject = SourceObject(directory="chunked", extension="avro")

    def load(self, **kwargs) -> None:
        """Load the translation settings.

        Args:
            **kwargs: Keyword arguments containing the translation settings.

        """
        super().load(**kwargs)

        chunk_settings = kwargs.get("steps", kwargs).get("translate", {})

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
