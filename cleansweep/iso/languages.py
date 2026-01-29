"""ISO-639-1 language codes."""

import re
from enum import Enum

import pycountry


class Language(Enum):
    """ISO-639-1 language codes."""

    _ignore_ = "member CLS"  # pylint: disable=invalid-name
    CLS = vars()

    for member in pycountry.languages:
        if hasattr(member, "alpha_2"):
            CLS[re.sub(r"[^\w\d]", "", re.sub(r"\(.*?\)", "", member.name))] = (  # type: ignore
                member.alpha_2  # type: ignore
            )

    def __str__(self) -> str:
        lang = pycountry.languages.get(alpha_2=self.value)  # type: ignore
        if lang is None:
            lang = pycountry.languages.get(alpha_3=self.value)  # type: ignore
        return re.sub(r" +$", "", re.sub(r"\(.*?\)", "", lang.name))  # type: ignore
