"""ISO-3166 countries."""

# pylint: disable=too-many-lines

import re
from enum import Enum

import pycountry


class Country(Enum):
    """ISO-3166 countries."""

    _ignore_ = "member CLS"  # pylint: disable=invalid-name
    CLS = vars()

    for member in pycountry.countries:
        CLS[re.sub(r"[^\w\d]", "", re.sub(r"\(.*?\)", "", member.name))] = member.alpha_2  # type: ignore

    def __str__(self) -> str:
        country = pycountry.countries.get(alpha_2=self.value)  # type: ignore
        return re.sub(r" +$", "", re.sub(r"\(.*?\)", "", country.name))  # type: ignore
