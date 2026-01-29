"""CleanSweep package.

The CleanSweep package contains provides a set of tools for cleaning and preparing data for
embedding data into a vector space.
"""

__all__ = ["hookimpl"]
__app_name__ = "cleansweep"
__version__ = "1.5.1"
__url__ = "https://github.com/sky-uk/GenAI-CleanSweep"

# from cleansweep import cleansweep as extensions  # type: ignore
from cleansweep.hooks.hookimpl import hookimpl
