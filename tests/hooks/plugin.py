"""Test plugin for the `documents_clean` hook."""

import pandas as pd

from cleansweep import hookimpl


@hookimpl
def documents_clean(documents: pd.DataFrame) -> pd.DataFrame:
    """Hook to perform additional cleaning on the documents after they have had standard cleaning
    procedures applied.
    """
    documents["column1"] = documents["column1"] * 2
    return documents
