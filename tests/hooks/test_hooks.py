""""""

import importlib
from pathlib import Path

import pandas as pd

from cleansweep.hooks.hookimpl import get_plugin_manager


def clean_documents(documents: pd.DataFrame, plugins: list) -> pd.DataFrame:
    """Test function for executing hooks."""
    plugin_manager = get_plugin_manager()
    for plugin in plugins:
        plugin_manager.register(plugin)

    results = plugin_manager.hook.documents_clean(documents=documents)

    if results:
        return results[0]
    return documents


class TestHooks:
    """Test suite for cleansweep hooks"""

    def test_document_clean(self):
        """Test the `documents_clean` hook."""
        # import plugin
        plugin_file = Path("tests/hooks/plugin.py")
        plugin_name = plugin_file.stem
        plugin_module = importlib.import_module(plugin_name)

        df = pd.DataFrame(
            {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]}
        )

        new_df = clean_documents(df, [plugin_module])
        assert new_df["column1"].tolist() == [2, 4, 6, 8, 10]
        assert new_df["column2"].tolist() == ["a", "b", "c", "d", "e"]
