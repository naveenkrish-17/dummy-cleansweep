import pandas as pd

from cleansweep.clean import clean_documents
from cleansweep.settings.clean import CleanSettings, RuleSettings
from cleansweep.settings.load import load_settings

app = load_settings(CleanSettings)


class TestCleanDocuments:
    """Test suite for the clean_documents function."""

    def test_clean_documents(self):
        """Test the clean_documents function."""
        # Prepare
        app.rules = []
        app.rules.append(
            RuleSettings(
                rule="replace strings in body",
                type="replace_substrings",
                columns=["content"],
                substrings="world",
                replacement="universe",
            )
        )
        app.rules.append(
            RuleSettings(
                rule="filter empty body",
                type="remove_null_or_empty",
                columns=["content"],
            )
        )
        documents = {"title": ["hello", "hello"], "content": ["world", ""]}
        df = pd.DataFrame(documents)
        # Execute
        cleaned = clean_documents(df, app.rules)
        assert cleaned.equals(
            pd.DataFrame({"title": ["hello"], "content": ["universe"]})
        )
