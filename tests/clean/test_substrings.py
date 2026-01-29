"""Test the substrings functions in the clean module."""

from cleansweep.clean.substrings import remove_substrings, replace_substrings


class TestReplaceSubstrings:
    """Test suite for `replace_substrings`"""

    def test_replace_substrings(self):
        """Test function returns expected value when provided a plain string to replace"""
        assert (
            replace_substrings("I'm not sure", "not", "definitely")
            == "I'm definitely sure"
        )
        assert (
            replace_substrings(
                "I'm not sure, I'm maybe going to make it",
                ["not", "maybe"],
                "definitely",
            )
            == "I'm definitely sure, I'm definitely going to make it"
        )

    def test_replace_substrings_regex(self):
        """Test function returns expected value when provided a regex pattern to replace"""
        assert replace_substrings("I'm not  sure", " {2,}", " ") == "I'm not sure"
        assert (
            replace_substrings("I'm not  sure000", ["(?<= ) {1,}", "\\d"], "")
            == "I'm not sure"
        )


class TestRemoveSubstrings:
    """Test suite for `remove_substrings`"""

    def test_remove_substrings(self):
        """Test function returns expected value when provided a plain string to replace"""
        assert remove_substrings("I'm not sure", "not") == "I'm  sure"
        assert (
            replace_substrings(
                "I'm not sure, I'm maybe going to make it", ["not", "maybe"]
            )
            == "I'm  sure, I'm  going to make it"
        )

    def test_remove_substrings_regex(self):
        """Test function returns expected value when provided a regex pattern to replace"""
        assert replace_substrings("I'm not  sure", " {2,}") == "I'm notsure"
        assert (
            replace_substrings("I'm not  sure000", ["(?<= ) {1,}", "\\d"])
            == "I'm not sure"
        )
