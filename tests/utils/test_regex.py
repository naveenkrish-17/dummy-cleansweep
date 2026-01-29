import pytest

from cleansweep.utils.regex import is_regex


class TestIsRegex:
    @pytest.mark.parametrize(
        "pattern, expected",
        [
            (r"\d+", True),  # valid regex for digits
            (r"[a-zA-Z]+", True),  # valid regex for letters
            (
                r"^a.*z$",
                True,
            ),  # valid regex for a string starting with 'a' and ending with 'z'
            (r"(", False),  # invalid regex with unbalanced parenthesis
            (r"[a-z", False),  # invalid regex with unbalanced square bracket
            (r"\\", True),  # valid regex for a single backslash
            ("", True),  # empty string is a valid regex
        ],
    )
    def test_is_regex(self, pattern, expected):
        assert is_regex(pattern) == expected

    def test_is_regex_non_string(self):
        with pytest.raises(TypeError):
            is_regex(123)  # non-string input should raise TypeError
