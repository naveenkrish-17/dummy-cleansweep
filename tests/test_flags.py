"""Test suite for the flag decorator."""

import pytest

from cleansweep.flags import flag


class TestFlag:
    """Test suite for the flag decorator."""

    scenarios = [
        pytest.param(
            "test_flag", {"default": False}, False, id="flag false, return default"
        ),
        pytest.param("test_flag", {"arg_pos": 0}, 1, id="flag false, return arg_pos"),
        pytest.param("test_flag", {"arg_pos": 1}, 2, id="flag false, return arg_pos"),
        pytest.param(
            "test_flag", {"arg_name": "kwarg1"}, 3, id="flag false, return default"
        ),
        pytest.param(
            "translate",
            {"arg_name": "kwarg1"},
            True,
            id="flag True, return True",
        ),
    ]

    @pytest.mark.parametrize("flag_name, kwargs, expected", scenarios)
    def test_flag(self, flag_name, kwargs, expected):
        """Test the flag function."""

        @flag(flag_name, **kwargs)
        def test_function(arg1, arg2, kwarg1):  # pylint: disable=unused-argument
            return True

        assert test_function(1, 2, kwarg1=3) == expected
