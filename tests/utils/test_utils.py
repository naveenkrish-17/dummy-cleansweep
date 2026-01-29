"""Test suite for the utils module."""

from cleansweep.utils.collections import dict_not_none


class TestDictNotNull:
    """Test suite for the dict_not_none function."""

    def test_dict_not_none(self):
        """Test that dict_not_none excludes None values."""
        result = dict_not_none(a=1, b=None, c=3, d=None)
        assert result == {"a": 1, "c": 3}

    def test_dict_not_none_all_none(self):
        """Test that dict_not_none returns an empty dictionary when all values are None."""
        result = dict_not_none(a=None, b=None, c=None, d=None)
        assert result == {}

    def test_dict_not_none_no_none(self):
        """Test that dict_not_none returns the same dictionary when no values are None."""
        result = dict_not_none(a=1, b=2, c=3, d=4)
        assert result == {"a": 1, "b": 2, "c": 3, "d": 4}

    def test_dict_not_none_empty(self):
        """Test that dict_not_none returns an empty dictionary when no arguments are given."""
        result = dict_not_none()
        assert result == {}
