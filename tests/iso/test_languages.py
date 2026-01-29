"""Test suite for the `languages` model."""

import pytest

from cleansweep.iso.languages import Language  # replace with actual module name


class TestLanguage:
    """Test suite for the Language enum class."""

    def test_language_enum_values(self):
        """Test that the Language enum class has the correct values."""
        assert Language.English.value == "en"
        assert Language.Spanish.value == "es"
        assert Language.Italian.value == "it"
        assert Language.German.value == "de"

    # Add more assertions for other languages...

    def test_invalid_language_enum(self):
        """Test that an invalid language raises a ValueError."""
        with pytest.raises(ValueError):
            _ = Language("invalid")

    def test_language_friendly_name(self):
        """Test that the friendly_name property returns the correct value."""
        assert Language.English.__str__() == "English"
        assert Language.Spanish.__str__() == "Spanish"
        assert Language.Italian.__str__() == "Italian"
        assert Language.German.__str__() == "German"
        assert Language.ChurchSlavic.__str__() == "Church Slavic"
