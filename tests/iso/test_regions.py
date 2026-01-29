"""Test suite for the `regions`  module."""

import pytest

from cleansweep.iso.regions import Country


class TestCountry:
    """Test suite for the Country enum class."""

    def test_country_enum_values(self):
        """Test that the Country enum class has the correct values."""
        assert Country.UnitedKingdom.value == "GB"
        assert Country.Italy.value == "IT"
        assert Country.Germany.value == "DE"
        assert Country.UnitedStates.value == "US"
        assert Country.BurkinaFaso.value == "BF"

    def test_invalid_country_enum(self):
        """Test that an invalid country raises a ValueError."""
        with pytest.raises(ValueError):
            _ = Country("invalid")

    def test_country___str__(self):
        """Test that the __str__() property returns the correct value."""
        assert Country.UnitedKingdom.__str__() == "United Kingdom"
        assert Country.Italy.__str__() == "Italy"
        assert Country.Germany.__str__() == "Germany"
        assert Country.UnitedStates.__str__() == "United States"
        assert Country.BurkinaFaso.__str__() == "Burkina Faso"
