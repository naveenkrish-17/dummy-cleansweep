"""Test suite for the enumerations module"""

import pytest

from cleansweep.chunk.utils import (
    NLTKTextSplitter,
    RecursiveCharacterTextSplitter,
    SpacyTextSplitter,
    get_text_splitter,
)


class TestGetTextSplitter:
    """Test suite for the get_text_splitter function"""

    def test_get_text_splitter(self):
        """Test for valid text splitter"""
        assert get_text_splitter("recursive") == RecursiveCharacterTextSplitter
        assert get_text_splitter("nltk") == NLTKTextSplitter
        assert get_text_splitter("spacy") == SpacyTextSplitter

    def test_get_text_splitter_invalid(self):
        """Test for invalid text splitter"""
        with pytest.raises(ValueError):
            get_text_splitter("invalid")
