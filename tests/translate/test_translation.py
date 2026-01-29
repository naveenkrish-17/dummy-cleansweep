import pytest

from cleansweep.iso.languages import Language
from cleansweep.settings.load import load_settings, settings
from cleansweep.settings.translation import TranslationSettings
from cleansweep.translate.translation import translate

app = load_settings(TranslationSettings)


class TestTranslate:

    @pytest.mark.asyncio
    async def test_basic_translation(self, mocker):
        """Test the basic translation functionality."""
        text = "Hello, how are you?"
        expected = "Bonjour, comment ça va?"
        mocker.patch(
            "cleansweep.translate.translation.process_api_calls",
            return_value=['{"items": [{"text": "Bonjour, comment ça va?"}]}'],
        )
        translated_text = await translate(
            [text],
            Language.French,
            Language.English,
            settings.prompts_template_dir,
            app.prompt,
            app.model,
        )
        assert translated_text[0].get("text") == expected
