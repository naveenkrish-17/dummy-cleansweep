"""Test core functionality"""

from pathlib import Path

import pytest

from cleansweep.core import get_plugin_module


def mock_read(*args, **kwargs):  # noqa pylint: disable=unused-argument
    """Mock read function"""
    return "Hello World!"


def mock_read_lines(*args, **kwargs):  # noqa pylint: disable=unused-argument
    """Mock read_lines function"""
    yield "Hello World!"


class TestGetPluginModule:
    """Test suite get_plugin_module function"""

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """Teardown test file"""

        def finalizer():
            test_file = Path(__file__).parent.parent.parent.joinpath(
                "cleansweep", "plugins", "test.py"
            )
            if test_file.exists():
                test_file.unlink()

        request.addfinalizer(finalizer)

    def test_get_plugin_module_local(self, tmp_path):
        """Test get_plugin_module method"""
        path = tmp_path / "test.py"
        path.write_text("def test():\n    return 'Hello World!'")

        module = get_plugin_module("file://" + str(path))

        assert module.test() == "Hello World!"

        # test plugin file already exists
        path = tmp_path / "test.py"
        path.write_text("def test():\n    return 'Hello World!'")

        module = get_plugin_module("file://" + str(path))
        assert module.test() == "Hello World!"

    def test_get_plugin_module_remote(self, mocker, tmp_path):
        """Test get_plugin_module method"""
        path = tmp_path / "test.py"
        path.write_text("def test():\n    return 'Hello World!'")

        mocker.patch("cleansweep.core.gcs_to_temp", return_value=path)

        module = get_plugin_module("gs:/" + str(path))

        assert module.test() == "Hello World!"

    def test_get_plugin_module_invalid(self):
        """Test get_plugin_module method"""
        with pytest.raises(ValueError):
            get_plugin_module("ftp://test.py")
