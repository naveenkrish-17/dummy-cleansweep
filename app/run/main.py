"""Module for running a custom step."""

from cleansweep import __app_name__
from cleansweep.core import get_plugin_module
from cleansweep.exceptions import PipelineError
from cleansweep.hooks.hookimpl import get_plugin_manager
from cleansweep.settings.base import settings
from cleansweep.settings.files import InputFiles
from cleansweep.settings.load import load_settings
from cleansweep.settings.run import RunSettings
from cleansweep.utils.exceptions import error_handler, initialize_except_hook
from cleansweep.utils.logging import set_app_labels, setup_logging

logger = setup_logging(__app_name__, settings.log_level, settings.dev_mode)
"""Logger for the module."""

initialize_except_hook(uncaught_hook=error_handler)


def main():
    """Process drop."""
    input_files = InputFiles()

    if input_files.config_file_uri is not None:
        logger.info("Config file: %s", input_files.config_file_uri)

    logger.info("Loading settings...")
    app = load_settings(RunSettings, input_files.config_file_uri)
    if app.plugin is None:
        raise PipelineError("No plugin specified")
    set_app_labels(app.labels)

    logger.info("🔌 Applying plugins...")
    plugin_manager = get_plugin_manager()
    try:
        plugin_manager.register(get_plugin_module(app.plugin))
    except ValueError as exc:
        logger.error("Error registering plugin: %s", exc)
    else:
        plugin_manager.hook.run(*app.args, **app.kwargs)

    exit(0)
