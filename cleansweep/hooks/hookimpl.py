"""Hook implementation for the Cleansweep application."""

__all__ = ["hookimpl", "get_plugin_manager"]

import pluggy

from cleansweep import __app_name__
from cleansweep.hooks import hookspecs

hookimpl = pluggy.HookimplMarker(__app_name__)


def get_plugin_manager():
    """Get the plugin manager for the Cleansweep application."""
    pm = pluggy.PluginManager(__app_name__)
    pm.add_hookspecs(hookspecs)
    pm.load_setuptools_entrypoints(__app_name__)
    return pm
