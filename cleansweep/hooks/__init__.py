"""Module for defining hooks which allow plugins to be created for the app.

Hooks are functions that are called at specific points in the application's
execution. Plugins can define hooks to extend the functionality of the app.

Plugins can define hooks by creating a function with the same name as the hook
and decorating it with the `@hookimpl` decorator. The function should accept
the arguments that the hook provides and return the results of the hook.
"""
