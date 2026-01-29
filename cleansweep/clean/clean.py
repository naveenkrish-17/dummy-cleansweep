"""Sub-module for cleaning the documents."""

__all__ = ["clean_documents"]

import logging

import pandas as pd

from cleansweep.clean.rules import get_rule
from cleansweep.hooks.hookimpl import get_plugin_manager
from cleansweep.settings.clean import RuleSettings

logger = logging.getLogger(__name__)
"""Logger for the clean module."""


def clean_documents(
    documents: pd.DataFrame, rules: list[RuleSettings], plugins: list | None = None
) -> pd.DataFrame:
    """Clean the documents using the plugins and rules defined in settings.

    Args:
        documents (DataFrame): The documents to clean.
        rules (list[RuleSettings]): The rules to apply.
        plugins (list, optional): The plugins to use for cleaning. Defaults to None.

    """
    plugin_manager = get_plugin_manager()
    if plugins is None:
        plugins = []
    for plugin in plugins:
        plugin_manager.register(plugin)

    # execute the rules defined in settings
    for rule in rules:
        logger.info("Applying rule: %s", rule.rule)
        rule_class = get_rule(rule.type)
        original_documents = documents.copy()
        documents = rule_class.apply(
            documents, **rule.model_dump(exclude={"rule", "type"})
        )

        if documents.empty:
            logger.warning(
                "Rule %s returned an empty DataFrame. Skipping further rules for this document set.",
                rule.rule,
            )
            break

        if len(original_documents) != len(documents):
            changed_count = len(original_documents) - len(documents)
            logger.info("Rule removed %d documents.", changed_count)
        else:

            original_documents.reset_index(drop=True, inplace=True)
            documents.reset_index(drop=True, inplace=True)

            changed_count = len(original_documents.compare(documents))
            logger.info("Rule affected %d documents.", changed_count)

    # execute the hooks
    results = plugin_manager.hook.documents_clean(documents=documents)
    if results:
        documents = results[0]

    return documents
