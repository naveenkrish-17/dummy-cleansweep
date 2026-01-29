"""Test cases for the model.transform module."""

# pylint: disable=too-many-lines
# pyright: ignore
# type: ignore

from pathlib import Path

import pytest

from cleansweep.model.transform import transform_to_model

# pylint: disable=protected-access, line-too-long

DATA = {
    "attributes": {"title": "Example Title"},
    "items": ["item1", "item2"],
    "objects": [{"key": "value"}, {"key": "value2"}],
    "nested": [
        {"key": [{"key1": "value", "key2": "value12"}]},
        {"key": [{"key1": "value2"}]},
        {"key": [{"key1": "value3", "key2": "value32"}]},
    ],
}


SCENARIOS = {
    "simple_mapping": [
        {
            "data": {
                "id": "blt1f5620909de6cd18",
                "attributes": {
                    "metadata": {
                        "description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                    },
                    "content_blocks": [
                        {
                            "title": "Main content",
                            "content": [
                                {
                                    "title": "Some Title",
                                    "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                                },
                                {
                                    "title": "Some other title",
                                    "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                                },
                            ],
                        },
                        {
                            "title": "Table content",
                            "table": {
                                "title": "Table title",
                                "rows": [["Row 1", "Row 2"]],
                                "columns": ["Column 1", "Column 2"],
                                "access_groups": [],
                            },
                        },
                    ],
                    "title": "A document title",
                    "updated_at": "2024-01-10T16:58:05.639Z",
                    "user_groups": [],
                },
            },
            "mapping": {
                "id": "$.id",
                "name": "$.attributes.title",
                "content": [
                    {
                        "title": "$.attributes.content_blocks[*].content[*].title",
                        "data": "$.attributes.content_blocks[*].content[*].content",
                    },
                    {
                        "title": "$.attributes.content_blocks[*].table.title",
                        "rows": "$.attributes.content_blocks[*].table.rows",
                        "columns": "$.attributes.content_blocks[*].table.columns",
                        "metadata": {
                            "access_groups": "$.attributes.content_blocks[*].table.access_groups",
                        },
                    },
                ],
                "metadata": {
                    "modified": "$.attributes.updated_at",
                    "description": "$.attributes.metadata.description",
                    "access_groups": "$.attributes.user_groups",
                },
            },
            "expected": {
                "id": "blt1f5620909de6cd18",
                "name": "A document title",
                "content": [
                    {
                        "title": "Some Title",
                        "data": [
                            (
                                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                            )
                        ],
                    },
                    {
                        "title": "Some other title",
                        "data": [
                            (
                                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                            )
                        ],
                    },
                    {
                        "title": "Table title",
                        "rows": [["Row 1", "Row 2"]],
                        "columns": ["Column 1", "Column 2"],
                        "metadata": {"access_groups": []},
                    },
                ],
                "metadata": {
                    "modified": "2024-01-10T16:58:05.639Z",
                    "description": (
                        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                        "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                    ),
                    "access_groups": [],
                },
            },
        }
    ],
    "simple_mapping_default_value": [
        {
            "data": {
                "id": "blt1f5620909de6cd18",
                "attributes": {
                    "metadata": {
                        "description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                    },
                    "content_blocks": [
                        {
                            "title": "Main content",
                            "content": [
                                {
                                    "title": "Some Title",
                                    "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                                },
                                {
                                    "title": "Some other title",
                                    "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                                },
                            ],
                        },
                    ],
                    "title": "A document title",
                    "updated_at": "2024-01-10T16:58:05.639Z",
                    "user_groups": [],
                },
            },
            "mapping": {
                "id": "$.id",
                "name": "$.attributes.title",
                "empty": "$.attributes.empty",
                "content": [
                    {
                        "title": "$.attributes.content_blocks[*].content[*].title",
                        "data": "$.attributes.content_blocks[*].content[*].content",
                    }
                ],
                "metadata": {
                    "modified": "$.attributes.updated_at",
                    "description": "$.attributes.metadata.description",
                    "access_groups": "$.attributes.user_groups",
                },
            },
            "expected": {
                "id": "blt1f5620909de6cd18",
                "name": "A document title",
                "content": [
                    {
                        "title": "Some Title",
                        "data": [
                            (
                                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                            )
                        ],
                    },
                    {
                        "title": "Some other title",
                        "data": [
                            (
                                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                            )
                        ],
                    },
                ],
                "metadata": {
                    "modified": "2024-01-10T16:58:05.639Z",
                    "description": (
                        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                        "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                    ),
                    "access_groups": [],
                },
            },
        }
    ],
    "mapping_source_array_with_different_schemas": [
        {
            "data": {
                "id": "blt1f5620909de6cd18",
                "attributes": {
                    "metadata": {
                        "description": (
                            "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                            "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                        ),
                    },
                    "content_blocks": [
                        {
                            "title": "Main content",
                            "content": [
                                {
                                    "title": "Some Title",
                                    "content": (
                                        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                        "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                                    ),
                                },
                                {
                                    "title": "Some other title",
                                    "content": (
                                        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                        "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                                    ),
                                },
                            ],
                        },
                        {
                            "title": "Other content",
                            "user_groups": ["blt59ef83e87100048f"],
                            "content": (
                                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                            ),
                        },
                    ],
                    "title": "A document title",
                    "updated_at": "2024-01-10T16:58:05.639Z",
                    "user_groups": [],
                },
            },
            "mapping": {
                "id": "$.id",
                "name": "$.attributes.title",
                "content": [
                    {
                        "title": "$.attributes.content_blocks[*].content[*].title",
                        "data": "$.attributes.content_blocks[*].content[*].content",
                    },
                    {
                        "title": "$.attributes.content_blocks[*].title",
                        "data": "$.attributes.content_blocks[*].content",
                        "metadata": {
                            "access_groups": "$.attributes.content_blocks[*].user_groups",
                        },
                    },
                ],
                "metadata": {
                    "modified": "$.attributes.updated_at",
                    "description": "$.attributes.metadata.description",
                    "access_groups": "$.attributes.user_groups",
                },
            },
            "expected": {
                "id": "blt1f5620909de6cd18",
                "name": "A document title",
                "content": [
                    {
                        "title": "Some Title",
                        "data": [
                            (
                                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                            )
                        ],
                    },
                    {
                        "title": "Some other title",
                        "data": [
                            (
                                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                            )
                        ],
                    },
                    {
                        "title": "Other content",
                        "data": [
                            (
                                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                            )
                        ],
                        "metadata": {"access_groups": ["blt59ef83e87100048f"]},
                    },
                ],
                "metadata": {
                    "modified": "2024-01-10T16:58:05.639Z",
                    "description": (
                        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                        "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                    ),
                    "access_groups": [],
                },
            },
        }
    ],
    "mapping_keep_source_array_order": [
        {
            "data": {
                "id": "blt1f5620909de6cd18",
                "attributes": {
                    "metadata": {
                        "description": (
                            "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                            "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                        ),
                    },
                    "content_blocks": [
                        {
                            "title": "Main content",
                            "content": [
                                {
                                    "title": "Some Title",
                                    "content": (
                                        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                        "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                                    ),
                                },
                                {
                                    "title": "Some other title",
                                    "content": (
                                        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                        "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                                    ),
                                },
                            ],
                        },
                        {
                            "title": "Other content",
                            "user_groups": ["blt59ef83e87100048f"],
                            "content": (
                                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                            ),
                        },
                        {
                            "title": "More content",
                            "content": [
                                {
                                    "title": "Final title",
                                    "content": (
                                        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                        "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                                    ),
                                },
                            ],
                        },
                    ],
                    "title": "A document title",
                    "updated_at": "2024-01-10T16:58:05.639Z",
                    "user_groups": [],
                },
            },
            "mapping": {
                "id": "$.id",
                "name": "$.attributes.title",
                "content": [
                    {
                        "title": "$.attributes.content_blocks[*].content[*].title",
                        "data": "$.attributes.content_blocks[*].content[*].content",
                    },
                    {
                        "title": "$.attributes.content_blocks[*].title",
                        "data": "$.attributes.content_blocks[*].content",
                        "metadata": {
                            "access_groups": "$.attributes.content_blocks[*].user_groups",
                        },
                    },
                ],
                "metadata": {
                    "modified": "$.attributes.updated_at",
                    "description": "$.attributes.metadata.description",
                    "access_groups": "$.attributes.user_groups",
                },
            },
            "expected": {
                "id": "blt1f5620909de6cd18",
                "name": "A document title",
                "content": [
                    {
                        "title": "Some Title",
                        "data": [
                            (
                                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                            )
                        ],
                    },
                    {
                        "title": "Some other title",
                        "data": [
                            (
                                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                            )
                        ],
                    },
                    {
                        "title": "Final title",
                        "data": [
                            (
                                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                            )
                        ],
                    },
                    {
                        "title": "Other content",
                        "data": [
                            (
                                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                                "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                            )
                        ],
                        "metadata": {"access_groups": ["blt59ef83e87100048f"]},
                    },
                ],
                "metadata": {
                    "modified": "2024-01-10T16:58:05.639Z",
                    "description": (
                        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
                        "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
                    ),
                    "access_groups": [],
                },
            },
        }
    ],
    "content_stack_mapping": [
        {
            "data": {
                "type": "article",
                "id": "blt1f5620909de6cd18",
                "attributes": {
                    "_in_progress": False,
                    "article_preview": "",
                    "banner": {"banner_image": None},
                    "contact_tree": [],
                    "display_title": "Sky Mobile tariff guide",
                    "key_info_content": [],
                    "linked_articles": {"linked_article_segment": []},
                    "metadata": {
                        "meta_title": "Sky Mobile tariff guide",
                        "description": "This is the value replacing the description",
                        "regions": ["UK"],
                        "link_to_uk_roi_article": [],
                        "product_types": [],
                        "linked_article": [],
                        "flow_start_article": [],
                        "keywords": "sky mobile, tariff, terms and and conditions",
                        "indexed": False,
                        "prevent_link_following": False,
                        "promote_search": False,
                        "nosnippet": False,
                        "redirect": {
                            "should_redirect": False,
                            "redirect_url": "",
                            "permanent": False,
                        },
                    },
                    "modular_blocks": [
                        {
                            "rich_text": {
                                "title": "Main content",
                                "user_groups": [],
                                "user_group_icon_override": [],
                                "mark_as_key_info_box": False,
                                "content_blocks": [
                                    {
                                        "title": "Plan, Add Ons, Usage Charges and Specialised Numbers",
                                        "content_left": "This is the first piece of content.",
                                    },
                                    {
                                        "title": "Changes to Sky Mobile Tariff Guide",
                                        "content_left": "This is the second piece of content.",
                                    },
                                ],
                            }
                        }
                    ],
                    "parent_flow": [],
                    "sales_offers": {"show_sales_offers": False},
                    "slug": "sky-mobile-tariff-guide",
                    "tags": ["sky mobile", "terms and conditions", "tariffs"],
                    "title": "Sky Mobile Tariff Guide [terms and conditions]",
                    "topics": [],
                    "updated_at": "2024-01-10T16:58:05.639Z",
                    "user_group_icon_override": [],
                    "user_groups": [],
                    "videos": [],
                },
            },
            "mapping": {
                "id": "$.id",
                "name": "$.attributes.metadata.meta_title",
                "content": [
                    {
                        "title": "$.attributes.modular_blocks[*].rich_text.content_blocks[*].title",
                        "data": "$.attributes.modular_blocks[*].rich_text.content_blocks[*].content_left",
                        "metadata": {
                            "access_groups": "$.attributes.modular_blocks[*].rich_text.user_groups",
                        },
                    },
                    {
                        "title": "$.attributes.modular_blocks[*].accordion.content_blocks[*].title",
                        "data": "$.attributes.modular_blocks[*].accordion.content_blocks[*].content_left",
                        "metadata": {
                            "access_groups": "$.attributes.modular_blocks[*].accordion.content_blocks[*].user_groups",
                        },
                    },
                ],
                "metadata": {
                    "modified": "$.attributes.updated_at",
                    "description": "$.attributes.metadata.description",
                    "access_groups": "$.attributes.user_groups",
                    "tags": "$.attributes.tags",
                    "region": "$.attributes.metadata.regions",
                    "slug": "$.attributes.slug",
                    "should_redirect": "$.attributes.metadata.redirect.should_redirect",
                    "redirect_url": "$.attributes.metadata.redirect.redirect_url",
                    "permanent": "$.attributes.metadata.redirect.permanent",
                    "keywords": "$.attributes.metadata.keywords",
                },
            },
            "expected": {
                "name": "Sky Mobile tariff guide",
                "id": "blt1f5620909de6cd18",
                "content": [
                    {
                        "title": "Plan, Add Ons, Usage Charges and Specialised Numbers",
                        "data": ["This is the first piece of content."],
                        "metadata": {"access_groups": []},
                    },
                    {
                        "title": "Changes to Sky Mobile Tariff Guide",
                        "data": ["This is the second piece of content."],
                        "metadata": {"access_groups": []},
                    },
                ],
                "metadata": {
                    "access_groups": [],
                    "modified": "2024-01-10T16:58:05.639Z",
                    "tags": ["sky mobile", "terms and conditions", "tariffs"],
                    "description": "This is the value replacing the description",
                    "region": ["UK"],
                    "slug": "sky-mobile-tariff-guide",
                    "should_redirect": False,
                    "redirect_url": "",
                    "permanent": False,
                    "keywords": "sky mobile, tariff, terms and and conditions",
                },
            },
        },
        {
            "data": {
                "type": "article",
                "id": "blt957e913e4a72fc83",
                "attributes": {
                    "_in_progress": False,
                    "article_preview": "",
                    "banner": {"banner_image": None},
                    "contact_tree": [],
                    "display_title": "Sky Mobile past tariff guides",
                    "key_info_content": [],
                    "linked_articles": {"linked_article_segment": []},
                    "metadata": {
                        "meta_title": "Sky Mobile past tariff guides",
                        "description": "This page contains past tariff guides from Sky Mobile.",
                        "regions": ["UK"],
                        "link_to_uk_roi_article": [],
                        "product_types": [],
                        "linked_article": [],
                        "flow_start_article": [],
                        "keywords": "",
                        "indexed": False,
                        "prevent_link_following": False,
                        "promote_search": False,
                        "nosnippet": False,
                        "redirect": {
                            "should_redirect": False,
                            "redirect_url": "",
                            "permanent": False,
                        },
                    },
                    "modular_blocks": [
                        {
                            "accordion": {
                                "title": "Sky Mobile Past Tariff Guides",
                                "user_groups": [],
                                "user_group_icon_override": [],
                                "content_blocks": [
                                    {
                                        "user_groups": [],
                                        "user_group_icon_override": [],
                                        "title": "Tariff Guide from 18 December 2023",
                                        "content_left": "The first piece of content.",
                                    },
                                    {
                                        "user_groups": [],
                                        "user_group_icon_override": [],
                                        "title": "Tariff Guide from 13 December 2023",
                                        "content_left": "The second piece of content.",
                                    },
                                ],
                            }
                        }
                    ],
                    "parent_flow": [],
                    "sales_offers": {"show_sales_offers": False},
                    "slug": "sky-mobile-past-tariff-guides",
                    "tags": ["sky mobile tariffs"],
                    "title": "Sky Mobile Past Tariff Guides [terms and conditions]",
                    "topics": [],
                    "updated_at": "2024-01-10T16:57:48.356Z",
                    "user_group_icon_override": [],
                    "user_groups": [],
                    "videos": [],
                },
            },
            "mapping": {
                "id": "$.id",
                "name": "$.attributes.metadata.meta_title",
                "content": [
                    {
                        "title": "$.attributes.modular_blocks[*].rich_text.content_blocks[*].title",
                        "data": "$.attributes.modular_blocks[*].rich_text.content_blocks[*].content_left",
                        "metadata": {
                            "access_groups": "$.attributes.modular_blocks[*].rich_text.user_groups",
                        },
                    },
                    {
                        "title": "$.attributes.modular_blocks[*].accordion.content_blocks[*].title",
                        "data": "$.attributes.modular_blocks[*].accordion.content_blocks[*].content_left",
                        "metadata": {
                            "access_groups": "$.attributes.modular_blocks[*].accordion.content_blocks[*].user_groups",
                        },
                    },
                    {
                        "title": "$.attributes.modular_blocks[*].instructions.content_blocks[*].title",
                        "data": "$.attributes.modular_blocks[*].instructions.content_blocks[*].content_left",
                        "metadata": {
                            "access_groups": "$.attributes.modular_blocks[*].instructions.content_blocks[*].user_groups",
                        },
                    },
                    {
                        "title": "$.attributes.modular_blocks[*].table.title",
                        "columns": "$.attributes.modular_blocks[*].table.table.columns",
                        "rows": "$.attributes.modular_blocks[*].table.table.rows",
                        "metadata": {
                            "access_groups": "$.attributes.modular_blocks[*].table.user_groups",
                        },
                    },
                    {
                        "title": "$.attributes.modular_blocks[*].tabbed.content_blocks[*].title",
                        "data": "$.attributes.modular_blocks[*].tabbed.content_blocks[*].content_left",
                        "metadata": {
                            "access_groups": "$.attributes.modular_blocks[*].tabbed.content_blocks[*].user_groups",
                        },
                    },
                ],
                "metadata": {
                    "modified": "$.attributes.updated_at",
                    "description": "$.attributes.metadata.description",
                    "access_groups": "$.attributes.user_groups",
                    "tags": "$.attributes.tags",
                    "region": "$.attributes.metadata.regions",
                    "slug": "$.attributes.slug",
                    "should_redirect": "$.attributes.metadata.redirect.should_redirect",
                    "redirect_url": "$.attributes.metadata.redirect.redirect_url",
                    "permanent": "$.attributes.metadata.redirect.permanent",
                    "keywords": "$.attributes.metadata.keywords",
                },
            },
            "expected": {
                "name": "Sky Mobile past tariff guides",
                "id": "blt957e913e4a72fc83",
                "content": [
                    {
                        "title": "Tariff Guide from 18 December 2023",
                        "data": ["The first piece of content."],
                        "metadata": {"access_groups": []},
                    },
                    {
                        "title": "Tariff Guide from 13 December 2023",
                        "data": ["The second piece of content."],
                        "metadata": {"access_groups": []},
                    },
                ],
                "metadata": {
                    "access_groups": [],
                    "modified": "2024-01-10T16:57:48.356Z",
                    "tags": ["sky mobile tariffs"],
                    "description": "This page contains past tariff guides from Sky Mobile.",
                    "region": ["UK"],
                    "slug": "sky-mobile-past-tariff-guides",
                    "should_redirect": False,
                    "redirect_url": "",
                    "permanent": False,
                    "keywords": "",
                },
            },
        },
    ],
    "map_tokens": [
        {
            "data": {
                "id": "blt1f5620909de6cd18",
                "attributes": {
                    "metadata": {
                        "description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                    },
                    "content_blocks": [
                        {
                            "title": "Main content",
                            "content": [
                                {
                                    "title": "Some Title",
                                    "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                                },
                                {
                                    "title": "Some other title",
                                    "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                                },
                            ],
                        },
                        {
                            "title": "Table content",
                            "table": {
                                "title": "Table title",
                                "rows": [["Row 1", "Row 2"]],
                                "columns": ["Column 1", "Column 2"],
                                "access_groups": [],
                            },
                        },
                    ],
                    "title": "A document title",
                    "updated_at": "2024-01-10T16:58:05.639Z",
                    "user_groups": [],
                },
            },
            "mapping": {
                "id": "$.id",
                "content": [
                    {
                        "title": "$.attributes.content_blocks[*].content[*].title",
                        "data": "$.attributes.content_blocks[*].content[*].content",
                    },
                    {
                        "title": "$.attributes.content_blocks[*].table.title",
                        "rows": "$.attributes.content_blocks[*].table.rows",
                        "columns": "$.attributes.content_blocks[*].table.columns",
                        "metadata": {
                            "access_groups": "$.attributes.content_blocks[*].table.access_groups",
                        },
                    },
                ],
                "metadata": {
                    "modified": "$.attributes.updated_at",
                    "description": "$.attributes.metadata.description",
                    "access_groups": "$.attributes.user_groups",
                },
            },
        },
        {
            "data": {
                "id": "blt1f5620909de6cd18",
                "attributes": {
                    "metadata": {
                        "description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                    },
                    "content_blocks": [
                        {
                            "title": "Main content",
                            "content": [
                                {
                                    "title": "Some Title",
                                    "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                                },
                                {
                                    "title": "Some other title",
                                    "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                                },
                            ],
                        },
                        {
                            "title": "Table content",
                            "table": {
                                "title": "Table title",
                                "rows": [["Row 1", "Row 2"]],
                                "columns": ["Column 1", "Column 2"],
                                "access_groups": [],
                            },
                        },
                    ],
                    "updated_at": "2024-01-10T16:58:05.639Z",
                    "user_groups": [],
                },
            },
            "mapping": {
                "id": "$.id",
                "name": "$.attributes.title",
                "content": [
                    {
                        "title": "$.attributes.content_blocks[*].content[*].title",
                        "data": "$.attributes.content_blocks[*].content[*].content",
                    },
                    {
                        "title": "$.attributes.content_blocks[*].table.title",
                        "rows": "$.attributes.content_blocks[*].table.rows",
                        "columns": "$.attributes.content_blocks[*].table.columns",
                        "metadata": {
                            "access_groups": "$.attributes.content_blocks[*].table.access_groups",
                        },
                    },
                ],
                "metadata": {
                    "modified": "$.attributes.updated_at",
                    "description": "$.attributes.metadata.description",
                    "access_groups": "$.attributes.user_groups",
                },
            },
        },
        {
            "data": {
                "id": "blt1f5620909de6cd18",
                "attributes": {
                    "metadata": {
                        "description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                    },
                    "content_blocks": [
                        {
                            "title": "Main content",
                            "content": [
                                {
                                    "title": "Some Title",
                                    "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                                },
                                {
                                    "title": "Some other title",
                                    "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                                },
                            ],
                        },
                        {
                            "title": "Table content",
                            "table": {
                                "title": "Table title",
                                "rows": [["Row 1", "Row 2"]],
                                "columns": ["Column 1", "Column 2"],
                                "access_groups": [],
                            },
                        },
                    ],
                    "title": "A document title",
                    "updated_at": "2024-01-10T16:58:05.639Z",
                    "user_groups": [],
                },
            },
            "mapping": {
                "id": "$.id",
                "name": "$.attributes.title",
                "metadata": {
                    "modified": "$.attributes.updated_at",
                    "description": "$.attributes.metadata.description",
                    "access_groups": "$.attributes.user_groups",
                },
            },
        },
    ],
}


def get_scenario(key: str) -> dict:
    """Return the scenario for the given key."""
    return SCENARIOS[key]
