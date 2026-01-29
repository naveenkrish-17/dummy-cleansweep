"""HTML text splitter based on headers and tags."""

import logging
import re
from typing import Callable, List

from bs4 import BeautifulSoup
from bs4.element import PageElement, Tag
from langchain_text_splitters.base import TextSplitter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class JillSplitter(TextSplitter):
    """HTML text splitter based on headers and tags."""

    headers = ["h6", "h5", "h4", "h3", "h2"]

    def __init__(
        self,
        chunk_size: int = 4000,
        chunk_overlap: int = 200,
        length_function: Callable[[str], int] = len,
    ) -> None:
        """Create a new TextSplitter.

        Args:
            chunk_size: Maximum size of chunks to return
            chunk_overlap: Overlap in characters between chunks
            length_function: Function that measures the length of given chunks

        """
        if chunk_overlap > chunk_size:
            raise ValueError(
                f"Got a larger chunk overlap ({chunk_overlap}) than chunk size "
                f"({chunk_size}), should be smaller."
            )
        self._chunk_size = chunk_size
        self._chunk_overlap = chunk_overlap
        self._length_function = length_function
        self._keep_separator = False
        self._add_start_index = False
        self._strip_whitespace = False

    def _section_document(
        self, tags: list[Tag | PageElement | BeautifulSoup], tag_name: str
    ) -> List[BeautifulSoup]:
        """Split the provided HTML tags into sections based on the specified tag name.

        Args:
            tags (list[Tag | PageElement | BeautifulSoup]): A list of HTML tags to be processed.
            tag_name (str): The name of the tag to split sections by.

        Returns:
            List[BeautifulSoup]: A list of BeautifulSoup objects, each representing a section of HTML content.

        """
        childs = []
        sections = []

        def inner_sections(soup, tag_name, childs=childs, sections=sections):
            for child in soup.children:
                if child.name == tag_name:
                    if childs:
                        sections.append(childs)
                        childs = []
                    childs.append(child)
                elif child.name is not None and re.match(
                    r"h\d|p|span|table", child.name
                ):
                    childs.append(child)
                elif hasattr(child, "children"):
                    childs, sections = inner_sections(child, tag_name)
                else:
                    childs.append(child)

            return childs, sections

        for child in tags:
            if child.name == tag_name:
                if childs:
                    sections.append(childs)
                    childs = []
                childs.append(child)
            elif child.name is not None and re.match(r"h\d|p|span|table", child.name):
                childs.append(child)
            elif hasattr(child, "children"):
                childs, sections = inner_sections(child, tag_name)
            else:
                childs.append(child)

        if childs:
            sections.append(childs)

        return [
            BeautifulSoup("".join([str(tag) for tag in section]), "html.parser")
            for section in sections
        ]

    def split_text(self, text: str) -> List[str]:
        """Split the provided HTML text into smaller chunks based on specified headers and tags.

        Args:
            text (str): The HTML text to be split.

        Returns:
            List[str]: A list of text chunks obtained from the original HTML text.

        """
        # html_string = re.sub(r"\n", "", re.sub(r"(?:^|\n) +", "", text))
        html_string = text
        soup = BeautifulSoup(html_string, "html.parser")

        # Iterate through all tags and remove their attributes
        for tag in soup.find_all(True):
            if ("src" in tag.attrs and tag.name == "img") or (
                "href" in tag.attrs and tag.name == "a"
            ):
                # keep image sources and anchors
                continue

            tag.attrs = {}

        sections = [list(soup.children)]
        for header in self.headers:
            w = []
            for section in sections:
                w.extend(self._section_document(section, header))
            sections = w

        # split tables out into their own sections
        w = []
        for section in sections:
            w.extend(self._section_document(section, "table"))
        sections = w

        # process tables here!!!
        table_headers = ["h1", "h2", "h3", "h4", "h5", "h6", "strong"]
        while any(
            self._length_function(str(section)) > self._chunk_size
            for section in sections
        ):
            if not table_headers:
                break

            new_sections = []
            header_tag = table_headers.pop(0)
            for section in sections:
                table = section.find("table")

                if table is None and section.name == "table":
                    table = section
                    section = None

                if table:
                    table.extract()

                    # does the table have a header row?
                    header = table.find("thead")
                    if header:
                        header.extract()

                    # get the body and split it into rows
                    body = table.find("tbody")

                    if body:
                        rows = list(body.children)

                        new_tables = []
                        w = []
                        for row in rows:
                            if row.find(header_tag):
                                # start a new section
                                if w:
                                    new_tables.append(w)
                                w = []
                            w.append(row)

                        if w:
                            new_tables.append(w)

                        for new_table in new_tables:
                            new_table_tag = BeautifulSoup("", "html.parser").new_tag(
                                "table"
                            )
                            if header:
                                new_table_tag.append(
                                    BeautifulSoup(str(header), "html.parser")
                                )
                            new_table_body = BeautifulSoup("", "html.parser").new_tag(
                                "tbody"
                            )
                            for row in new_table:
                                new_table_body.append(
                                    BeautifulSoup(str(row), "html.parser")
                                )
                            new_table_tag.append(new_table_body)
                            new_sections.append(new_table_tag)

                    if section:
                        new_sections.append(section)
                elif section:
                    if not isinstance(section, list):
                        section = [section]
                    # split sections into smaller sections
                    low_level_tags = [
                        "p",
                        "span",
                    ]
                    for tag_name in low_level_tags:
                        w = []
                        for s in section:
                            w.extend(self._section_document(s, tag_name))
                        section = w
                    new_sections.extend(section)

            sections = new_sections

        # split sections into smaller sections

        texts = [str(section) for section in sections]
        chunks = self._merge_splits(texts, "")
        return chunks
