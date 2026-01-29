"""HTML Splitter module."""

import copy
import html
import logging
import re
from typing import Callable, List

import html2text
from bs4 import BeautifulSoup
from bs4.element import PageElement, Tag
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_text_splitters.base import TextSplitter

from cleansweep.chunk.jill import JillSplitter

logger = logging.getLogger(__name__)


class HTMLSectionSplitter(TextSplitter):
    """Split HTML text into sections based on headers and tags."""

    headers = ["h6", "h5", "h4", "h3", "h2"]

    def __init__(
        self,
        chunk_size: int = 4000,
        length_function: Callable[[str], int] = len,
    ) -> None:
        """Create a new TextSplitter.

        Args:
            chunk_size: Maximum size of chunks to return
            chunk_overlap: Overlap in characters between chunks
            length_function: Function that measures the length of given chunks

        """
        super().__init__(chunk_size, 0, length_function, False, False, False)
        self._text_splitter = RecursiveCharacterTextSplitter(
            # separators=["\n\n", "\n", r"<\/[ \w]+>", "/>", " ", ""],
            # is_separator_regex=True,
            chunk_size=chunk_size,
            chunk_overlap=200,
            length_function=length_function,
            keep_separator=False,
            add_start_index=False,
            strip_whitespace=False,
        )

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
        pattern = r"h\d|p|span|table|em|strong|pre|code|blockquote|ul|ol|li|a|img"

        def inner_sections(soup, tag_name, childs=childs, sections=sections):
            for child in soup.children:
                if child.name == tag_name:
                    if childs:
                        sections.append(childs)
                        childs = []
                    childs.append(child)
                elif child.name is not None and re.match(pattern, child.name):
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
            elif child.name is not None and re.match(pattern, child.name):
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

    def _simple_soup(self, html: str) -> BeautifulSoup:
        soup = BeautifulSoup(html, "html.parser")

        # spans are pretty pointless, remove them
        for span in soup.find_all("span"):
            span.unwrap()

        def simplify(node):
            # Recursively simplify all children first
            for child in node.find_all(recursive=False):

                # if the tag and it's children are empty, remove them all
                if not child.text.strip():
                    child.extract()
                    continue

                simplify(child)

            # If the current tag has exactly one child and both have the same name
            if len(node.contents) == 1 and isinstance(node.contents[0], Tag):
                child = node.contents[0]
                if node.name == child.name:
                    # Replace the parent tag with the child tag
                    node.replace_with(child)

        simplify(soup)

        # Unwrap all <div> tags that contain html
        soup = self._unwrap_divs_with_html(soup)

        return soup

    def _unwrap_divs_with_html(self, soup: BeautifulSoup) -> BeautifulSoup:
        """Unwrap all <div> tags in the provided object, removing the <div> tags but keeping their contents.

        Args:
            soup (BeautifulSoup): A BeautifulSoup object containing the HTML content to be processed.

        Returns:
            BeautifulSoup: The modified BeautifulSoup object with all <div> tags unwrapped.

        """
        # Find all <div> tags
        for div in soup.find_all("div"):
            # Check if the <div> contains any nested tags (not just text)
            if any(child.name for child in div.children):
                div.unwrap()  # Remove the <div> but keep its contents

        return soup

    def _filter_contents(self, tag: Tag, *val) -> List[Tag]:
        """Filter the children of a given tag based on specified tag names.

        Args:
            tag (Tag): The parent tag whose children are to be filtered.
            *val (Tuple[str, ...]): A variable length argument list of tag names to filter by.

        Returns:
            List[Tag]: A list of child tags that match the specified tag names.

        """
        return [
            child
            for child in tag.children
            if isinstance(child, Tag) and child.name in val
        ]

    def _filter_only_tags(self, tag: Tag) -> List[Tag]:
        """Filter out non-tag elements (like text and comments) from the given BeautifulSoup tag's contents.

        Args:
            tag (Tag): The BeautifulSoup tag to filter.

        """
        return [child for child in tag.contents if isinstance(child, Tag)]

    def _clean_tags(self, soup: BeautifulSoup) -> BeautifulSoup:
        # Iterate through all tags and remove their attributes, remove tags that are empty
        for tag in soup.find_all(True):
            if ("src" in tag.attrs and tag.name == "img") or (
                "href" in tag.attrs and tag.name == "a"
            ):
                # keep image sources and anchors
                continue

            tag.attrs = {}

            # fetching text from tag and remove whitespaces
            if len(tag.get_text(strip=True)) == 0:

                # Remove empty tag
                tag.extract()
        return soup

    def _append_list_to_tag(self, tag: Tag, lst: List[Tag]) -> Tag:
        for item in [list_item for list_item in lst]:
            tag.append(item)
        return tag

    def _convert_tables(self, soup: BeautifulSoup) -> BeautifulSoup:
        # iterate through all tables and convert any used for layout into divs

        def append_row_cells_to_tag(row, tag):
            for cell in self._filter_contents(row, "td"):
                tag = self._append_list_to_tag(tag, cell.contents)

        for table in soup.find_all("table"):
            header = table.find("thead", recursive=False)
            body = table.find("tbody", recursive=False)

            if header:
                # evaluate the header usage
                header_rows = self._filter_contents(header, "tr")

                if header_rows and self._is_header_row(header_rows[0]):
                    # if table has a header assume it is a table so ignore
                    continue

                if not body:
                    body = soup.new_tag("tbody")
                    for row in header_rows:
                        body.append(row)

                    table.append(body)
                    header.extract()

            if not body:
                # no body and no header, extract the table
                table.extract()
                continue

            rows = self._filter_contents(body, "tr")
            tag_type = "div"
            if (
                len(rows) == 1
                and len(rows[0].contents) == 1
                and self._is_header_row(rows[0])
            ):
                # single row with single cell and single tag, assume it's a header
                tag_type = "h3"
            elif len(rows[0].contents) == 2:
                # table with two columns, assume col 1 is header and col 2 is data
                new_rows = []
                for row in rows:
                    _nt = soup.new_tag("h4")
                    _nt = self._append_list_to_tag(_nt, row.contents[0].contents)
                    _nc = soup.new_tag("td")
                    _nc.append(_nt)
                    _nr = soup.new_tag("tr")
                    _nr.append(_nc)
                    row.contents[0].extract()
                    new_rows.append(_nr)
                    new_rows.append(row)
                rows = new_rows
            elif len(rows[0].contents) > 1 and len(rows) > 1:
                # multiple cells in first row, assume it is a table so ignore
                continue

            nt = soup.new_tag(tag_type)
            for row in rows:
                if (
                    len(rows) > 1
                    and len(row.contents) == 1
                    and self._is_header_row(row)
                ):
                    # single cell with single tag, assume it's a header
                    tag_type = "h2"
                    _nt = soup.new_tag(tag_type)

                    append_row_cells_to_tag(copy.deepcopy(row), _nt)

                    nt.append(_nt)
                    continue

                for cell in self._filter_contents(row, "td"):
                    for child in [c for c in cell.contents]:
                        nt.append(child)
            table.replace_with(nt)
        return soup

    def _is_header_row(self, row) -> bool:
        cells = len(row.contents)
        headings = [
            th
            for th in self._filter_contents(row, "td", "th")
            if self._filter_only_tags(th)
            and self._filter_only_tags(th)[0].name
            in ["h1", "h2", "h3", "h4", "h5", "h6", "strong"]
            and len(self._filter_only_tags(th)) == 1
        ]
        return len(headings) == cells

    def _process_table(
        self, soup: BeautifulSoup, section: Tag | None, table: Tag
    ) -> list[Tag] | None:

        def set_header_from_row(row, header=None):

            if self._is_header_row(row):
                header = soup.new_tag("thead")
                header.append(row)
            return header

        def create_new_table():
            new_table = soup.new_tag("table")
            if header:
                new_table.append(copy.deepcopy(header))
            return new_table

        table.extract()
        new_sections = []

        body = table.find("tbody")
        if body:
            body.extract()
        else:
            return

        header = table.find("thead")
        if header:
            header.extract()
        else:
            # check the first row, is it a header row?
            row = [tr for tr in body.contents if tr.name == "tr"][0]
            header = set_header_from_row(row)

        # create new tables that are smaller than chunk_size
        new_table = create_new_table()
        new_body = soup.new_tag("tbody")

        for row in [r for r in body.contents]:
            if row.name == "tr":

                if (
                    self._length_function(str(new_table))
                    + self._length_function(str(row))
                    + self._length_function(str(new_body))
                ) > self._chunk_size and len(new_body.contents) > 0:
                    new_table.append(new_body)
                    new_sections.append(new_table)
                    new_table = create_new_table()
                    new_body = soup.new_tag("tbody")
                new_body.append(row)

        if len(new_body.contents) > 0:
            new_table.append(new_body)
            new_sections.append(new_table)

        if section and section.text.strip() != "":
            new_sections.append(section)
        return new_sections

    def _process_section(self, section: Tag | List[Tag]) -> List[Tag]:
        new_sections = []
        if len(str(section)) <= self._chunk_size:
            new_sections.append(section)
            return new_sections

        if self.is_plain_text(str(section)):
            new_sections.extend(self._text_splitter.split_text(str(section)))

        else:
            if not isinstance(section, list):
                section = [section]
            # split sections into smaller sections
            low_level_tags = [
                "p",
            ]
            for tag_name in low_level_tags:
                w = []
                for s in [section]:
                    w.extend(self._section_document(s, tag_name))
                section = w

            # check the sections aren't still massive
            for s in section:
                if len(str(s)) <= self._chunk_size:
                    new_sections.append(s)
                    continue

                if s.name == "[document]":
                    s = s.contents[0]

                tag_type = s.name

                if not hasattr(s, "contents"):
                    new_sections.append(s)
                    continue

                s_chunks = self._text_splitter.split_text(
                    "".join([str(tag) for tag in s.contents])
                )
                for chunk in s_chunks:
                    _soup = BeautifulSoup(chunk, "html.parser")
                    _nt = _soup.new_tag(tag_type)
                    self._append_list_to_tag(_nt, _soup.contents)
                    new_sections.append(_nt)
        return new_sections

    def _merge_splits(self, splits, separator) -> List[str]:

        # merge chunks
        chunks = []
        w = []
        separator_len = self._length_function(separator)
        total = 0
        for text in splits:
            _len = self._length_function(text)
            if (total + _len + separator_len) > self._chunk_size:
                chunks.append(separator.join(w))
                w = []
                total = 0
            w.append(text)
            total = self._length_function(separator.join(w))
            if total > self._chunk_size:
                logger.warning(
                    "Created chunk of size %d, which is longer than the specified %d",
                    total,
                    self._chunk_size,
                )

        if w:
            chunks.append(separator.join(w))
        return chunks

    def _old_split_text(self, text: str) -> List[str]:
        """Split the provided HTML text into smaller chunks based on specified headers and tags.

        Args:
            text (str): The HTML text to be split.

        Returns:
            List[str]: A list of text chunks obtained from the original HTML text.

        """
        soup = self._simple_soup(text.replace("\xa0", " "))
        soup = self._clean_tags(soup)
        # soup = self._convert_tables(soup)

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

        # split sections
        sections = [content for section in sections for content in section.contents]

        new_sections = []

        for section in sections:
            table = None
            if section.name == "table":
                table = section
                section = None
            else:
                table = section.find("table")

            if isinstance(table, int):
                table = None

            if table:
                # just add the table to the new sections
                new_sections.append(table)

            elif section:

                new_sections.extend(self._process_section(section))

        sections = new_sections

        texts = [str(section) for section in sections]
        chunks = self._merge_splits(texts, "")

        new_chunks = []
        for chunk in chunks:
            if self._length_function(chunk) <= self._chunk_size:
                new_chunks.append(chunk)
                continue

            soup = BeautifulSoup(chunk, "html.parser")
            soup = self._convert_tables(soup)
            sections = []
            for section in soup.contents:
                table = None
                if section.name == "table":
                    table = section
                    section = None
                else:
                    table = section.find("table")

                if isinstance(table, int):
                    table = None

                if table:
                    r = self._process_table(soup, section, table)
                    if r:
                        sections.extend(r)

                elif section:

                    sections.extend(self._process_section(section))

            _texts = [str(section) for section in sections]
            new_chunks.extend(self._merge_splits(_texts, ""))

        chunks = new_chunks

        # brute force, anything still too big gets converted to markdown and smooshed
        new_chunks = []
        h = html2text.HTML2Text()
        for chunk in chunks:
            if self._length_function(chunk) <= self._chunk_size:
                new_chunks.append(chunk)
                continue

            markdown = h.handle(chunk)
            new_chunks.extend(self._text_splitter.split_text(markdown))

        return new_chunks

    def split_text(self, text: str) -> List[str]:
        """Split the provided HTML text into smaller chunks based on specified headers and tags.

        Args:
            text (str): The HTML text to be split.

        Returns:
            List[str]: A list of text chunks obtained from the original HTML text.

        """
        soup = self._simple_soup(text.replace("\xa0", " "))
        soup = self._clean_tags(soup)

        if self._length_function(str(soup)) <= self._chunk_size:
            return [str(soup)]

        js = JillSplitter(
            chunk_size=self._chunk_size,
            length_function=self._length_function,
            chunk_overlap=self._chunk_overlap,
        )
        h = html2text.HTML2Text()

        chunkety_chunks = []
        for jill_chunk in js.split_text(str(soup)):
            if self._length_function(jill_chunk) <= self._chunk_size:
                chunkety_chunks.append(jill_chunk)
            else:
                # convert to markdown and text split
                markdown = h.handle(jill_chunk)
                if self._length_function(markdown) <= self._chunk_size:
                    chunkety_chunks.append(markdown)
                else:
                    chunkety_chunks.extend(self._text_splitter.split_text(markdown))

        return chunkety_chunks

    def is_plain_text(self, input_string: str) -> bool:
        """Check if the input string is plain text (not HTML).

        Args:
            input_string (str): The string to check.

        Returns:
            bool: True if the string is plain text, False if it contains HTML.

        """
        soup = BeautifulSoup(input_string, "html.parser")
        # If the soup has any tags, it's likely HTML
        return soup.find() is None and soup.text == html.unescape(input_string)
