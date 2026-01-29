"""Slack Block Kit Model."""

from typing import Literal, Optional, TypeAlias

from pydantic import BaseModel

# region blocks


class PlainText(BaseModel):
    """A class to represent a plain text block."""

    type: Literal["plain_text"] = "plain_text"
    text: str
    emoji: Optional[bool] = None


class Markdown(BaseModel):
    """A class to represent a markdown block."""

    type: Literal["mrkdwn"] = "mrkdwn"
    text: str


class Divider(BaseModel):
    """A class to represent a divider block."""

    type: Literal["divider"] = "divider"


class Image(BaseModel):
    """A class to represent an image block."""

    type: Literal["image"] = "image"
    title: Optional[PlainText] = None
    image_url: str
    alt_text: str


TextBlock: TypeAlias = PlainText | Markdown

# endregion

# region sections


class SectionBase(BaseModel):
    """A class to represent a section block."""

    type: Literal["section"] = "section"


class Section(SectionBase):
    """A class to represent a section block with fields."""

    fields: list[TextBlock]


class SectionWithText(SectionBase):
    """A class to represent a section block with text."""

    text: TextBlock


class SectionWithAccessory(SectionBase):
    """A class to represent a section block with an accessory."""

    text: TextBlock
    accessory: Image


class Context(BaseModel):
    """A class to represent a context block."""

    type: Literal["context"] = "context"
    elements: list[TextBlock | Image]


class Header(BaseModel):
    """A class to represent a header block."""

    type: Literal["header"] = "header"
    text: PlainText


# endregion

# region rich text


class Style(BaseModel):
    """A class to represent the style of a text block."""

    bold: Optional[bool] = None
    italic: Optional[bool] = None
    strike: Optional[bool] = None


class Text(BaseModel):
    """A class to represent a text block."""

    type: Literal["text"] = "text"
    text: str
    style: Optional[Style] = None


class Emoji(BaseModel):
    """A class to represent an emoji block."""

    type: Literal["emoji"] = "emoji"
    name: str


class RichTextSection(BaseModel):
    """A class to represent a rich text section block."""

    type: Literal["rich_text_section"] = "rich_text_section"
    elements: list[Text | Emoji] = []


class RichTextPreformatted(BaseModel):
    """A class to represent a rich text preformatted block."""

    type: Literal["rich_text_preformatted"] = "rich_text_preformatted"
    elements: list[Text] = []


class RichTextQuote(BaseModel):
    """A class to represent a rich text quote block."""

    type: Literal["rich_text_quote"] = "rich_text_quote"
    elements: list[Text | Emoji] = []


class RichTextList(BaseModel):
    """A class to represent a rich text list block."""

    type: Literal["rich_text_list"] = "rich_text_list"
    style: Literal["ordered", "bullet"] = "bullet"
    elements: list[RichTextSection] = []


class RichText(BaseModel):
    """A class to represent a rich text block."""

    type: Literal["rich_text"] = "rich_text"
    elements: list[
        RichTextSection | RichTextList | RichTextPreformatted | RichTextQuote
    ] = []


# endregion

ParentBlock: TypeAlias = (
    Section
    | SectionWithAccessory
    | SectionWithText
    | Context
    | Header
    | Divider
    | RichText
)


class MessageBlocks(BaseModel):
    """A class to represent a message block."""

    blocks: list[ParentBlock]

    @property
    def serialize_blocks(self):
        """Serialize the blocks."""
        return [block.model_dump(exclude_none=True) for block in self.blocks]
