"""The networks module contains types for common network-related fields."""

from pathlib import Path
from typing import Any, TypeAlias, TypeGuard, get_args, get_origin

from pydantic import GetCoreSchemaHandler, UrlConstraints
from pydantic_core import CoreSchema, SchemaValidator, Url, ValidationError
from pydantic_core.core_schema import UrlSchema
from typing_extensions import Annotated

from cleansweep.utils.collections import dict_not_none


class PathLikeUrl(Url):
    """Extend the `Url` type to allow for `fspath` protocol."""

    def __new__(cls, url: str):
        """Create a new instance of the class."""
        if url.startswith("file://") and not url.startswith("file:///"):
            url = f"file:///{url[7:]}"
        return super().__new__(cls, url)

    def __fspath__(self) -> str:
        """Get the path as a string."""
        return self.__str__()

    @classmethod
    def __get_pydantic_core_schema__(  # pylint: disable=unused-argument
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Get the Pydantic core schema for the type."""
        return UrlSchema(type="url")

    @property
    def name(self) -> str:
        """Get the name of the file."""
        return str(self).rsplit("/", maxsplit=1)[-1]

    @property
    def suffix(self) -> str:
        """Get the file extension."""
        return self.name.rsplit(".", maxsplit=1)[-1]


HttpUrl: TypeAlias = Annotated[
    PathLikeUrl, UrlConstraints(max_length=2083, allowed_schemes=["http", "https"])
]
"""A type that will accept any http or https URL.

* TLD required
* Host required
* Max length 2083
"""

# pylint: disable=pointless-string-statement, line-too-long
CloudStorageUrl: TypeAlias = Annotated[
    PathLikeUrl,
    UrlConstraints(allowed_schemes=["gs"], host_required=True),
]
"""A type that will accept any "gs" URL.

* Limited scheme allowed
* Top-level domain (TLD) not required
* Host required

Assuming an input URL of `"gs://some_bucket/folder/file.json"`,
the types export the following properties:

- `scheme`: the URL scheme (`gs`), always set.
- `host`: the URL host (`some_bucket`), always set.
- `path`: optional path (`folder/file.json`).
"""

FtpUrl: TypeAlias = Annotated[
    PathLikeUrl,
    UrlConstraints(allowed_schemes=["ftp", "sftp"], host_required=True),
]
"""A type that will accept any "ftp" or "sftp" URL.

* Limited scheme allowed
* Top-level domain (TLD) not required
* Host required

Assuming an input URL of `"ftp://example.com/folder/file.json"`,
the types export the following properties:

- `scheme`: the URL scheme (`ftp`), always set.
- `host`: the URL host (`example.com`), always set.
- `path`: optional path (`folder/file.json`).
"""

FileUrl: TypeAlias = Annotated[PathLikeUrl, UrlConstraints(allowed_schemes=["file"])]
"""A type that will accept any file URL.

* Host not required
"""


def isurlinstance(url: Any, url_type: Any) -> TypeGuard[Url]:
    """Check if the given URL is an instance of the given URL type.

    This function is useful for checking if a URL is an instance of a specific URL type as opposed
    to just being a PathLikeUrl.

    Args:
        url (Any): The URL to check.
        url_type (Any): The URL type to check against.

    Returns:
        bool: True if the URL is an instance of the given URL type, otherwise False.

    """
    if get_origin(url_type) != Annotated:
        return False

    base_type, *metadata = get_args(url_type)
    if base_type != PathLikeUrl:
        return False

    validator = SchemaValidator(
        UrlSchema(
            type="url",
            **dict_not_none(**metadata[0].__dict__),
        )
    )
    try:
        validator.validate_python(url)
        return True
    except ValidationError:
        return False


def convert_to_url(
    file_path: str | PathLikeUrl,
) -> PathLikeUrl:
    """Get the path as a Url object.

    Args:
        file_path (Union[str, CloudStorageUrl, FileUrl, FtpUrl, HttpUrl]): The path to the
            file.

    Returns:
        PathLikeUrl: The path as a Url object.

    """
    if isinstance(file_path, str):
        return PathLikeUrl(file_path)

    return file_path


def file_type(file_path: Url | PathLikeUrl) -> str:
    """Get the file type of the file.

    Args:
        file_path (PathLikeUrl): The path to the file.

    Returns:
        str: The file type of the config file.

    """
    if file_path.path is None:
        raise ValueError("The file path is not set")
    return Path(file_path.path).suffix


def raw_path(
    file_path: CloudStorageUrl | FileUrl | FtpUrl | HttpUrl | PathLikeUrl | Url,
) -> str:
    """Get the raw path of the file by stripping the scheme.

    Args:
        file_path (Union[CloudStorageUrl, FileUrl, FtpUrl, HttpUrl]): The path to the config
            file.

    Returns:
        str: The raw path of the config file.

    """
    url_host = file_path.host
    if not url_host:
        url_host = ""

    return f"{url_host}{file_path.path}"
