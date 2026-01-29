"""Utility functions for working with tar files."""

import tarfile
from pathlib import Path
from tempfile import mkdtemp


def is_safe_path(base_dir: Path, target_path: Path) -> bool:
    """Check if the target path is within the base directory."""
    try:
        return target_path.resolve().is_relative_to(base_dir.resolve())
    except AttributeError:
        # For older Python versions without is_relative_to()
        return str(target_path.resolve()).startswith(str(base_dir.resolve()))


def extract_tar(tar_path: Path) -> Path:
    """Extract the contents of a tar.gz file to a temporary directory securely.

    Args:
        tar_path (Path): The path to the tar.gz file to be extracted.

    Returns:
        Path: The path to the directory where the contents have been extracted.

    Raises:
        ValueError: If the archive contains unsafe paths.

    """
    temp = mkdtemp()
    extracted = Path(temp).joinpath("extracted")
    extracted.mkdir()

    with tarfile.open(tar_path, "r:gz") as tar:
        for member in tar.getmembers():
            member_path = extracted.joinpath(member.name)
            if not is_safe_path(extracted, member_path):
                raise ValueError(f"Unsafe file path detected: {member.name}")
            tar.extract(member, path=extracted)

    return extracted
