#!/usr/bin/env python3
"""Script to read a YAML file and convert it to a dictionary."""

import argparse
import logging
import sys
from pathlib import Path

import yaml
from colorlog import ColoredFormatter
from dotenv.main import DotEnv, set_key, unset_key

formatter = ColoredFormatter(
    "%(asctime)s %(log_color)s%(levelname)s%(reset)s %(message)s",
    datefmt=None,
    reset=True,
    log_colors={
        "DEBUG": "white",
        "INFO": "green",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "red,bg_white",
    },
    secondary_log_colors={},
    style="%",
)

logging.basicConfig(
    level=logging.INFO,
    force=True,
    format="%(asctime)s %(levelname)-8s: %(message)s",
)

for hdlr in logging.getLogger().handlers:
    hdlr.setFormatter(formatter)

logger = logging.getLogger(__name__)


def read_yaml_to_dict(file_path: str) -> dict:
    """Read a YAML file and return its contents as a dictionary.

    Args:
        file_path (str): Path to the YAML file

    Returns:
        dict: Contents of the YAML file as a dictionary

    Raises:
        FileNotFoundError: If the file doesn't exist
        yaml.YAMLError: If the file is not valid YAML

    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if not path.is_file():
        raise ValueError(f"Path is not a file: {file_path}")

    try:
        with open(path, "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
            return data if data is not None else {}
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML file {file_path}: {e}")


excluded_env_vars = {
    "dev_mode",
    "OPENAI_API_TYPE",
    "OPENAI_API_BASE",
    "OPENAI_API_VERSION",
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_ID",
    "AZURE_CLIENT_SECRET",
    "AZURE_SCOPE",
    "slack_bot_token",
    "cag_v3_api_key",
}


def load_dotenv() -> DotEnv:
    """Load environment variables from a .env file located two directories above the current file.

    Returns:
        DotEnv: An instance of DotEnv initialized with the .env file path.

    Raises:
        FileExistsError: If the .env file already exists at the specified location.

    """
    path = Path(__file__).parent.parent.joinpath(".env")
    path.touch(mode=0o600, exist_ok=True)
    denv = DotEnv(path)
    return denv


def unset_vars(denv: DotEnv):
    """Unset environment variables defined in a DotEnv object, except those specified in the excluded_env_vars list.

    Args:
        denv (DotEnv): The DotEnv object containing environment variables to unset.

    Notes:
        - Only variables not present in excluded_env_vars are unset.
        - The unset_key function is used to remove each variable from the environment file.

    """
    for var in denv.dict().keys():
        if var not in excluded_env_vars:
            unset_key(denv.dotenv_path, var)


def set_vars(denv: DotEnv, env: dict):
    """Set environment variables defined in a DotEnv object.

    Args:
        denv (DotEnv): The DotEnv object containing environment variables to set.
        env (dict): A dictionary of environment variables to set.

    """
    for item in env:
        var = item["name"]
        value = item.get("value")
        if var in excluded_env_vars or value is None:
            continue
        set_key(denv.dotenv_path, var, value)


def main():
    """Load YAML and write to .env."""
    parser = argparse.ArgumentParser(
        description="Read a Cloud Run execution YAML file and write its contents to a .env file"
    )
    parser.add_argument("file_path", help="Path to the YAML file to read")

    args = parser.parse_args()

    try:
        data = read_yaml_to_dict(args.file_path)
        env = data["spec"]["template"]["spec"]["containers"][0]["env"]
        denv = load_dotenv()
        unset_vars(denv)
        set_vars(denv, env)

    except (FileNotFoundError, ValueError, yaml.YAMLError, KeyError) as e:
        logger.error("Error setting environment variables: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
