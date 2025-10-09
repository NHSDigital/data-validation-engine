"""Configuration for pytest."""

from pathlib import Path

# These need to be imported so that autouse fixtures are triggered.
from .fixtures import *  # pylint: disable=wildcard-import,unused-wildcard-import


def get_test_file_path(test_file_name: str) -> Path:
    """Get the path to a specific test file."""
    return Path(__file__).parent.joinpath("testdata", test_file_name)
