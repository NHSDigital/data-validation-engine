"""Tests for dynamic generation of pydantic models"""

# pylint: disable=redefined-outer-name
import json

import pydantic
import pytest

from dve.metadata_parser import exc
from dve.metadata_parser.utilities import chain_get


def test_chain_get():
    assert chain_get("str", __builtins__) == str
    assert chain_get("constr", pydantic) == pydantic.constr


def test_chain_get_failure():
    type_ = "someunknowntype"
    with pytest.raises(exc.TypeNotFoundError, match=type_):
        chain_get(type_, pydantic, __builtins__)
