import pytest

from dve.metadata_parser.utilities import generate_alphanumeric_type_name

@pytest.mark.parametrize(
    "mind, maxd, etype",
    [
        (None, 10, "AN10"),
        (10, 10, "AN10"),
        (1, 10, "AN1_10")
    ]
)
def test_generate_alphanumeric_type_name(mind, maxd, etype):
    _type = generate_alphanumeric_type_name(maxd, mind)
    assert _type == etype
