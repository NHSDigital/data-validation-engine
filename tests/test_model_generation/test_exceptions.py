import warnings

from dve.metadata_parser import exc


def test_loc_warning():
    message = "bad value"
    field = "field"
    with warnings.catch_warnings(record=True) as warns:
        warnings.warn(exc.LocWarning(msg=message, loc=field))

    assert isinstance(warns[0].message, exc.LocWarning)
    assert message in str(warns[0].message)
    assert field in str(warns[0].message)
