"""Tests for the domain types."""

import datetime as dt
from typing import Optional, Union
from warnings import catch_warnings

import pytest
from pydantic import BaseModel, ValidationError
from typing_extensions import Literal

from dve.metadata_parser import domain_types as hct

UTC = dt.timezone.utc
"""The UTC timezone."""


class ATestModel(BaseModel):
    nhsnumber: Optional[hct.NHSNumber]
    postcode: Optional[hct.Postcode]
    org_id: Optional[hct.OrgID]
    nhsnumber2: Optional[hct.permissive_nhs_number()]


class DatetimeModel(BaseModel):
    formatted_datetime: hct.FormattedDatetime


class ReportingPeriodModel(BaseModel):
    reporting_period_start: Optional[hct.reportingperiod(reporting_period_type="start")]
    reporting_period_end: Optional[hct.reportingperiod(reporting_period_type="end")]


@pytest.mark.parametrize(
    ("number", "expected"),
    [
        (7540381035, "7540381035"),
        (2138619753, "2138619753"),
        ("754 038 1035", "7540381035"),
        ("   213-861-9753   ", "2138619753"),
    ],
)
def test_nhsnumber(number, expected):
    model = ATestModel(nhsnumber=number)

    assert model.nhsnumber == expected
    assert len(model.nhsnumber) == 10


@pytest.mark.parametrize(
    "number",
    [
        "0123456789",
        "2222222222",
        "9510343064",
    ],
)
def test_possibly_invalid_nhsnumbers_emit_warnings(number: str):
    with pytest.warns():
        model = ATestModel(nhsnumber=number)
    assert model.nhsnumber == number


@pytest.mark.parametrize(
    "number",
    [
        "0123456789",
        "2222222222",
        "9510343064",
    ],
)
def test_possibly_invalid_nhsnumbers_dont_emit_warnings(number: str):
    with catch_warnings(record=True) as warning:
        model = ATestModel(nhsnumber2=number)
    assert model.nhsnumber2 == number
    assert warning == []


@pytest.mark.parametrize("number", [1234567891, "Helloworld", 123456789.1])
def test_nhsnumber_fails(number):
    with pytest.raises(ValidationError, match="NHS number invalid"):
        _ = ATestModel(nhsnumber=number)


@pytest.mark.parametrize(
    ("postcode", "expected"),
    [
        ("LS47 9AJ", "LS47 9AJ"),
        ("   LS47     9AJ   ", "LS47 9AJ"),
        ("Z1 4AU", "Z1 4AU"),
        ("Z1  4AU", "Z1 4AU"),
        ("N/A", None),
        (None, None),
    ],
)
def test_postcode(postcode, expected):
    model = ATestModel(postcode=postcode)

    assert model.postcode == expected


@pytest.mark.parametrize(("org_id", "expected"), [("AB123", "AB123"), ("ABCDE", "ABCDE")])
def test_org_id_passes(org_id, expected):
    model = ATestModel(org_id=org_id)

    assert model.org_id == expected


@pytest.mark.parametrize(("org_id",), [("ab123",), ("AB%42",), (" AB123 ",), ("",)])
def test_org_id_fails(org_id):
    with pytest.raises(ValidationError):
        model = ATestModel(org_id=org_id)


@pytest.mark.parametrize(
    ["datetime_to_validate", "expected"],
    [
        ("20211101132552", dt.datetime(2021, 11, 1, 13, 25, 52)),
        ("20211101132552Z", dt.datetime(2021, 11, 1, 13, 25, 52, 0, UTC)),
        ("20211101132552+0000", dt.datetime(2021, 11, 1, 13, 25, 52, 0, UTC)),
        ("20211101T132552", dt.datetime(2021, 11, 1, 13, 25, 52)),
        ("20211101T132552Z", dt.datetime(2021, 11, 1, 13, 25, 52, 0, UTC)),
        ("2021-11-01T13:25:52", dt.datetime(2021, 11, 1, 13, 25, 52)),
        ("2021-11-01T13:25:52Z", dt.datetime(2021, 11, 1, 13, 25, 52, 0, UTC)),
        ("2021-11-01T13:00:00+0000", dt.datetime(2021, 11, 1, 13, 0, 0, 0, UTC)),
        ("2021-11-01T13:00:00+00:00", dt.datetime(2021, 11, 1, 13, 0, 0, 0, UTC)),
        ("2021-11-01T13:25:52.123000Z", dt.datetime(2021, 11, 1, 13, 25, 52, 123000, UTC)),
        ("2021-11-01T13:25:52.123000Z", dt.datetime(2021, 11, 1, 13, 25, 52, 123000, UTC)),
        ("2021-11-01T132552.123000Z", dt.datetime(2021, 11, 1, 13, 25, 52, 123000, UTC)),
        ("20211101T13:25:52.123000Z", dt.datetime(2021, 11, 1, 13, 25, 52, 123000, UTC)),
        ("2020010113000001", dt.datetime(2020, 1, 1, 12, 0, 0, 0, UTC)),
        ("20200101T13000001", dt.datetime(2020, 1, 1, 12, 0, 0, 0, UTC)),
        ("2023-11-0122:11:16", dt.datetime(2023, 11, 1, 22, 11, 16)),
        ("2023-11-01 22:11:16", dt.datetime(2023, 11, 1, 22, 11, 16)),
        ("2023-11-0122:11:16", dt.datetime(2023, 11, 1, 22, 11, 16)),
        ("20231101 221116", dt.datetime(2023, 11, 1, 22, 11, 16)),
        ("20231101 221116Z", dt.datetime(2023, 11, 1, 22, 11, 16, 0, UTC)),
        ("20231101 221116.123000", dt.datetime(2023, 11, 1, 22, 11, 16, 123000)),
        ("20231101 221116.123000Z", dt.datetime(2023, 11, 1, 22, 11, 16, 123000, UTC)),
        ("20231101221116", dt.datetime(2023, 11, 1, 22, 11, 16)),
        ("20231101221116Z", dt.datetime(2023, 11, 1, 22, 11, 16, 0, UTC)),
        ("20231101221116.123000", dt.datetime(2023, 11, 1, 22, 11, 16, 123000)),
        ("20231101221116.123000Z", dt.datetime(2023, 11, 1, 22, 11, 16, 123000, UTC)),
        ("     2021-11-01 13:00:00     ", dt.datetime(2021, 11, 1, 13, 0, 0)),
        (dt.datetime(2021, 11, 1, 13, 25, 52), dt.datetime(2021, 11, 1, 13, 25, 52)),
        (
            dt.datetime(2021, 11, 1, 13, 25, 52, 0, UTC),
            dt.datetime(2021, 11, 1, 13, 25, 52, 0, UTC),
        ),
        (None, None),
    ],
)
def test_formatteddatetime(
    datetime_to_validate: Union[str, dt.datetime, None], expected: Optional[dt.datetime]
):
    """Test that FormattedDatetime correctly parses happy-path datetimes."""
    assert hct.FormattedDatetime.validate(datetime_to_validate) == expected


@pytest.mark.parametrize(
    "datetime_to_validate",
    ["", "1970-01-01", "1970-01-01T25:00", "1970-01-01T25:00:00", "01/01/1970 23:00:00"],
)
def test_formatteddatetime_raises_valueerror(datetime_to_validate: Union[str, dt.datetime]):
    """Test that FormattedDatetime correctly raises ValueErrors for rubbish datetimes."""
    with pytest.raises(ValueError):
        hct.FormattedDatetime.validate(datetime_to_validate)


@pytest.mark.parametrize(
    ["datetime_to_validate", "date_format", "timezone_treatment", "expected"],
    [
        ["1970-01-01", "%Y-%m-%d", "forbid", dt.datetime(1970, 1, 1)],
        ["1970-01-01", "%Y-%m-%d", "permit", dt.datetime(1970, 1, 1)],
        ["1970-01-01T23:00", "%Y-%m-%dT%H:%M", "forbid", dt.datetime(1970, 1, 1, 23)],
        ["1970-01-01T23:00", "%Y-%m-%dT%H:%M", "permit", dt.datetime(1970, 1, 1, 23)],
        [
            "1970-01-01T23:00:00+00:00",
            "%Y-%m-%dT%H:%M:%S%z",
            "require",
            dt.datetime(1970, 1, 1, 23, tzinfo=UTC),
        ],
        [
            "1970-01-01T23:00:00+00:00",
            "%Y-%m-%dT%H:%M:%S%z",
            "permit",
            dt.datetime(1970, 1, 1, 23, tzinfo=UTC),
        ],
        ["1970-01-01T23:00:00Z", None, "require", dt.datetime(1970, 1, 1, 23, tzinfo=UTC)],
        ["1970-01-01T23:00:00Z", None, "permit", dt.datetime(1970, 1, 1, 23, tzinfo=UTC)],
        ["01/01/1970 23:00:00", "%d/%m/%Y %H:%M:%S", "forbid", dt.datetime(1970, 1, 1, 23)],
        ["01/01/1970 23:00:00", "%d/%m/%Y %H:%M:%S", "permit", dt.datetime(1970, 1, 1, 23)],
    ],
)
def test_formatteddatetime_constraints(
    datetime_to_validate: Union[str, dt.datetime],
    date_format: str,
    timezone_treatment: Literal["forbid", "permit", "require"],
    expected: dt.datetime,
):
    """Test that FormattedDatetime correctly parses happy-path datetimes when constraints are set."""
    datetime_type = hct.formatteddatetime(date_format, timezone_treatment)
    assert datetime_type.validate(datetime_to_validate) == expected


@pytest.mark.parametrize(
    ["datetime_to_validate", "date_format", "timezone_treatment"],
    [
        ["1970-01-01", "%Y-%m-%d %H:%M:%S", "forbid"],
        ["1970-01-01", "%Y-%m-%d %H:%M:%S", "permit"],
        ["1970-01-01", "%Y-%m-%d %H:%M:%S", "require"],
        ["1970-01-01T23:00:00+00:00", "%Y-%m-%dT%H:%M:%S%z", "forbid"],
        ["1970-01-01T23:00:00+00:00", None, "forbid"],
        ["1970-01-01T23:00:00", "%Y-%m-%dT%H:%M:%S", "require"],
        ["1970-01-01T23:00:00", None, "require"],
    ],
)
def test_formatteddatetime_failing_constraints(
    datetime_to_validate: Union[str, dt.datetime],
    date_format: str,
    timezone_treatment: Literal["forbid", "permit", "require"],
):
    """Test that FormattedDatetime correctly fails sad-path datetimes when constraints are set."""
    datetime_type = hct.formatteddatetime(date_format, timezone_treatment)
    with pytest.raises(ValueError):
        datetime_type.validate(datetime_to_validate)


@pytest.mark.parametrize(
    "datetime_to_validate",
    ["", "1970-01-01", "1970-01-01T25:00", None],
)
def test_formatteddatetime_in_model_raises(datetime_to_validate: Union[str, dt.datetime, None]):
    """Ensure that FormattedDatetime emits an error when used wrongly in a model."""
    with pytest.raises(ValidationError):
        DatetimeModel(formatted_datetime=datetime_to_validate)


class DateModel(BaseModel):
    """Model for testing FormattedDate."""

    formatted_date: Optional[hct.conformatteddate("%Y-%m-%d")]
    formatted_date_constrained: Optional[hct.conformatteddate("%Y-%m-%d", ge="1970-01-01")]
    formatted_date_constrained2: Optional[hct.conformatteddate("%Y-%m-%d", lt="1970-01-01")]


@pytest.mark.parametrize(
    ["field", "date_to_validate", "expected"],
    (
        ("formatted_date", "1970-01-01", dt.date(1970, 1, 1)),
        ("formatted_date_constrained", "1970-01-01", dt.date(1970, 1, 1)),
        ("formatted_date_constrained2", "1969-12-30", dt.date(1969, 12, 30)),
    ),
)
def test_conformatteddate(field, date_to_validate, expected):
    """Test that ConFormattedDate correctly parses happy-path dates."""
    data = {field: date_to_validate}
    model = DateModel(**data)
    assert getattr(model, field) == expected


@pytest.mark.parametrize(
    ["field", "date_to_validate"],
    (
        ("formatted_date", "19700101"),
        ("formatted_date_constrained", "1969-12-30"),
        ("formatted_date_constrained2", "1970-01-01"),
    ),
)
def test_confomatteddate_raises(field, date_to_validate):
    """Test that ConFormattedDate correctly raises on dates"""
    data = {field: date_to_validate}
    with pytest.raises(ValidationError):
        model = DateModel(**data)


@pytest.mark.parametrize(
    ["field", "value", "expected"],
    (
        (
            ("reporting_period_start", "2020-01-01", dt.date(2020, 1, 1)),
            ("reporting_period_end", "2020-01-31", dt.date(2020, 1, 31)),
            ("reporting_period_start", "2020-02-01", dt.date(2020, 2, 1)),
            ("reporting_period_end", "2020-02-29", dt.date(2020, 2, 29)),
            ("reporting_period_start", "2021-12-01", dt.date(2021, 12, 1)),
            ("reporting_period_end", "2021-12-31", dt.date(2021, 12, 31)),
        )
    ),
)
def test_reportingperiod(field, value, expected):
    data = {field: value}
    model = ReportingPeriodModel(**data)
    assert getattr(model, field) == expected


@pytest.mark.parametrize(
    ["field", "value"],
    (
        (
            ("reporting_period_start", "2020-01-05"),
            ("reporting_period_end", "2020-01-27"),
            ("reporting_period_end", "2020-02-30"),
            ("reporting_period_start", "2020-12-30"),
        )
    ),
)
def test_reportingperiod_raises(field, value):
    data = {field: value}
    with pytest.raises(ValueError):
        model = ReportingPeriodModel(**data)
