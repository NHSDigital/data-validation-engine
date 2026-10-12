"""Function implementations to be registered as UDFs."""

# pylint: disable=invalid-name,missing-function-docstring
import datetime
from decimal import Decimal
from typing import Optional, Union

from dve.metadata_parser.domain_types import NHSNumber


def over_10k(x: float) -> bool:  # noqa: D103
    return x > 10000


def over_1k(x: float) -> bool:  # noqa: D103
    return x > 1000


def under_10k(x: float) -> bool:  # noqa: D103
    return x < 10000


def under_5k(x: float) -> bool:  # noqa: D103
    return x < 5000


def over_5(x: float) -> bool:  # noqa: D103
    return x > 5


def over_10(x: float) -> bool:  # noqa: D103
    return x > 10


def x_not_greater_than_y(x: float, y: float) -> bool:  # noqa: D103
    return x <= y


def date_in_current_financial_year(test_date: datetime.date) -> bool:
    """Checks the date is in the current financial year"""
    current_fiscal_year_start: datetime.date = datetime.date(datetime.date.today().year, 4, 1)
    return test_date >= current_fiscal_year_start


def check_correct_numeric_signage(
    val: Union[float, int, Decimal], expected_sign: str = "+/-"
) -> Optional[bool]:
    """Check the expected sign of a numeric type."""
    if val is None:
        return None
    if expected_sign not in ("+/-", "+", "-"):
        return None

    if expected_sign == "+/-":
        return True
    if expected_sign == "+":
        return val >= 0
    if expected_sign == "-":
        return val <= 0
    return None


def number_matches_within_tolerance(
    comparator: Decimal, number: Decimal, tolerance: Decimal
) -> Optional[bool]:
    """Check that `number` matches `comparator` to within a fixed tolerance."""
    if comparator is None or number is None or tolerance is None:
        return None

    return abs(number - comparator) <= abs(tolerance)


def number_matches_within_percentage(
    comparator: Decimal, number: Decimal, percentage: Decimal
) -> Optional[bool]:
    """Check that `number` matches `comparator` to within a percentage of the value of `comparator`.

    `tolerance` should be a decimal representaion of the percentage (e.g. 0.01 for 1%).

    """
    if percentage is None or comparator is None:
        return None

    tolerance = comparator * percentage
    return number_matches_within_tolerance(comparator, number, tolerance)


def nhsno_mod11_check(nhs_no: str) -> bool:
    """Check that a given nhs number passes mod11 checks."""
    return NHSNumber.check_validates(nhs_no)
