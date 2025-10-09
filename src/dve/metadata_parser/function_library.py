"""Function library for wrapping into pydantic validators"""

import functools

import pydantic


def _nullcheck(func):
    """Decorator to nullcheck incoming values from pydantic validators
    this removes the need to add a nullcheck to the value for each function.
    Nullchecks may still be needed for any additional values passed in.

    e.g.
    ```python
    @_nullcheck # nullchecks value but not other_field
    def func(value, other_field):
        if other_field is None:
            # do null thing
        # logic
    ```
    """

    @functools.wraps(func)
    def inner(value, *args, **kwargs):
        if value is None or not str(value).strip():
            return None
        return func(value, *args, **kwargs)

    return inner


# demo function
@_nullcheck
@pydantic.validate_arguments
def normalise(value, capitalize: bool = False):  # pragma: no cover
    """Normalises a string by capitalising it"""
    if capitalize:
        return str(value).capitalize()
    return value


@_nullcheck
def exclude_word(value, word: str):
    """Returns None if the word is present in the value"""
    if word.lower() in str(value).lower():
        return None
    return value


@_nullcheck
@pydantic.validate_arguments
def split(value, split_on: str, keep: int = 0):
    """Splits a string on a given delimiter and keeps only the value at the given index
    defaults to 0
    """
    try:
        return str(value).split(split_on)[keep]
    except IndexError as exc:
        raise ValueError from exc


def static_key(value):  # pylint: disable=W0613
    """Return a fixed value, for use as a static join key"""
    return 1
