"""Wrapping functions for wrapping generic functions"""

import warnings
from collections.abc import Callable, Iterable
from typing import Any, Optional, Union

import pydantic

from dve.metadata_parser import exc

PydanticCompatible = Callable[
    [Any, dict[str, Any], pydantic.fields.ModelField, pydantic.BaseConfig], Any
]
"""Function Compatable with pydantic
        Args:
            value (Any): Value to be validated
            values (dict[str, Any]): dict of previously validated fields
            field (pydantic.fields.ModelField): field object containing field name and type
            config (pydantic.BaseConfig): the config that determines things like aliases

"""


def error_handler(
    error_type: Union[type[Exception], type[Warning]],
    error_message: str,
    field: pydantic.fields.ModelField,
):
    """Determines whether to raise an error or warning based on error_type

    Args:
        error_type (Union[type[Exception], type[Warning]]): type of error to raise
        error_message (str): message to apply
        field (pydantic.fields.ModelField): field that caused the error to be raised

    Raises:
        error_type

    """
    if issubclass(error_type, exc.LocWarning):
        warnings.warn(error_type(msg=error_message, loc=field.name))
    elif issubclass(error_type, Warning):
        warnings.warn(error_message, error_type)
    else:
        raise error_type(error_message)


def pydantic_wrapper(
    error_type: Union[type[Exception], type[Warning]],
    error_message: str,
    *field_names: str,
    failure_function: Callable = lambda x: x is False,
    return_result: bool = True,
    **kwargs,
) -> Callable[[Callable], PydanticCompatible]:
    """Wraps generic functions and returns a pydantic compatible function
    takes an error type and error_message to allow the failure of a function to be customised

    takes field_names of fields to be included into the generic function. should be in order
    that they appear in the function.

    takes a function that will result in the passed exception being raised

    Args:
        error_type (type[Exception]): The exception type to be raised if the failure_function
        evaluates to True
        error_message (str): Message to be passed to the above exception
        failure_function (Optional[Callable]): A callable that when it evaluates to True
        raises the above exception. Defaults to lambda x: x is False.
        return_result (bool): Whether to return the result from the wrapped function or the
        or the original value that was passed.
        True should be used when the function transforms the data in some way
        e.g. stripping whitespace from NHS numbers.
        False should be used when the function is a comparison
        e.g. x > y. in this case the value of the field is returned rather than the bool.

    """

    def wrapper(
        func: Callable,
    ) -> PydanticCompatible:
        """Wraps the passed function and returns a function with a pydantic compatible calling
        signature

        Args:
            func (Callable): function to be wrapped

        Raises:
            error_type: error passed in with the message given

        Returns:
            Callable: wrapped function with call signature:
            (value: Any, values: dict, field: ModelField, config: BaseConfig) -> Any

        """

        def inner(
            value: Any,
            values: dict[str, Any],
            field: pydantic.fields.ModelField,  # pylint: disable=unused-argument
            config: pydantic.BaseConfig,  # pylint: disable=unused-argument
        ) -> Any:
            fields = [values.get(name) for name in field_names]
            result = None
            try:
                result = func(value, *fields, **kwargs)
            except (ValueError, TypeError, AssertionError):
                error_handler(error_type, error_message, field)
            else:
                # only want to check if the return is False if we are returning the value
                if not return_result and failure_function(result):
                    error_handler(error_type, error_message, field)
            if return_result:
                return result
            return value

        inner.__name__ = f"wrapped_{func.__name__}"
        return inner

    return wrapper


validator_args = pydantic.validator.__kwdefaults__.copy()


def create_validator(
    function: Callable,
    field: str,
    error_type: type[Exception],
    error_message: str,
    fields: Optional[Iterable[str]] = None,
    return_result=True,
    **kwargs,
):
    """Creates a pydantic_validator from a function

    Args:
        function (Callable): function to wrap
        field (str): field validator is applier to
        fields (Iterable[str]): other fields to be included in validation (in order of arguments)
        error_type (type[Exception]): Error to be raised on failure
        error_message (str): Message to be raised on failure
        kwargs:
            pydantic_wrapper_kwargs:
                failure_function: function called to cause the function to raise the passed
                error when evaluates to True
                return_result: bool = True: whether to return the result of the wrapped function
                or the unchanged value

            pydantic.validator kwargs:
                pre: bool = False
                each_item: bool = False
                always: bool = False
                check_fields: bool = True
                whole: bool = None
                allow_reuse: bool = False

            function kwargs

    Returns:
        classmethod: wrapped function wrapped by pydantics validator

    """
    validator_kwargs = _validator_kwargs(
        **{key: kwargs.pop(key, default) for key, default in validator_args.items()}
    )

    if fields is None:
        fields = []

    wrapped = pydantic_wrapper(
        error_type,
        error_message,
        *fields,
        return_result=return_result,
        **kwargs,
    )(function)

    validator_kwargs.update(allow_reuse=True)
    validator = pydantic.validator(field, **validator_kwargs)(wrapped)
    return validator


@pydantic.validate_arguments
def _validator_kwargs(
    pre: bool = False,
    each_item: bool = False,
    always: bool = False,
    check_fields: bool = True,
    whole: Optional[bool] = None,
    allow_reuse: bool = False,
    **kwargs,  # pylint: disable=unused-argument
):
    return {
        "pre": pre,
        "each_item": each_item,
        "always": always,
        "check_fields": check_fields,
        "whole": whole,
        "allow_reuse": allow_reuse,
    }
