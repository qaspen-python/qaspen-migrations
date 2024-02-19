from __future__ import annotations
import re
import typing

from qaspen.columns.base import Column


def prepare_string_value(column_info_value: typing.Any) -> str:
    if isinstance(column_info_value, str):
        return f'"{column_info_value}"'

    if isinstance(column_info_value, (int, float, bool)):
        return str(column_info_value)

    if issubclass(column_info_value, Column):
        return f"columns.{column_info_value.__name__}"

    return str(column_info_value)


def unquote_inner_column(column_repr: str) -> str:
    pattern = r'(inner_column=)(")([^"]+\([^"]*\))(")'
    return re.sub(pattern, r"\1\3", column_repr, flags=re.DOTALL)


def to_column_repr(
    column_type: type[Column[typing.Any]],
    **column_kwargs: typing.Any,
) -> str:
    return unquote_inner_column(
        f"columns.{column_type.__name__}({to_string_kwargs(**column_kwargs)})",
    )


def to_string_kwargs(
    **column_kwargs: typing.Any,
) -> str:
    return ", ".join(
        f"{argument_name}={prepare_string_value(argument_value)}"
        for argument_name, argument_value in column_kwargs.items()
        if argument_value is not None
    )
