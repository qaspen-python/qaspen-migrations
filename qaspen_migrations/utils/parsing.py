from __future__ import annotations
import typing

from qaspen.columns.base import Column

from qaspen_migrations.schema import ColumnInfo


T = typing.TypeVar("T")


def parse_attribute_value_and_cast_or_none(
    object_to_get_from: typing.Any,
    attribute_name: str,
    cast_type: type[T],
) -> T | None:
    try:
        attribute_value: typing.Final = getattr(
            object_to_get_from,
            attribute_name,
        )
        return typing.cast(
            cast_type,  # type: ignore[valid-type]
            attribute_value,
        )
    except Exception:  # noqa: BLE001
        return None


def parse_int_attribute(
    object_to_parse_from: typing.Any,
    attribute_name: str,
) -> int | None:
    return parse_attribute_value_and_cast_or_none(
        object_to_parse_from,
        attribute_name,
        cast_type=int,
    )


def parse_float_attribute(
    object_to_parse_from: typing.Any,
    attribute_name: str,
) -> float | None:
    return parse_attribute_value_and_cast_or_none(
        object_to_parse_from,
        attribute_name,
        cast_type=float,
    )


def parse_column_attribute(
    object_to_parse_from: typing.Any,
    attribute_name: str,
) -> Column[typing.Any] | None:
    return parse_attribute_value_and_cast_or_none(
        object_to_parse_from,
        attribute_name,
        cast_type=Column[typing.Any],
    )


def table_column_to_column_info(
    table_column: Column[typing.Any],
) -> ColumnInfo:
    inner_column: typing.Final = parse_column_attribute(
        table_column,
        "inner_column",
    )
    return ColumnInfo(
        main_column_type=type(table_column),
        inner_column_type=(
            type(inner_column) if inner_column is not None else None
        ),
        db_column_name=table_column._column_data.column_name,
        is_null=table_column._column_data.is_null,
        database_default=(
            table_column._column_data.database_default.lower()
            if table_column._column_data.database_default is not None
            else None
        ),
        max_length=(
            parse_int_attribute(inner_column, "_max_length")
            or parse_int_attribute(table_column, "_max_length")
        ),
        scale=(
            parse_int_attribute(inner_column, "scale")
            or parse_int_attribute(table_column, "scale")
        ),
        precision=(
            parse_int_attribute(inner_column, "precision")
            or parse_int_attribute(table_column, "precision")
        ),
    )
