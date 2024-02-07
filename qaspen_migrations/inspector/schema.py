from __future__ import annotations

import re
import typing

import pydantic
from qaspen import BaseTable  # noqa: TCH002

from qaspen_migrations.exceptions import FieldParsingError
from qaspen_migrations.utils import (
    get_int_attribute,
)

if typing.TYPE_CHECKING:
    from qaspen.fields.base import Field


def _parse_numeric_attributes(
    attribute_name: str,
    values: dict[typing.Any, typing.Any],
) -> int | None:
    attribute_value: typing.Final = values.get(attribute_name)
    if values.get("data_type") not in ["decimal", "numeric"]:
        return None
    try:
        return int(attribute_value)  # type: ignore[arg-type]
    except ValueError:
        return None


def _extract_min_and_max_from_check(
    column_name: str,
    check_clause: str | None,
) -> tuple[float | None, float | None]:
    if check_clause is None:
        return None, None

    min_pattern = rf"{column_name}\s*>=\s*(\d+)"
    max_pattern = rf"{column_name}\s*<=\s*(\d+)"

    # Search for minimum value
    min_match = re.search(min_pattern, check_clause)
    minimum_value = int(min_match.group(1)) if min_match else None

    # Search for maximum value
    max_match = re.search(max_pattern, check_clause)
    maximum_value = int(max_match.group(1)) if max_match else None

    return minimum_value, maximum_value


class ColumnInfoSchema(pydantic.BaseModel):
    column_name: str
    is_null: bool = True
    database_default: typing.Any = None
    max_length: int | None = None
    scale: int | None = None
    precision: int | None = None

    @classmethod
    def from_database(
        cls: type[ColumnInfoSchema],
        incoming_data: dict[typing.Any, typing.Any],
    ) -> ColumnInfoSchema:
        column_name: typing.Final = incoming_data.get("column_name")
        if column_name is None:
            raise FieldParsingError("Field name is empty.")

        is_null: typing.Final = incoming_data.get("is_null") == "YES"
        max_length: typing.Final = incoming_data.get("max_length")
        database_default = incoming_data.get("database_default")
        if isinstance(database_default, str):
            database_default = (
                None
                if "nextval" in database_default
                else database_default.split("::")[0]
            )

        scale: typing.Final = _parse_numeric_attributes(
            "scale",
            incoming_data,
        )
        precision: typing.Final = _parse_numeric_attributes(
            "precision",
            incoming_data,
        )

        return ColumnInfoSchema(
            column_name=column_name,
            is_null=is_null,
            database_default=database_default,
            max_length=max_length,
            scale=scale,
            precision=precision,
        )

    @classmethod
    def from_field(
        cls: type[ColumnInfoSchema],
        model_field: Field[typing.Any],
    ) -> ColumnInfoSchema:
        return ColumnInfoSchema(
            column_name=model_field._field_data.field_name,
            is_null=model_field._field_data.is_null,
            database_default=model_field._field_data.database_default,
            max_length=get_int_attribute(model_field, "_max_length"),
            scale=get_int_attribute(model_field, "scale"),
            precision=get_int_attribute(model_field, "precision"),
        )


class TableDumpSchema(pydantic.BaseModel):
    table: type[BaseTable]
    column_info: list[ColumnInfoSchema] = pydantic.Field(default_factory=list)

    def add_column_data(
        self,
        column_data: ColumnInfoSchema,
    ) -> None:
        self.column_info.append(column_data)
