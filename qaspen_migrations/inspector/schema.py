from __future__ import annotations

import typing

import pydantic
from qaspen import BaseTable

from qaspen_migrations.exceptions import FieldParsingError
from qaspen_migrations.utils import check_inclusion, get_int_attribute

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


class ColumnDataSchema(pydantic.BaseModel):
    column_name: str
    is_primary: bool = False
    unique: bool = False
    is_null: bool = True
    database_default: typing.Any = None
    max_length: int | None = None
    numeric_scale: int | None = None
    numeric_precision: int | None = None

    @classmethod
    def from_database(
        cls: type[ColumnDataSchema],
        incoming_data: dict[typing.Any, typing.Any],
    ) -> ColumnDataSchema:
        column_name: typing.Final = incoming_data.get("column_name")
        if column_name is None:
            raise FieldParsingError("Field name is empty.")

        is_primary: typing.Final = check_inclusion(
            "PRIMARY KEY",
            incoming_data.get(
                "column_constraint",
            ),
        )

        unique: typing.Final = check_inclusion(
            "UNIQUE",
            incoming_data.get(
                "column_constraint",
            ),
        )

        is_null: typing.Final = incoming_data.get("is_null") == "YES"

        database_default = incoming_data.get("database_default")
        if isinstance(database_default, str):
            database_default = (
                None if "nextval" in database_default else database_default
            )

        max_length: typing.Final = incoming_data.get("max_length")
        numeric_scale: typing.Final = _parse_numeric_attributes(
            "numeric_scale",
            incoming_data,
        )
        numeric_precision: typing.Final = _parse_numeric_attributes(
            "numeric_precision",
            incoming_data,
        )

        return ColumnDataSchema(
            column_name=column_name,
            is_primary=is_primary,
            unique=unique,
            is_null=is_null,
            database_default=database_default,
            max_length=max_length,
            numeric_scale=numeric_scale,
            numeric_precision=numeric_precision,
        )

    @classmethod
    def from_field(
        cls: type[ColumnDataSchema],
        model_field: Field[typing.Any],
    ) -> ColumnDataSchema:
        return ColumnDataSchema(
            column_name=model_field.field_name,
            is_primary=model_field.is_primary,  # type: ignore[attr-defined] # TODO(insani7y): add is_primary to qaspen core type:
            unique=model_field.unique,  # type: ignore[attr-defined] # TODO(insani7y): add unique to qaspen core
            is_null=model_field.is_null,
            database_default=model_field.database_default,
            max_length=get_int_attribute(model_field, "max_length"),
            numeric_scale=get_int_attribute(model_field, "numeric_scale"),
            numeric_precision=get_int_attribute(model_field, "numeric_scale"),
        )


class TableDumpSchema(pydantic.BaseModel):
    model: type[BaseTable]
    column_data: list[ColumnDataSchema] = pydantic.Field(default_factory=list)

    def add_column_data(
        self,
        column_data: ColumnDataSchema,
    ) -> None:
        self.column_data.append(column_data)
