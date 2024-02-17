"""Module with column info schemas."""
from __future__ import annotations
import typing

import pydantic
from qaspen import BaseTable  # noqa: TCH002
from qaspen.fields import ArrayField
from qaspen.sql_type import primitive_types
from qaspen.sql_type.base import SQLType

from qaspen_migrations.exceptions import FieldParsingError
from qaspen_migrations.types_mapping import POSTGRES_TYPE_MAPPING
from qaspen_migrations.utils import (
    get_int_attribute,
)


if typing.TYPE_CHECKING:
    from qaspen.fields.base import Field


def prepare_string_value(column_info_value: typing.Any) -> str:
    if isinstance(column_info_value, str):
        return f'"{column_info_value}"'

    if isinstance(column_info_value, (int, float, bool)):
        return str(column_info_value)

    if issubclass(column_info_value, SQLType):
        return f"primitive_types.{column_info_value.__name__}"

    return str(column_info_value)


class ColumnInfoSchema(pydantic.BaseModel):
    column_name: str
    sql_type: type[SQLType]
    is_array: bool = False
    is_null: bool = True
    database_default: str | None = None
    max_length: int | None = None
    scale: int | None = None
    precision: int | None = None

    def __hash__(self) -> int:
        return hash(tuple(self.model_dump().values()))

    def __repr__(self) -> str:
        column_info_arguments: typing.Final = ",\n\t".join(
            [
                f"{column_info_key}={prepare_string_value(column_info_value)}"
                for column_info_key, column_info_value in self.model_dump(
                    exclude_defaults=True,
                    exclude_none=True,
                    exclude_unset=True,
                ).items()
            ],
        )
        return f"""ColumnInfoSchema(
            {column_info_arguments}
        )"""

    @classmethod
    def from_database(
        cls: type[ColumnInfoSchema],
        incoming_data: dict[typing.Any, typing.Any],
    ) -> ColumnInfoSchema:
        raise NotImplementedError

    @classmethod
    def from_field(
        cls: type[ColumnInfoSchema],
        model_field: Field[typing.Any],
    ) -> ColumnInfoSchema:
        sql_type = (
            model_field.inner_field._sql_type
            if isinstance(model_field, ArrayField)
            else model_field._sql_type
        )

        return ColumnInfoSchema(
            column_name=model_field._field_data.field_name,
            is_array=isinstance(model_field, ArrayField),
            sql_type=sql_type,
            is_null=model_field._field_data.is_null,
            database_default=model_field._field_data.database_default,
            max_length=get_int_attribute(model_field, "_max_length"),
            scale=get_int_attribute(model_field, "scale"),
            precision=get_int_attribute(model_field, "precision"),
        )


def _parse_numeric_attributes(
    attribute_name: str,
    sql_type: type[SQLType],
    values: dict[typing.Any, typing.Any],
) -> int | None:
    attribute_value: typing.Final = values.get(attribute_name)
    if sql_type not in [primitive_types.Decimal, primitive_types.Numeric]:
        return None
    try:
        return int(attribute_value)  # type: ignore[arg-type]
    except ValueError:
        return None


class PostgresColumnInfoSchema(ColumnInfoSchema):
    @classmethod
    def from_database(
        cls: type[ColumnInfoSchema],
        incoming_data: dict[typing.Any, typing.Any],
    ) -> ColumnInfoSchema:
        column_name: typing.Final = incoming_data.get("column_name")
        if column_name is None:
            raise FieldParsingError("Field name is empty.")

        incoming_type = incoming_data.get("sql_type", "")
        try:
            sql_type: typing.Final = POSTGRES_TYPE_MAPPING[
                incoming_type.strip("_")
            ]
        except LookupError as exception:
            raise FieldParsingError(
                f"Unknown sql type '{incoming_type}'.",
            ) from exception

        is_null: typing.Final = incoming_data.get("is_null") == "YES"
        max_length: typing.Final = incoming_data.get("max_length")
        database_default = incoming_data.get("database_default")
        scale: typing.Final = _parse_numeric_attributes(
            "scale",
            sql_type,
            incoming_data,
        )
        precision: typing.Final = _parse_numeric_attributes(
            "precision",
            sql_type,
            incoming_data,
        )
        is_array: typing.Final = incoming_type.startswith("_")
        return ColumnInfoSchema(
            column_name=column_name,
            is_array=is_array,
            sql_type=sql_type,
            is_null=is_null,
            database_default=database_default,
            max_length=max_length,
            scale=scale,
            precision=precision,
        )


class TableDumpSchema(pydantic.BaseModel):
    table: type[BaseTable]
    column_info_set: set[ColumnInfoSchema] = pydantic.Field(
        default_factory=set,
    )

    def add_column_data(
        self,
        column_data: ColumnInfoSchema,
    ) -> None:
        self.column_info_set.add(column_data)

    def all_column_names(self) -> set[str]:
        return {
            column_info.column_name for column_info in self.column_info_set
        }
