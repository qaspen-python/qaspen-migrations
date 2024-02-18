from __future__ import annotations
import typing

import pydantic
from qaspen.fields.base import Field  # noqa: TCH002
from qaspen.table.base_table import BaseTable  # noqa: TCH002


class TableDumpSchema(pydantic.BaseModel):
    table: type[BaseTable]
    table_fields_set: set[Field[typing.Any]] = pydantic.Field(
        default_factory=set,
    )

    def add_column_data(
        self,
        table_field: Field[typing.Any],
    ) -> None:
        self.table_fields_set.add(table_field)

    def all_field_names(self) -> set[str]:
        return {
            table_field._field_data.field_name
            for table_field in self.table_fields_set
        }

    class Config:  # noqa: D106
        arbitrary_types_allowed = True


class MigrationChangesSchema(pydantic.BaseModel):
    table: type[BaseTable]
    to_add_fields: set[Field[typing.Any]] = pydantic.Field(
        default_factory=set,
    )
    to_alter_fields: set[
        tuple[Field[typing.Any], Field[typing.Any]]
    ] = pydantic.Field(
        default_factory=set,
    )
    to_drop_fields: set[Field[typing.Any]] = pydantic.Field(
        default_factory=set,
    )

    @property
    def should_create_table(self) -> bool:
        return (
            bool(self.to_add_fields)
            and not bool(self.to_alter_fields)
            and not bool(self.to_drop_fields)
        )

    @property
    def should_skip_table(self) -> bool:
        return (
            not bool(self.to_add_fields)
            and not bool(self.to_alter_fields)
            and not bool(self.to_drop_fields)
        )

    @property
    def should_drop_table(self) -> bool:
        return (
            not bool(self.to_add_fields)
            and not bool(self.to_alter_fields)
            and bool(self.to_drop_fields)
        )

    class Config:  # noqa: D106
        arbitrary_types_allowed = True
