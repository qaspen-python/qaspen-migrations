from __future__ import annotations

import typing

from qaspen import BaseTable  # noqa: TCH002

from qaspen_migrations.inspector.schema import (
    ColumnInfoSchema,
    TableDumpSchema,
)

if typing.TYPE_CHECKING:
    from qaspen.fields.base import Field


def dump_qaspen_model_to_model_dump(
    table: type[BaseTable],
) -> TableDumpSchema:
    table_dump: typing.Final = TableDumpSchema(table=table)
    model_field: Field[typing.Any]
    for model_field in table.all_fields():
        table_dump.add_column_data(ColumnInfoSchema.from_field(model_field))
    return table_dump
