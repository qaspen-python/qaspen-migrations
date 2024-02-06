from __future__ import annotations

import typing

from qaspen import BaseTable  # noqa: TCH002

from qaspen_migrations.inspector.schema import (
    ColumnDataSchema,
    TableDumpSchema,
)

if typing.TYPE_CHECKING:
    from qaspen.fields.base import Field


def dump_qaspen_model_to_model_dump(
    model: type[BaseTable],
) -> TableDumpSchema:
    table_dump: typing.Final = TableDumpSchema(model=model)
    model_field: Field[typing.Any]
    for model_field in model.all_fields():
        table_dump.add_column_data(ColumnDataSchema.from_field(model_field))
    return table_dump
