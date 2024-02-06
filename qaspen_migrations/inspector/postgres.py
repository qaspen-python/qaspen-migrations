from __future__ import annotations

import contextlib
import typing

from psycopg.errors import UndefinedTable
from qaspen_psycopg.engine import PsycopgEngine

from qaspen_migrations.inspector.base import BaseInspector
from qaspen_migrations.inspector.schema import (
    ColumnDataSchema,
    TableDumpSchema,
)

if typing.TYPE_CHECKING:
    from qaspen import BaseTable


class PostgresInspector(BaseInspector[PsycopgEngine]):
    inspect_query = """
        SELECT
            c.column_name as column_name,
            t.constraint_type as column_constraint,
            udt_name as data_type,
            is_nullable as is_null,
            column_default as database_default,
            character_maximum_length as max_length,
            numeric_precision,
            numeric_scale
        FROM
            information_schema.constraint_column_usage
        CONST JOIN
            information_schema.table_constraints t USING (
                table_catalog, table_schema, table_name, constraint_catalog, constraint_schema, constraint_name
            )
        RIGHT JOIN
            information_schema.columns c using (column_name, table_catalog, table_schema, table_name)
        WHERE
            c.table_catalog = '{}'
            and c.table_name = '{}'
            and c.table_schema = '{}';
    """

    async def inspect_database(
        self,
        models: list[type[BaseTable]],
    ) -> list[TableDumpSchema]:
        database_dump: typing.Final = []
        for model in models:
            table_dump = TableDumpSchema(model=model)

            with contextlib.suppress(UndefinedTable):
                inspect_result = await self.engine.execute(
                    self.inspect_query.format(
                        self.engine.database,
                        model.original_table_name(),
                        model._table_meta.table_schema,
                        self.inspect_query,
                    ),
                    [],
                )

                for column_info in inspect_result:
                    table_dump.add_column_data(
                        ColumnDataSchema.from_database(column_info),
                    )
            database_dump.append(table_dump)

        await self.engine.stop_connection_pool()
        return database_dump
