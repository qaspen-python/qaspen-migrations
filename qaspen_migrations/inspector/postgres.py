from __future__ import annotations

import typing

from qaspen_psycopg.engine import PsycopgEngine

from qaspen_migrations.inspector.base import BaseInspector
from qaspen_migrations.inspector.schema import (
    ColumnInfoSchema,
    TableDumpSchema,
)

if typing.TYPE_CHECKING:
    from qaspen import BaseTable


class PostgresInspector(BaseInspector[PsycopgEngine]):
    inspect_info_query = """
        SELECT
            c.column_name,
            c.udt_name AS data_type,
            c.is_nullable as is_null,
            c.column_default as database_default,
            c.character_maximum_length AS max_length,
            c.numeric_precision AS precision,
            c.numeric_scale AS scale
        FROM
            information_schema.columns c
        WHERE
            c.table_catalog = '{}'
            and c.table_name = '{}'
            and c.table_schema = '{}';
    """

    async def inspect_database(
        self,
        tables: list[type[BaseTable]],
    ) -> list[TableDumpSchema]:
        database_dump: typing.Final = []
        for table in tables:
            table_dump = TableDumpSchema(table=table)
            inspect_result = await self.engine.execute(
                self.inspect_info_query.format(
                    self.engine.database,
                    table.original_table_name(),
                    table._table_meta.table_schema,
                ),
                [],
            )

            for column_info in inspect_result:
                table_dump.add_column_data(
                    ColumnInfoSchema.from_database(column_info),
                )
            database_dump.append(table_dump)

        await self.engine.stop_connection_pool()
        return database_dump
