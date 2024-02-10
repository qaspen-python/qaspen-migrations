from __future__ import annotations

import typing

from qaspen_psycopg.engine import PsycopgEngine

from qaspen_migrations.inspector.base import BaseInspector
from qaspen_migrations.inspector.schema import PostgresColumnInfoSchema

if typing.TYPE_CHECKING:
    from qaspen import BaseTable


class PostgresInspector(
    BaseInspector[PostgresColumnInfoSchema, PsycopgEngine],
):
    schema_type = PostgresColumnInfoSchema
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

    def build_inspect_info_query(self, table: type[BaseTable]) -> str:
        return self.inspect_info_query.format(
            self.engine.database,
            table.original_table_name(),
            table._table_meta.table_schema,
        )
