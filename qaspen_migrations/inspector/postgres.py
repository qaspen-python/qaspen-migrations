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
            ic.column_name,
            ic.udt_name AS sql_type,
            ic.is_nullable AS is_null,
            ic.column_default AS database_default,
            ic.character_maximum_length AS max_length,
            ic.numeric_precision AS precision,
            ic.numeric_scale AS scale,
            CASE
                WHEN att.atttypid = ANY (ARRAY[1002, 1015])
                    AND att.atttypmod > 0 THEN att.atttypmod - 4
                ELSE NULL
            END AS array_elements_length
        FROM
            information_schema.columns ic
        JOIN
            pg_catalog.pg_namespace nsp ON nsp.nspname = ic.table_schema
        JOIN
            pg_catalog.pg_class cls ON cls.relname = ic.table_name
                AND cls.relnamespace = nsp.oid
        JOIN
            pg_catalog.pg_attribute att ON att.attrelid = cls.oid
                AND att.attname = ic.column_name
        WHERE
            ic.table_catalog = '{}'
            AND ic.table_name = '{}'
            AND ic.table_schema = '{}'
            AND att.attnum > 0
            AND NOT att.attisdropped;
    """

    def build_inspect_info_query(self, table: type[BaseTable]) -> str:
        return self.inspect_info_query.format(
            self.engine.database,
            table.original_table_name(),
            table._table_meta.table_schema,
        )
