import contextlib
import typing

from psycopg.errors import UndefinedTable
from qaspen import BaseTable
from qaspen.querystring.querystring import QueryString
from qaspen_psycopg.engine import PsycopgEngine

from qaspen_migrations.inspector.base import BaseInspector


class PostgresInspector(BaseInspector[PsycopgEngine]):
    inspect_query = """
        select
        c.column_name,
        col_description(
            '{}' :: regclass, ordinal_position
        ) as column_comment,
        t.constraint_type as column_key,
        udt_name as data_type,
        is_nullable,
        column_default,
        character_maximum_length,
        numeric_precision,
        numeric_scale
        from
        information_schema.constraint_column_usage const
        join information_schema.table_constraints t using (
            table_catalog, table_schema, table_name,
            constraint_catalog, constraint_schema,
            constraint_name
        )
        right join information_schema.columns c using (
            column_name, table_catalog, table_schema,
            table_name
        )
        where
        c.table_catalog = '{}'
        and c.table_name = '{}'
        and c.table_schema = '{}';
    """

    async def inspect_database(
        self,
        models: typing.List[typing.Type[BaseTable]],
    ) -> None:
        import asyncio

        for model in models:
            await asyncio.sleep(1)
            with contextlib.suppress(UndefinedTable):
                inspect_result = await self.engine.execute(
                    QueryString(
                        model.schemed_original_table_name(),
                        self.engine.database,
                        model.original_table_name(),
                        model._table_meta.table_schema,
                        sql_template=self.inspect_query,
                    ),
                )
                print(inspect_result)
        await self.engine.stop_connection_pool()
