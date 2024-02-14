from __future__ import annotations

import abc
import dataclasses
import typing

from qaspen.abc.db_engine import BaseEngine

from qaspen_migrations.inspector.schema import (
    ColumnInfoSchema,
    TableDumpSchema,
)

if typing.TYPE_CHECKING:
    from qaspen import BaseTable
    from qaspen.fields.base import Field

Engine = typing.TypeVar(
    "Engine",
    bound=BaseEngine[typing.Any, typing.Any, typing.Any],
)
ColumnInfoSchemaType = typing.TypeVar(
    "ColumnInfoSchemaType",
    bound=ColumnInfoSchema,
)


@dataclasses.dataclass
class BaseInspector(
    abc.ABC,
    typing.Generic[ColumnInfoSchemaType, Engine],
):
    engine: Engine
    tables: list[type[BaseTable]]
    schema_type: type[ColumnInfoSchemaType] = dataclasses.field(init=False)
    inspect_info_query: str = dataclasses.field(init=False)

    async def inspect_database(
        self,
    ) -> list[TableDumpSchema]:
        database_dump: typing.Final = []
        for table in self.tables:
            table_dump = TableDumpSchema(table=table)
            inspect_result = await self.engine.execute(
                self.build_inspect_info_query(table),
                [],
            )

            for column_info in inspect_result:
                table_dump.add_column_data(
                    self.schema_type.from_database(column_info),
                )
            database_dump.append(table_dump)

        await self.engine.stop_connection_pool()
        return database_dump

    @abc.abstractmethod
    def build_inspect_info_query(self, table: type[BaseTable]) -> str:
        raise NotImplementedError

    def inspect_local_state(
        self,
    ) -> list[TableDumpSchema]:
        database_dump: typing.Final = []
        for table in self.tables:
            table_dump = TableDumpSchema(table=table)
            field: Field[typing.Any]
            for field in table.all_fields():
                table_dump.add_column_data(
                    ColumnInfoSchema.from_field(field),
                )
            database_dump.append(table_dump)

        return database_dump
