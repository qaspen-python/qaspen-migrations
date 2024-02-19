from __future__ import annotations
import abc
import dataclasses
import typing

from qaspen.abc.db_engine import BaseEngine
from qaspen.columns.base import BaseColumn  # noqa: TCH002

from qaspen_migrations.schema import ColumnInfo, TableDump
from qaspen_migrations.utils.parsing import table_column_to_column_info


if typing.TYPE_CHECKING:
    from qaspen import BaseTable


Engine = typing.TypeVar(
    "Engine",
    bound=BaseEngine[typing.Any, typing.Any, typing.Any],
)


@dataclasses.dataclass(frozen=True)
class BaseInspector(
    abc.ABC,
    typing.Generic[Engine],
):
    engine: Engine
    tables: list[type[BaseTable]]
    inspect_info_query: str = dataclasses.field(init=False)

    @abc.abstractmethod
    def database_column_to_column_info(
        self,
        column_info: dict[str, typing.Any],
    ) -> ColumnInfo:
        raise NotImplementedError

    @abc.abstractmethod
    def build_inspect_info_query(self, table: type[BaseTable]) -> str:
        raise NotImplementedError

    async def inspect_database(
        self,
    ) -> list[TableDump]:
        database_dump: typing.Final = []
        for table in self.tables:
            table_dump = TableDump(table=table)
            inspect_result = await self.engine.execute(
                self.build_inspect_info_query(table),
                [],
            )

            for database_column_info in inspect_result:
                table_dump.add_column_info(
                    self.database_column_to_column_info(database_column_info),
                )
            database_dump.append(table_dump)

        await self.engine.stop_connection_pool()
        return database_dump

    def inspect_local_state(
        self,
    ) -> list[TableDump]:
        database_dump: typing.Final = []
        for table in self.tables:
            table_dump = TableDump(table=table)
            column: BaseColumn[typing.Any]
            for column in table.all_columns():
                table_dump.add_column_info(
                    table_column_to_column_info(column),
                )
            database_dump.append(table_dump)

        return database_dump
