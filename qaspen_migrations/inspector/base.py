from __future__ import annotations
import abc
import dataclasses
import typing

from qaspen import BaseTable, fields
from qaspen.abc.db_engine import BaseEngine
from qaspen.fields.base import BaseField, Field  # noqa: TCH002

from qaspen_migrations.schema import (
    TableDumpSchema,
)


Engine = typing.TypeVar(
    "Engine",
    bound=BaseEngine[typing.Any, typing.Any, typing.Any],
)


@dataclasses.dataclass
class BaseInspector(
    abc.ABC,
    typing.Generic[Engine],
):
    engine: Engine
    tables: list[type[BaseTable]]
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

            for database_field_info in inspect_result:
                table_dump.add_column_data(
                    self.database_info_to_field(database_field_info),
                )
            database_dump.append(table_dump)

        await self.engine.stop_connection_pool()
        return database_dump

    @abc.abstractmethod
    def database_info_to_field(
        self,
        field_info: dict[str, typing.Any],
    ) -> Field[typing.Any]:
        raise NotImplementedError

    @abc.abstractmethod
    def build_inspect_info_query(self, table: type[BaseTable]) -> str:
        raise NotImplementedError

    def inspect_local_state(
        self,
    ) -> list[TableDumpSchema]:
        database_dump: typing.Final = []
        for table in self.tables:
            table_dump = TableDumpSchema(table=table)
            field: BaseField[typing.Any]
            for field in table.all_fields():
                table_dump.add_column_data(field)
            database_dump.append(table_dump)

        return database_dump

    def __map_field_type(
        self,
        table_field_type: type[Field[typing.Any]],
        column_name: str | None,
        database_default: str | None,
        is_null: bool,
        precision: int | None,
        scale: int | None,
        max_length: int | None,
        is_array: bool,
    ) -> Field[typing.Any]:
        if is_array:
            return fields.ArrayField(
                inner_field=self.__map_field_type(
                    table_field_type=table_field_type,
                    column_name=None,
                    database_default=None,
                    is_null=is_null,
                    precision=precision,
                    scale=scale,
                    max_length=max_length,
                    is_array=False,
                ),
            )

        if issubclass(
            table_field_type,
            (fields.DecimalField, fields.NumericField),
        ):
            return table_field_type(
                db_field_name=column_name,
                database_default=database_default,
                precision=precision,
                scale=scale,
                is_null=is_null,
            )

        if issubclass(
            table_field_type,
            (fields.VarCharField),
        ):
            return table_field_type(
                is_null=is_null,
                db_field_name=column_name,
                database_default=database_default,
                max_length=max_length or 255,
            )

        return table_field_type(
            is_null=is_null,
            db_field_name=column_name,
            database_default=database_default,
        )
