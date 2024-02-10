from __future__ import annotations

import abc
import dataclasses
import typing

from qaspen.abc.db_engine import BaseEngine

from qaspen_migrations.exceptions import InconsistentTableError
from qaspen_migrations.inspector.schema import (
    ColumnInfoSchema,
    MigrationChangesSchema,
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

    async def __inspect_database(
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

    def __inspect_local_state(
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

    async def generate_migration_changes(
        self,
    ) -> list[MigrationChangesSchema]:
        migration_changes: typing.Final = []
        dump_from_local_state: typing.Final = self.__inspect_local_state()
        dump_from_database: typing.Final = await self.__inspect_database()

        for table_dump_from_local_state, table_dump_from_database in zip(
            dump_from_local_state,
            dump_from_database,
        ):
            if (
                table_dump_from_local_state.table
                != table_dump_from_database.table
            ):
                raise InconsistentTableError

            column_names_from_local_state = (
                table_dump_from_local_state.all_column_names()
            )
            column_names_from_database = (
                table_dump_from_database.all_column_names()
            )
            # Get to create columns according to
            # name column name presence in set from database
            to_create_columns = {
                column_info
                for column_info in table_dump_from_local_state.column_info_set
                if column_info.column_name not in column_names_from_database
            }
            # Get to delete columns according to
            # name column name presence in set from local state
            to_delete_columns = {
                column_info
                for column_info in table_dump_from_database.column_info_set
                if column_info.column_name not in column_names_from_local_state
            }

            # Columns are suspected for update
            # if they are neither created nor deleted
            suspected_for_update_columns_from_local_state = (
                table_dump_from_local_state.column_info_set - to_create_columns
            )
            suspected_for_update_columns_from_database = (
                table_dump_from_database.column_info_set - to_delete_columns
            )

            to_update_columns: set[
                tuple[ColumnInfoSchema, ColumnInfoSchema]
            ] = set()
            for (
                suspected_column_from_local_state,
                suspected_column_from_database,
            ) in zip(
                sorted(
                    suspected_for_update_columns_from_local_state,
                    key=lambda suspect_column: suspect_column.column_name,
                ),
                sorted(
                    suspected_for_update_columns_from_database,
                    key=lambda suspect_column: suspect_column.column_name,
                ),
            ):
                # If suspected column schemas are not equal -
                # column should be updated
                #
                # Here we are not looking for a name difference,
                # because all such columns are already either in
                # to_create_columns or to_delete_columns
                if (
                    suspected_column_from_local_state
                    != suspected_column_from_database
                ):
                    # Generating a tuple like (from_column, to_column)
                    to_update_columns.add(
                        (
                            suspected_column_from_database,
                            suspected_column_from_local_state,
                        ),
                    )

            migration_changes.append(
                MigrationChangesSchema(
                    table=table_dump_from_local_state.table,
                    to_create_columns=to_create_columns,
                    to_update_columns=to_update_columns,
                    to_delete_columns=to_delete_columns,
                ),
            )

        return migration_changes
