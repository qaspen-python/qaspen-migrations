from __future__ import annotations
import dataclasses
import typing

from qaspen_migrations.exceptions import (
    MigrationGenerationError,
)
from qaspen_migrations.inspector.mapping import map_inspector
from qaspen_migrations.migrations.versioner import MigrationsVersioner
from qaspen_migrations.migrations.writer import MigrationsWriter
from qaspen_migrations.operations.generator import OperationGenerator
from qaspen_migrations.schema import (
    ColumnInfo,
    TableDiff,
    TableDump,
)
from qaspen_migrations.utils.loaders import MigrationLoader


if typing.TYPE_CHECKING:
    from qaspen.abc.db_engine import BaseEngine
    from qaspen.table.base_table import BaseTable


@dataclasses.dataclass(slots=True, frozen=True)
class MigrationMaker:
    engine: BaseEngine[
        typing.Any,
        typing.Any,
        typing.Any,
    ]
    migrations_path: str
    tables: list[type[BaseTable]]

    async def make_migrations(self) -> None:
        migrations_versioner: typing.Final = MigrationsVersioner(
            MigrationLoader(self.engine.engine_type, self.migrations_path),
        )

        await migrations_versioner.is_version_in_database_up_to_date()
        inspector: typing.Final = map_inspector(self.engine, self.tables)
        table_diff: typing.Final = self.__generate_tables_diff(
            inspector.inspect_local_state(),
            await inspector.inspect_database(),
        )
        operations_generator: typing.Final = OperationGenerator(table_diff)
        to_migrate, to_rollback = operations_generator.generate_operations()

        migrations_writer = MigrationsWriter(
            migrations_versioner,
            to_migrate,
            to_rollback,
        )

        await migrations_writer.save_migration()

    @staticmethod
    def __generate_tables_diff(
        dump_from_local_state: list[TableDump],
        dump_from_database: list[TableDump],
    ) -> list[TableDiff]:
        table_diff: typing.Final = []

        for table_dump_from_local_state, table_dump_from_database in zip(
            dump_from_local_state,
            dump_from_database,
        ):
            if (
                table_dump_from_local_state.table
                != table_dump_from_database.table
            ):
                raise MigrationGenerationError(
                    "Local table name doesn't match "
                    "table name from database: "
                    f"{table_dump_from_local_state.table} "
                    f"!= {table_dump_from_database.table}.",
                )

            column_names_from_local_state = (
                table_dump_from_local_state.all_column_names()
            )
            column_names_from_database = (
                table_dump_from_database.all_column_names()
            )
            # Get to add columns according to
            # name column name presence in set from database
            to_add_columns = {
                column_info
                for column_info in table_dump_from_local_state.table_columns
                if column_info.db_column_name not in column_names_from_database
            }
            # Get to drop columns according to
            # name column name presence in set from local state
            to_drop_columns = {
                column_info
                for column_info in table_dump_from_database.table_columns
                if column_info.db_column_name
                not in column_names_from_local_state
            }

            # Columns are suspected for update
            # if they are neither created nor deleted
            suspected_for_update_columns_from_local_state = (
                table_dump_from_local_state.table_columns - to_add_columns
            )
            suspected_for_update_columns_from_database = (
                table_dump_from_database.table_columns - to_drop_columns
            )

            to_alter_columns: set[tuple[ColumnInfo, ColumnInfo]] = set()
            for (
                suspected_column_from_local_state,
                suspected_column_from_database,
            ) in zip(
                sorted(
                    suspected_for_update_columns_from_local_state,
                    key=lambda suspect_column: suspect_column.db_column_name,
                ),
                sorted(
                    suspected_for_update_columns_from_database,
                    key=lambda suspect_column: suspect_column.db_column_name,
                ),
            ):
                if (
                    suspected_column_from_local_state.db_column_name
                    != suspected_column_from_database.db_column_name
                ):
                    raise MigrationGenerationError(
                        "column names are not equal: "
                        f"{suspected_column_from_local_state.db_column_name} "
                        f"!= {suspected_column_from_database.db_column_name}.",
                    )
                # If suspected column schemas are not equal -
                # column should be updated
                #
                # Here we are not looking for a name difference,
                # because all such columns are already either in
                # to_add_columns or to_drop_columns
                if (
                    suspected_column_from_local_state
                    != suspected_column_from_database
                ):
                    # Generating a tuple like (from_column, to_column)
                    to_alter_columns.add(
                        (
                            suspected_column_from_database,
                            suspected_column_from_local_state,
                        ),
                    )

            table_diff.append(
                TableDiff(
                    table=table_dump_from_local_state.table,
                    to_add_columns=to_add_columns,
                    to_alter_columns=to_alter_columns,
                    to_drop_columns=to_drop_columns,
                ),
            )

        return table_diff
