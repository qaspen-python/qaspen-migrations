from __future__ import annotations
import dataclasses
import typing

from qaspen_migrations.ddl.mapping import map_ddl_generator
from qaspen_migrations.exceptions import (
    MigrationGenerationError,
)
from qaspen_migrations.inspector.mapping import map_inspector
from qaspen_migrations.migrations.writer import MigrationsWriter
from qaspen_migrations.schema.migration_changes import (
    MigrationChangesSchema,
)


if typing.TYPE_CHECKING:
    from qaspen.abc.db_engine import BaseEngine
    from qaspen.table.base_table import BaseTable

    from qaspen_migrations.schema.column_info import (
        ColumnInfoSchema,
        TableDumpSchema,
    )


@dataclasses.dataclass
class MigrationsMaker:
    engine: BaseEngine[
        typing.Any,
        typing.Any,
        typing.Any,
    ]
    migrations_path: str
    tables: list[type[BaseTable]]

    async def make_migrations(self) -> None:
        inspector: typing.Final = map_inspector(self.engine, self.tables)
        migration_changes: typing.Final = generate_migration_changes(
            inspector.inspect_local_state(),
            await inspector.inspect_database(),
        )
        ddl_generator: typing.Final = map_ddl_generator(
            self.engine,
            migration_changes,
        )

        to_apply, to_rollback = ddl_generator.generate_ddl_elements()
        migrations_writer = MigrationsWriter(
            self.migrations_path,
            self.engine.engine_type,
            to_apply,
            to_rollback,
        )

        await migrations_writer.save_migration()


def generate_migration_changes(
    dump_from_local_state: list[TableDumpSchema],
    dump_from_database: list[TableDumpSchema],
) -> list[MigrationChangesSchema]:
    migration_changes: typing.Final = []

    for table_dump_from_local_state, table_dump_from_database in zip(
        dump_from_local_state,
        dump_from_database,
    ):
        if table_dump_from_local_state.table != table_dump_from_database.table:
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
            for column_info in table_dump_from_local_state.column_info_set
            if column_info.column_name not in column_names_from_database
        }
        # Get to drop columns according to
        # name column name presence in set from local state
        to_drop_columns = {
            column_info
            for column_info in table_dump_from_database.column_info_set
            if column_info.column_name not in column_names_from_local_state
        }

        # Columns are suspected for update
        # if they are neither created nor deleted
        suspected_for_update_columns_from_local_state = (
            table_dump_from_local_state.column_info_set - to_add_columns
        )
        suspected_for_update_columns_from_database = (
            table_dump_from_database.column_info_set - to_drop_columns
        )

        to_alter_columns: set[
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
            if (
                suspected_column_from_local_state.column_name
                != suspected_column_from_database.column_name
            ):
                raise MigrationGenerationError(
                    "Column names are not equal: "
                    f"{suspected_column_from_local_state.column_name} "
                    f"!= {suspected_column_from_database.column_name}.",
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

        migration_changes.append(
            MigrationChangesSchema(
                table=table_dump_from_local_state.table,
                to_add_columns=to_add_columns,
                to_alter_columns=to_alter_columns,
                to_drop_columns=to_drop_columns,
            ),
        )

    return migration_changes
