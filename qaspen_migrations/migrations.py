from __future__ import annotations
import dataclasses
import importlib
import typing

from qaspen import BaseTable
from qaspen.abc.db_engine import BaseEngine

from qaspen_migrations.ddl.mapping import map_ddl_generator
from qaspen_migrations.exceptions import (
    ConfigurationError,
    MigrationGenerationError,
)
from qaspen_migrations.inspector.mapping import map_inspector
from qaspen_migrations.schema.migration import (
    MigrationChangesSchema,
)
from qaspen_migrations.tables import QaspenMigrationTable


if typing.TYPE_CHECKING:
    from qaspen_migrations.schema.column_info import (
        ColumnInfoSchema,
        TableDumpSchema,
    )


@dataclasses.dataclass()
class TableManager:
    table_paths: list[str]
    __tables: list[type[BaseTable]] = dataclasses.field(
        init=False,
        default_factory=list,
    )

    def __post_init__(self) -> None:
        self.__tables = self.__load_tables()

    def __load_tables_from_module(
        self,
        tables_file_path: str,
    ) -> list[type[BaseTable]]:
        module_tables: typing.Final[list[type[BaseTable]]] = []
        tables_module: typing.Final = importlib.import_module(tables_file_path)
        for module_member in dir(tables_module):
            tables_module_attribute = getattr(
                tables_module,
                module_member,
            )
            try:
                if (
                    issubclass(
                        tables_module_attribute,
                        BaseTable,
                    )
                    and not tables_module_attribute._table_meta.abstract
                ):
                    module_tables.append(tables_module_attribute)
            except TypeError:
                continue

        return module_tables

    def __load_tables(self) -> list[type[BaseTable]]:
        tables: typing.Final = []
        for model_path in self.table_paths:
            tables.extend(self.__load_tables_from_module(model_path))

        tables.append(QaspenMigrationTable)
        return tables

    @property
    def tables(self) -> list[type[BaseTable]]:
        return self.__tables


@dataclasses.dataclass
class MigrationsManager:
    engine_path: str
    migrations_path: str
    table_manager: TableManager
    __engine: BaseEngine[
        typing.Any,
        typing.Any,
        typing.Any,
    ] = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.__engine = self.__load_engine()

    def __load_engine(
        self,
    ) -> BaseEngine[typing.Any, typing.Any, typing.Any]:
        engine_path, engine_object = self.engine_path.split(":")
        engine_module: typing.Final = importlib.import_module(engine_path)

        try:
            engine: typing.Final = getattr(engine_module, engine_object)
        except AttributeError as exc:
            raise ConfigurationError("No engine object found.") from exc
        if not issubclass(type(engine), BaseEngine):
            raise ConfigurationError("No engine object found.")

        return typing.cast(
            BaseEngine[typing.Any, typing.Any, typing.Any],
            engine,
        )

    async def __generate_migration_changes(
        self,
        dump_from_local_state: list[TableDumpSchema],
        dump_from_database: list[TableDumpSchema],
    ) -> list[MigrationChangesSchema]:
        migration_changes: typing.Final = []

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

    async def make_migrations(self) -> None:
        inspector: typing.Final = map_inspector(
            self.__engine,
            self.table_manager.tables,
        )
        migration_changes: typing.Final = (
            await self.__generate_migration_changes(
                inspector.inspect_local_state(),
                await inspector.inspect_database(),
            )
        )
        ddl_generator: typing.Final = map_ddl_generator(
            self.__engine,
            migration_changes,
        )

        to_apply, to_rollback = ddl_generator.generate_ddl_elements()
