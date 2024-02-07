from __future__ import annotations

import dataclasses
import importlib
import typing

from qaspen import BaseTable
from qaspen.abc.db_engine import BaseEngine

from qaspen_migrations.exceptions import ConfigurationError
from qaspen_migrations.inspector.mapping import map_inspector
from qaspen_migrations.tables import QaspenMigrationTable

if typing.TYPE_CHECKING:
    from qaspen_migrations.inspector.base import BaseInspector


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
    __inspector: BaseInspector[
        BaseEngine[typing.Any, typing.Any, typing.Any]
    ] = dataclasses.field(init=False)
    __engine: BaseEngine[
        typing.Any,
        typing.Any,
        typing.Any,
    ] = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.__engine = self.__load_engine()
        self.__inspector = map_inspector(self.__engine)

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

    async def make_migrations(self) -> None:
        database_dump = await self.__inspector.inspect_database(
            self.table_manager.tables,
        )
        print(database_dump)  # noqa: T201
