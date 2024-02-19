from __future__ import annotations
import dataclasses
import importlib
import typing

import toml
from qaspen import BaseTable
from qaspen.abc.db_engine import BaseEngine

from qaspen_migrations.exceptions import ConfigurationError
from qaspen_migrations.settings import (
    QASPEN_MIGRATIONS_TOML_KEY,
    QaspenMigrationsSettings,
    QaspenMigrationTable,
)


if typing.TYPE_CHECKING:
    import pathlib


T = typing.TypeVar("T")


def load_config(config_path: pathlib.Path) -> QaspenMigrationsSettings:
    if config_path.exists():
        content = config_path.read_text()
        settings: typing.Final = toml.loads(content)
    else:
        raise ConfigurationError(
            f"No config file by path {config_path.name} found",
        )
    try:
        migrations_settings: typing.Final = settings["tool"][
            QASPEN_MIGRATIONS_TOML_KEY
        ]
    except LookupError as exc:
        raise ConfigurationError(
            "No qaspen migrations settings found in config file."
            "Please, run 'init' one more time and try again.",
        ) from exc

    return QaspenMigrationsSettings(**migrations_settings)


def load_engine(
    engine_path: str,
) -> BaseEngine[typing.Any, typing.Any, typing.Any]:
    engine_path, engine_object = engine_path.split(":")
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


@dataclasses.dataclass()
class TableLoader:
    table_paths: list[str]
    tables: list[type[BaseTable]] = dataclasses.field(
        init=False,
        default_factory=list,
    )

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

    def load_tables(self) -> list[type[BaseTable]]:
        tables: typing.Final = []
        for model_path in self.table_paths:
            tables.extend(self.__load_tables_from_module(model_path))

        tables.append(QaspenMigrationTable)
        return tables
