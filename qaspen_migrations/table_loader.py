from __future__ import annotations
import dataclasses
import importlib
import typing

from qaspen import BaseTable

from qaspen_migrations.settings import QaspenMigrationTable


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
