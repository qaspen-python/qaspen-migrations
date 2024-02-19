from __future__ import annotations
import abc
import dataclasses
import typing


if typing.TYPE_CHECKING:
    from qaspen_migrations.schema import (
        ColumnInfo,
    )


class BaseDDLElement(abc.ABC):
    @abc.abstractmethod
    def to_database_expression(self) -> str:
        raise NotImplementedError


@dataclasses.dataclass(slots=True, frozen=True)
class BaseCreateTableDDLElement(BaseDDLElement):
    table_name_with_schema: str
    to_add_columns: list[ColumnInfo]


@dataclasses.dataclass(slots=True, frozen=True)
class BaseDropTableDDLElement(BaseDDLElement):
    table_name_with_schema: str


@dataclasses.dataclass(slots=True, frozen=True)
class BaseAlterColumnDDLElement(BaseDDLElement):
    table_name_with_schema: str
    from_column_info: ColumnInfo
    to_column_info: ColumnInfo


@dataclasses.dataclass(slots=True, frozen=True)
class BaseAddColumnDDLElement(BaseDDLElement):
    table_name_with_schema: str
    column_info: ColumnInfo


@dataclasses.dataclass(slots=True, frozen=True)
class BaseDropColumnDDLElement(BaseDDLElement):
    table_name_with_schema: str
    column_name: str


@dataclasses.dataclass(slots=True, frozen=True)
class BaseColumnDDlElement(BaseDDLElement):
    column_info: ColumnInfo
