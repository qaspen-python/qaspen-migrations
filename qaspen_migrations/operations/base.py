from __future__ import annotations
import abc
import dataclasses
import enum
import typing

from qaspen_migrations.ddl.base import (
    BaseAddColumnDDLElement,
    BaseAlterColumnDDLElement,
    BaseCreateTableDDLElement,
    BaseDDLElement,
    BaseDropColumnDDLElement,
    BaseDropTableDDLElement,
)
from qaspen_migrations.utils.parsing import table_column_to_column_info


if typing.TYPE_CHECKING:
    from qaspen.columns.base import Column

    from qaspen_migrations.schema import ColumnInfo


class OperationsEnum(enum.StrEnum):
    CREATE_TABLE = "self.operations.create_table"
    DROP_TABLE = "self.operations.drop_table"
    ALTER_COLUMN = "self.operations.alter_column"
    ADD_COLUMN = "self.operations.add_column"
    DROP_COLUMN = "self.operations.drop_column"


CreateTableDDLElementType = typing.TypeVar(
    "CreateTableDDLElementType",
    bound=BaseCreateTableDDLElement,
)
DropTableDDLElementType = typing.TypeVar(
    "DropTableDDLElementType",
    bound=BaseDropTableDDLElement,
)
AlterColumnDDLElementType = typing.TypeVar(
    "AlterColumnDDLElementType",
    bound=BaseAlterColumnDDLElement,
)
AddColumnDDLElementType = typing.TypeVar(
    "AddColumnDDLElementType",
    bound=BaseAddColumnDDLElement,
)
DropColumnDDLElementType = typing.TypeVar(
    "DropColumnDDLElementType",
    bound=BaseDropColumnDDLElement,
)


class BaseOperationsImplementer(
    typing.Generic[
        CreateTableDDLElementType,
        DropTableDDLElementType,
        AlterColumnDDLElementType,
        AddColumnDDLElementType,
        DropColumnDDLElementType,
    ],
):
    create_table_ddl: type[CreateTableDDLElementType] = dataclasses.field(
        init=False,
    )
    drop_table_ddl: type[DropTableDDLElementType] = dataclasses.field(
        init=False,
    )
    alter_column_table_ddl: type[
        AlterColumnDDLElementType
    ] = dataclasses.field(
        init=False,
    )
    add_column_ddl: type[AddColumnDDLElementType] = dataclasses.field(
        init=False,
    )
    drop_column_ddl: type[DropColumnDDLElementType] = dataclasses.field(
        init=False,
    )

    def create_table(
        self,
        table_name: str,
        to_add_columns: list[Column[typing.Any]],
    ) -> BaseDDLElement:
        return self.create_table_ddl(
            table_name,
            [
                table_column_to_column_info(to_add_column)
                for to_add_column in to_add_columns
            ],
        )

    def drop_table(
        self,
        table_name: str,
    ) -> BaseDDLElement:
        return self.drop_table_ddl(table_name)

    def alter_column(
        self,
        table_name: str,
        from_column: Column[typing.Any],
        to_column: Column[typing.Any],
    ) -> BaseDDLElement:
        return self.alter_column_table_ddl(
            table_name,
            table_column_to_column_info(from_column),
            table_column_to_column_info(to_column),
        )

    def add_column(
        self,
        table_name: str,
        to_add_column: Column[typing.Any],
    ) -> BaseDDLElement:
        return self.add_column_ddl(
            table_name,
            table_column_to_column_info(to_add_column),
        )

    def drop_column(self, table_name: str, column_name: str) -> BaseDDLElement:
        return self.drop_column_ddl(table_name, column_name)


class BaseOperation(abc.ABC):
    operation: OperationsEnum

    @abc.abstractmethod
    def to_migration_repr(self) -> str:
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.to_migration_repr().replace("'", "")


@dataclasses.dataclass(slots=True, frozen=True, repr=False)
class CreateTableOperation(BaseOperation):
    table_name: str
    to_add_columns_info: list[ColumnInfo]
    operation: OperationsEnum = OperationsEnum.CREATE_TABLE

    def to_migration_repr(self) -> str:
        columns_repr: typing.Final = [
            table_column_info.to_table_column_repr()
            for table_column_info in self.to_add_columns_info
        ]
        return f"""{self.operation}(
                "{self.table_name}",
                {[', '.join(columns_repr)]},
            )"""


@dataclasses.dataclass(slots=True, frozen=True, repr=False)
class DropTableOperation(BaseOperation):
    table_name: str
    operation: OperationsEnum = OperationsEnum.DROP_TABLE

    def to_migration_repr(self) -> str:
        return f"""{self.operation}("{self.table_name}")"""


@dataclasses.dataclass(slots=True, frozen=True, repr=False)
class AlterColumnOperation(BaseOperation):
    table_name: str
    from_column_info: ColumnInfo
    to_column_info: ColumnInfo
    operation: OperationsEnum = OperationsEnum.ALTER_COLUMN

    def to_migration_repr(self) -> str:
        return f"""{self.operation}(
                "{self.table_name}",
                {self.from_column_info.to_table_column_repr()},
                {self.to_column_info.to_table_column_repr()},
            )"""


@dataclasses.dataclass(slots=True, frozen=True, repr=False)
class AddColumnOperation(BaseOperation):
    table_name: str
    to_add_column: ColumnInfo
    operation: OperationsEnum = OperationsEnum.ADD_COLUMN

    def to_migration_repr(self) -> str:
        return f"""{self.operation}(
            "{self.table_name}",
            {self.to_add_column.to_table_column_repr()},
        )"""


@dataclasses.dataclass(slots=True, frozen=True, repr=False)
class DropColumnOperation(BaseOperation):
    table_name: str
    column_name: str
    operation: OperationsEnum = OperationsEnum.DROP_COLUMN

    def to_migration_repr(self) -> str:
        return f"""{self.operation}(
            "{self.table_name}",
            "{self.column_name}",
        )"""
