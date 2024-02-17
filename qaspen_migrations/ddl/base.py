from __future__ import annotations
import abc
import dataclasses
import typing


if typing.TYPE_CHECKING:
    from qaspen_migrations.schema.column_info import ColumnInfoSchema
    from qaspen_migrations.schema.migration_changes import (
        MigrationChangesSchema,
    )


class BaseDDLElement(abc.ABC):
    @abc.abstractmethod
    def to_database_expression(self) -> str:
        raise NotImplementedError


@dataclasses.dataclass
class BaseCreateTableDDLElement(BaseDDLElement):
    table_name_with_schema: str
    to_create_columns: list[ColumnInfoSchema]

    def __repr__(self) -> str:
        repr_to_create_columns = ",\n\t".join(
            repr(to_create_column)
            for to_create_column in self.to_create_columns
        )
        return f"""{type(self).__name__}(
            table_with_schema={self.table_name_with_schema},
            to_create_columns=[
                {repr_to_create_columns}
            ]
        )"""


@dataclasses.dataclass
class BaseDropTableDDLElement(BaseDDLElement):
    table_name_with_schema: str


@dataclasses.dataclass
class BaseAlterColumnDDLElement(BaseDDLElement):
    table_name_with_schema: str
    column_info: ColumnInfoSchema


@dataclasses.dataclass
class BaseAddColumnDDLElement(BaseDDLElement):
    table_name_with_schema: str
    column_info: ColumnInfoSchema


@dataclasses.dataclass
class BaseDropColumnDDLElement(BaseDDLElement):
    table_name_with_schema: str
    column_name: str


@dataclasses.dataclass
class BaseColumnDDlElement(BaseDDLElement):
    column_info: ColumnInfoSchema


@dataclasses.dataclass
class BaseDDLGenerator:
    migration_changes: list[MigrationChangesSchema]
    create_table_dll_element_type: type[
        BaseCreateTableDDLElement
    ] = dataclasses.field(
        init=False,
    )
    drop_table_dll_element_type: type[
        BaseDropTableDDLElement
    ] = dataclasses.field(
        init=False,
    )
    alter_column_dll_element_type: type[
        BaseAlterColumnDDLElement
    ] = dataclasses.field(
        init=False,
    )
    add_column_dll_element_type: type[
        BaseAddColumnDDLElement
    ] = dataclasses.field(
        init=False,
    )
    drop_column_dll_element_type: type[
        BaseDropColumnDDLElement
    ] = dataclasses.field(
        init=False,
    )
    column_dll_element_type: type[BaseColumnDDlElement] = dataclasses.field(
        init=False,
    )
    __to_migrate_elements: list[BaseDDLElement] = dataclasses.field(
        init=False,
        default_factory=list,
    )
    __to_rollback_elements: list[BaseDDLElement] = dataclasses.field(
        init=False,
        default_factory=list,
    )

    def __generate_create_table(
        self,
        table_name: str,
        to_create_columns: set[ColumnInfoSchema],
    ) -> None:
        self.__to_migrate_elements.append(
            self.create_table_dll_element_type(
                table_name,
                list(to_create_columns),
            ),
        )
        self.__to_rollback_elements.append(
            self.drop_table_dll_element_type(
                table_name,
            ),
        )

    def __generate_drop_table(
        self,
        table_name: str,
        to_create_columns: set[ColumnInfoSchema],
    ) -> None:
        self.__to_migrate_elements.append(
            self.drop_table_dll_element_type(
                table_name,
            ),
        )
        self.__to_rollback_elements.append(
            self.create_table_dll_element_type(
                table_name,
                list(to_create_columns),
            ),
        )

    def __generate_add_column(
        self,
        table_name: str,
        column_info: ColumnInfoSchema,
    ) -> None:
        self.__to_migrate_elements.append(
            self.add_column_dll_element_type(
                table_name,
                column_info,
            ),
        )
        self.__to_rollback_elements.append(
            self.drop_column_dll_element_type(
                table_name,
                column_info.column_name,
            ),
        )

    def __generate_drop_column(
        self,
        table_name: str,
        column_info: ColumnInfoSchema,
    ) -> None:
        self.__to_migrate_elements.append(
            self.drop_column_dll_element_type(
                table_name,
                column_info.column_name,
            ),
        )
        self.__to_rollback_elements.append(
            self.add_column_dll_element_type(
                table_name,
                column_info,
            ),
        )

    def __generate_alter_column(
        self,
        table_name: str,
        alter_from_column_info: ColumnInfoSchema,
        alter_to_column_info: ColumnInfoSchema,
    ) -> None:
        self.__to_migrate_elements.append(
            self.alter_column_dll_element_type(
                table_name,
                alter_to_column_info,
            ),
        )
        self.__to_rollback_elements.append(
            self.alter_column_dll_element_type(
                table_name,
                alter_from_column_info,
            ),
        )

    def generate_ddl_elements(
        self,
    ) -> tuple[list[BaseDDLElement], list[BaseDDLElement]]:
        for migration_change in self.migration_changes:
            if migration_change.should_skip_table:
                continue

            schemed_table_name = migration_change.table.schemed_table_name()

            if migration_change.should_create_table:
                self.__generate_create_table(
                    schemed_table_name,
                    migration_change.to_add_columns,
                )
                continue

            if migration_change.should_drop_table:
                self.__generate_drop_table(
                    schemed_table_name,
                    migration_change.to_drop_columns,
                )
                continue

            for to_create_column in migration_change.to_add_columns:
                self.__generate_add_column(
                    schemed_table_name,
                    to_create_column,
                )
            for to_delete_column in migration_change.to_drop_columns:
                self.__generate_drop_column(
                    schemed_table_name,
                    to_delete_column,
                )
            for (
                alter_from_column,
                alter_to_column,
            ) in migration_change.to_alter_columns:
                self.__generate_alter_column(
                    schemed_table_name,
                    alter_from_column,
                    alter_to_column,
                )

        return self.__to_migrate_elements, self.__to_rollback_elements
