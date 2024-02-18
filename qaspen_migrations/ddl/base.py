from __future__ import annotations
import abc
import dataclasses
import typing


if typing.TYPE_CHECKING:
    from qaspen.fields.base import Field

    from qaspen_migrations.schema import (
        MigrationChangesSchema,
    )


class BaseDDLElement(abc.ABC):
    table_field: Field[typing.Any]

    @abc.abstractmethod
    def to_database_expression(self) -> str:
        raise NotImplementedError


@dataclasses.dataclass
class BaseCreateTableDDLElement(BaseDDLElement):
    table_name_with_schema: str
    to_create_fields: list[Field[typing.Any]]


@dataclasses.dataclass
class BaseDropTableDDLElement(BaseDDLElement):
    table_name_with_schema: str


@dataclasses.dataclass
class BaseAlterColumnDDLElement(BaseDDLElement):
    table_name_with_schema: str
    table_field: Field[typing.Any]


@dataclasses.dataclass
class BaseAddColumnDDLElement(BaseDDLElement):
    table_name_with_schema: str
    table_field: Field[typing.Any]


@dataclasses.dataclass
class BaseDropColumnDDLElement(BaseDDLElement):
    table_name_with_schema: str
    field_name: str


@dataclasses.dataclass
class BaseColumnDDlElement(BaseDDLElement):
    table_field: Field[typing.Any]


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
        to_create_fields: set[Field[typing.Any]],
    ) -> None:
        self.__to_migrate_elements.append(
            self.create_table_dll_element_type(
                table_name,
                list(to_create_fields),
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
        to_create_fields: set[Field[typing.Any]],
    ) -> None:
        self.__to_migrate_elements.append(
            self.drop_table_dll_element_type(
                table_name,
            ),
        )
        self.__to_rollback_elements.append(
            self.create_table_dll_element_type(
                table_name,
                list(to_create_fields),
            ),
        )

    def __generate_add_column(
        self,
        table_name: str,
        table_field: Field[typing.Any],
    ) -> None:
        self.__to_migrate_elements.append(
            self.add_column_dll_element_type(
                table_name,
                table_field,
            ),
        )
        self.__to_rollback_elements.append(
            self.drop_column_dll_element_type(
                table_name,
                table_field.field_name,
            ),
        )

    def __generate_drop_column(
        self,
        table_name: str,
        table_field: Field[typing.Any],
    ) -> None:
        self.__to_migrate_elements.append(
            self.drop_column_dll_element_type(
                table_name,
                table_field.field_name,
            ),
        )
        self.__to_rollback_elements.append(
            self.add_column_dll_element_type(
                table_name,
                table_field,
            ),
        )

    def __generate_alter_column(
        self,
        table_name: str,
        alter_from_table_field: Field[typing.Any],
        alter_to_table_field: Field[typing.Any],
    ) -> None:
        self.__to_migrate_elements.append(
            self.alter_column_dll_element_type(
                table_name,
                alter_to_table_field,
            ),
        )
        self.__to_rollback_elements.append(
            self.alter_column_dll_element_type(
                table_name,
                alter_from_table_field,
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
                    migration_change.to_add_fields,
                )
                continue

            if migration_change.should_drop_table:
                self.__generate_drop_table(
                    schemed_table_name,
                    migration_change.to_drop_fields,
                )
                continue

            for to_create_column in migration_change.to_add_fields:
                self.__generate_add_column(
                    schemed_table_name,
                    to_create_column,
                )
            for to_delete_column in migration_change.to_drop_fields:
                self.__generate_drop_column(
                    schemed_table_name,
                    to_delete_column,
                )
            for (
                alter_from_column,
                alter_to_column,
            ) in migration_change.to_alter_fields:
                self.__generate_alter_column(
                    schemed_table_name,
                    alter_from_column,
                    alter_to_column,
                )

        return self.__to_migrate_elements, self.__to_rollback_elements
