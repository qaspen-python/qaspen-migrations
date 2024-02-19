from __future__ import annotations
import dataclasses
from typing import TYPE_CHECKING

from qaspen_migrations.operations.base import (
    AddColumnOperation,
    AlterColumnOperation,
    BaseOperation,
    CreateTableOperation,
    DropColumnOperation,
    DropTableOperation,
)


if TYPE_CHECKING:
    from qaspen_migrations.schema import ColumnInfo, TableDiff


@dataclasses.dataclass(slots=True, frozen=True)
class OperationGenerator:
    tables_diff: list[TableDiff]
    __to_migrate_elements: list[BaseOperation] = dataclasses.field(
        init=False,
        default_factory=list,
    )
    __to_rollback_elements: list[BaseOperation] = dataclasses.field(
        init=False,
        default_factory=list,
    )

    def __generate_create_table(
        self,
        table_name: str,
        to_add_columns: set[ColumnInfo],
    ) -> None:
        self.__to_migrate_elements.append(
            CreateTableOperation(
                table_name,
                list(to_add_columns),
            ),
        )
        self.__to_rollback_elements.append(
            DropTableOperation(table_name),
        )

    def __generate_drop_table(
        self,
        table_name: str,
        to_add_columns: set[ColumnInfo],
    ) -> None:
        self.__to_migrate_elements.append(
            DropTableOperation(
                table_name,
            ),
        )
        self.__to_rollback_elements.append(
            CreateTableOperation(
                table_name,
                list(to_add_columns),
            ),
        )

    def __generate_add_column(
        self,
        table_name: str,
        column_info: ColumnInfo,
    ) -> None:
        self.__to_migrate_elements.append(
            AddColumnOperation(
                table_name,
                column_info,
            ),
        )
        self.__to_rollback_elements.append(
            DropColumnOperation(
                table_name,
                column_info.db_column_name,
            ),
        )

    def __generate_drop_column(
        self,
        table_name: str,
        column_info: ColumnInfo,
    ) -> None:
        self.__to_migrate_elements.append(
            DropColumnOperation(
                table_name,
                column_info.db_column_name,
            ),
        )
        self.__to_rollback_elements.append(
            AddColumnOperation(
                table_name,
                column_info,
            ),
        )

    def __generate_alter_column(
        self,
        table_name: str,
        alter_from_column_info: ColumnInfo,
        alter_to_column_info: ColumnInfo,
    ) -> None:
        self.__to_migrate_elements.append(
            AlterColumnOperation(
                table_name,
                alter_from_column_info,
                alter_to_column_info,
            ),
        )
        self.__to_rollback_elements.append(
            AlterColumnOperation(
                table_name,
                alter_from_column_info,
                alter_to_column_info,
            ),
        )

    def generate_operations(
        self,
    ) -> tuple[list[BaseOperation], list[BaseOperation]]:
        for table_diff in self.tables_diff:
            if table_diff.should_skip_table:
                continue

            schemed_table_name = table_diff.table.schemed_table_name()

            if table_diff.should_create_table:
                self.__generate_create_table(
                    schemed_table_name,
                    table_diff.to_add_columns,
                )
                continue

            if table_diff.should_drop_table:
                self.__generate_drop_table(
                    schemed_table_name,
                    table_diff.to_drop_columns,
                )
                continue

            for to_create_column in table_diff.to_add_columns:
                self.__generate_add_column(
                    schemed_table_name,
                    to_create_column,
                )
            for to_delete_column in table_diff.to_drop_columns:
                self.__generate_drop_column(
                    schemed_table_name,
                    to_delete_column,
                )
            for (
                alter_from_column,
                alter_to_column,
            ) in table_diff.to_alter_columns:
                self.__generate_alter_column(
                    schemed_table_name,
                    alter_from_column,
                    alter_to_column,
                )

        return self.__to_migrate_elements, self.__to_rollback_elements
