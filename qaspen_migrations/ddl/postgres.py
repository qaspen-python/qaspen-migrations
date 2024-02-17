import typing

from qaspen_migrations.ddl.base import (
    BaseAddColumnDDLElement,
    BaseAlterColumnDDLElement,
    BaseColumnDDlElement,
    BaseCreateTableDDLElement,
    BaseDDLGenerator,
    BaseDropColumnDDLElement,
    BaseDropTableDDLElement,
)


class CreateTable(BaseCreateTableDDLElement):
    def to_database_expression(self) -> str:
        table_columns_expressions: typing.Final = ", \n\t".join(
            [
                Column(column_info).to_database_expression()
                for column_info in self.to_create_columns
            ],
        )
        return (
            f"CREATE TABLE {self.table_name_with_schema}"
            f"(\n\t{table_columns_expressions}\n);"
        )


class DropTable(BaseDropTableDDLElement):
    def to_database_expression(self) -> str:
        return f"DROP TABLE {self.table_name_with_schema};"


class AlterColumn(BaseAlterColumnDDLElement):
    @property
    def alter_default(self) -> str:
        return (
            "DROP DEFAULT"
            if self.column_info.database_default is None
            else f"SET DEFAULT {self.column_info.database_default}"
        )

    @property
    def alter_is_null(self) -> str:
        return "DROP NOT NULL" if self.column_info.is_null else "SET NOT NULL"

    @property
    def alter_data_type(self) -> str:
        return f"TYPE {Column(self.column_info).full_sql_type}"

    def to_database_expression(self) -> str:
        return (
            f"ALTER TABLE {self.table_name_with_schema}\n"
            f"\tALTER COLUMN {self.column_info.column_name} "
            f"{self.alter_default},\n"
            f"\tALTER COLUMN {self.column_info.column_name} "
            f"{self.alter_is_null},\n"
            f"\tALTER COLUMN {self.column_info.column_name} "
            f"{self.alter_data_type};"
        )


class AddColumn(BaseAddColumnDDLElement):
    def to_database_expression(self) -> str:
        return (
            f"ALTER TABLE {self.table_name_with_schema}\n\t"
            f"ADD COLUMN {Column(self.column_info).to_database_expression()};"
        )


class DropColumn(BaseDropColumnDDLElement):
    def to_database_expression(self) -> str:
        return (
            f"ALTER TABLE {self.table_name_with_schema}"
            f"DROP COLUMN {self.column_name};"
        )


class Column(BaseColumnDDlElement):
    @property
    def column_name(self) -> str:
        return self.column_info.column_name

    @property
    def is_null(self) -> str:
        return "NULL" if self.column_info.is_null else "NOT NULL"

    @property
    def full_sql_type(self) -> str:
        return f"{self.__sql_type}{self.__type_args}{self.__is_array}"

    @property
    def __sql_type(self) -> str:
        return self.column_info.sql_type.sql_type()

    @property
    def __is_array(self) -> str:
        return "[]" if self.column_info.is_array else ""

    @property
    def database_default(self) -> str:
        return (
            f"DEFAULT {self.column_info.database_default}"
            if self.column_info.database_default
            else ""
        )

    @property
    def __type_args(self) -> str:
        resulting_args = []
        if self.column_info.max_length is not None:
            resulting_args.append(str(self.column_info.max_length))

        if self.column_info.scale is not None:
            resulting_args.append(str(self.column_info.scale))

        if self.column_info.precision is not None:
            resulting_args.append(str(self.column_info.precision))

        return f"({', '.join(resulting_args)})" if resulting_args else ""

    def to_database_expression(self) -> str:
        return (
            f"{self.column_name} "
            f"{self.full_sql_type} "
            f"{self.is_null} "
            f"{self.database_default}"
        ).strip(" ")


class PostgresDDLGenerator(BaseDDLGenerator):
    create_table_dll_element_type = CreateTable
    drop_table_dll_element_type = DropTable
    alter_column_dll_element_type = AlterColumn
    add_column_dll_element_type = AddColumn
    drop_column_dll_element_type = DropColumn
    column_dll_element_type = Column
