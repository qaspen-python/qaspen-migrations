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
                Column(table_field).to_database_expression()
                for table_field in self.to_create_fields
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
            if self.table_field.database_default is None
            else f"SET DEFAULT {self.table_field.database_default}"
        )

    @property
    def alter_is_null(self) -> str:
        return "DROP NOT NULL" if self.table_field.is_null else "SET NOT NULL"

    @property
    def alter_data_type(self) -> str:
        return f"TYPE {Column(self.table_field).sql_type}"

    def to_database_expression(self) -> str:
        return (
            f"ALTER TABLE {self.table_name_with_schema}\n"
            f"\tALTER COLUMN {self.table_field.field_name} "
            f"{self.alter_default},\n"
            f"\tALTER COLUMN {self.table_field.field_name} "
            f"{self.alter_is_null},\n"
            f"\tALTER COLUMN {self.table_field.field_name} "
            f"{self.alter_data_type};"
        )


class AddColumn(BaseAddColumnDDLElement):
    def to_database_expression(self) -> str:
        return (
            f"ALTER TABLE {self.table_name_with_schema}\n\t"
            f"ADD COLUMN {Column(self.table_field).to_database_expression()};"
        )


class DropColumn(BaseDropColumnDDLElement):
    def to_database_expression(self) -> str:
        return (
            f"ALTER TABLE {self.table_name_with_schema}"
            f"DROP COLUMN {self.field_name};"
        )


class Column(BaseColumnDDlElement):
    @property
    def field_name(self) -> str:
        return self.table_field._field_data.field_name

    @property
    def is_null(self) -> str:
        return "NULL" if self.table_field.is_null else "NOT NULL"

    @property
    def sql_type(self) -> str:
        return self.table_field._field_type

    @property
    def database_default(self) -> str:
        return (
            f"DEFAULT {self.table_field.database_default}"
            if self.table_field.database_default
            else ""
        )

    @property
    def __type_args(self) -> str:
        resulting_args = []
        if hasattr(self.table_field, "max_length"):
            resulting_args.append(str(self.table_field.max_length))

        if hasattr(self.table_field, "scale"):
            resulting_args.append(str(self.table_field.scale))

        if hasattr(self.table_field, "precision"):
            resulting_args.append(str(self.table_field.precision))

        return f"({', '.join(resulting_args)})" if resulting_args else ""

    def to_database_expression(self) -> str:
        return (
            f"{self.field_name} "
            f"{self.sql_type} "
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
