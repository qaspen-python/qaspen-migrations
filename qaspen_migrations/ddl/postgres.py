import typing

from qaspen_migrations.ddl.base import (
    BaseAddColumnDDLElement,
    BaseAlterColumnDDLElement,
    BaseColumnDDlElement,
    BaseCreateTableDDLElement,
    BaseDropColumnDDLElement,
    BaseDropTableDDLElement,
)


class CreateTable(BaseCreateTableDDLElement):
    def to_database_expression(self) -> str:
        column_database_expression: typing.Final = ", \n".join(
            [
                Column(column_info).to_database_expression()
                for column_info in self.to_add_columns
            ],
        )
        return f"""
                CREATE TABLE {self.table_name_with_schema}
                ({column_database_expression});
            """


class DropTable(BaseDropTableDDLElement):
    def to_database_expression(self) -> str:
        return f"DROP TABLE {self.table_name_with_schema};"


class AlterColumn(BaseAlterColumnDDLElement):
    def __generate_alter_default(self) -> str:
        default_expression: typing.Final = (
            "DROP DEFAULT"
            if self.to_column_info.database_default is None
            else f"SET DEFAULT {self.to_column_info.database_default}"
        )
        return (
            f"ALTER COLUMN {self.to_column_info.db_column_name} "
            f"{default_expression}"
        )

    def __generate_alter_is_null(self) -> str:
        null_expression: typing.Final = (
            "DROP NOT NULL" if self.to_column_info.is_null else "SET NOT NULL"
        )
        return (
            f"ALTER COLUMN {self.to_column_info.db_column_name} "
            f"{null_expression}"
        )

    def __generate_alter_data_type(self) -> str:
        return (
            f"ALTER COLUMN {self.to_column_info.db_column_name} "
            f"TYPE {Column(self.to_column_info).full_sql_type}"
        )

    def to_database_expression(self) -> str:
        alter_statements: typing.Final = []

        if (
            self.to_column_info.database_default
            != self.from_column_info.database_default
        ):
            alter_statements.append(self.__generate_alter_default())

        if self.to_column_info.is_null != self.from_column_info.is_null:
            alter_statements.append(self.__generate_alter_is_null())

        if (
            self.to_column_info.main_column_type
            != self.from_column_info.main_column_type
            or self.to_column_info.inner_column_type
            != self.from_column_info.inner_column_type
        ):
            alter_statements.append(self.__generate_alter_data_type())
        return (
            f"ALTER TABLE {self.table_name_with_schema}\n"
            f"{' '.join(alter_statements)}"
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
        return self.column_info.db_column_name

    @property
    def is_null(self) -> str:
        return "NULL" if self.column_info.is_null else "NOT NULL"

    @property
    def full_sql_type(self) -> str:
        return f"{self.__sql_type}{self.__type_args}{self.__is_array}"

    @property
    def __sql_type(self) -> str:
        if self.column_info.is_array:
            assert self.column_info.inner_column_type is not None
            return self.column_info.inner_column_type._sql_type.sql_type()

        return self.column_info.main_column_type._sql_type.sql_type()

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
