import typing

from qaspen_migrations.ddl import postgres
from qaspen_migrations.operations.base import BaseOperationsImplementer


class PostgresOperationsImplementer(
    BaseOperationsImplementer[
        typing.Any,
        typing.Any,
        typing.Any,
        typing.Any,
        typing.Any,
    ],
):
    create_table_ddl = postgres.CreateTable
    drop_table_ddl = postgres.DropTable
    alter_column_table_ddl = postgres.AlterColumn
    add_column_ddl = postgres.AddColumn
    drop_column_ddl = postgres.DropColumn
