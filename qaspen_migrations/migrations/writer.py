"""Class of a template filler."""
from __future__ import annotations
import dataclasses
import datetime
import pathlib
import textwrap
import typing
import uuid

import aiofile
import pytz
from jinja2 import Environment, FileSystemLoader

from qaspen_migrations.settings import (
    MIGRATION_CREATED_DATETIME_FORMAT,
    QASPEN_MIGRATION_TEMPLATE_NAME,
    QASPEN_MIGRATION_TEMPLATE_PATH,
)


if typing.TYPE_CHECKING:
    from qaspen_migrations.migrations.base import BaseMigration
    from qaspen_migrations.operations.base import BaseOperation


jinja_environment: typing.Final = Environment(
    loader=FileSystemLoader(QASPEN_MIGRATION_TEMPLATE_PATH),
    autoescape=True,
    enable_async=True,
)

ENGINE_TYPE_DATABASE_TYPE_FOR_DDL_MAP = {"PSQLPsycopg": "postgres"}


def dedent_filter(input_text: str) -> str:
    return textwrap.dedent(input_text)


jinja_environment.filters["dedent"] = dedent_filter


@dataclasses.dataclass
class MigrationsOperator:
    migrations_path: str
    migrations: list[BaseMigration] = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.migrations = self.__load_migrations()

    def __load_migrations(self) -> list[BaseMigration]:
        return []

    def get_latest_migration_version(self) -> str | None:
        return None


@dataclasses.dataclass
class MigrationsWriter:
    migrations_path: str
    engine_type: str
    to_migrate_operations: list[BaseOperation]
    to_rollback_operations: list[BaseOperation]
    __migrations_loader: MigrationsOperator = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.__migrations_loader = MigrationsOperator(self.migrations_path)

    def parse_ddl_elements_to_import(self) -> list[str]:
        return []

    async def save_migration(self) -> str:
        new_migration_version: typing.Final = uuid.uuid4().hex[:10]
        new_migration_created_datetime: typing.Final = datetime.datetime.now(
            tz=pytz.UTC,
        ).strftime(MIGRATION_CREATED_DATETIME_FORMAT)
        previous_migration_version: typing.Final = (
            self.__migrations_loader.get_latest_migration_version()
        )

        migration_template: typing.Final = jinja_environment.get_template(
            QASPEN_MIGRATION_TEMPLATE_NAME,
        )
        rendered_migration_template: typing.Final = (
            await migration_template.render_async(
                version=new_migration_version,
                created_datetime=new_migration_created_datetime,
                previous_version=previous_migration_version,
                ddl_elemets_to_import=self.parse_ddl_elements_to_import(),
                elements_to_migrate=self.to_migrate_operations,
                elements_to_rollback=self.to_rollback_operations,
            )
        )
        async with aiofile.async_open(
            pathlib.Path(self.migrations_path) / f"{new_migration_version}.py",
            "w",
        ) as new_migration_file:
            await new_migration_file.write(rendered_migration_template)

        return new_migration_version
