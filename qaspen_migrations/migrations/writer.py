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

from qaspen_migrations.exceptions import ConfigurationError
from qaspen_migrations.settings import (
    MIGRATION_CREATED_DATETIME_FORMAT,
    QASPEN_MIGRATION_TEMPLATE_NAME,
    QASPEN_MIGRATION_TEMPLATE_PATH,
)


if typing.TYPE_CHECKING:
    from qaspen_migrations.ddl.base import BaseDDLElement
    from qaspen_migrations.migrations.base import BaseMigration


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
    to_migrate_ddl_elements: list[BaseDDLElement]
    to_rollback_ddl_elements: list[BaseDDLElement]
    __migrations_loader: MigrationsOperator = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.__migrations_loader = MigrationsOperator(self.migrations_path)

    def parse_ddl_elements_to_import(self) -> list[str]:
        return []

    def parse_database_type_for_ddl(self) -> str:
        try:
            return ENGINE_TYPE_DATABASE_TYPE_FOR_DDL_MAP[self.engine_type]
        except LookupError as exception:
            raise ConfigurationError(
                f"No DDL module for {self.engine_type} engine type.\n"
                f"Valid types: "
                f"{', '.join(ENGINE_TYPE_DATABASE_TYPE_FOR_DDL_MAP.keys())}",
            ) from exception

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
                elements_to_migrate=self.to_migrate_ddl_elements,
                elements_to_rollback=self.to_rollback_ddl_elements,
                database_type=self.parse_database_type_for_ddl(),
            )
        )
        async with aiofile.async_open(
            pathlib.Path(self.migrations_path) / f"{new_migration_version}.py",
            "w",
        ) as new_migration_file:
            await new_migration_file.write(rendered_migration_template)

        return new_migration_version
