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
    from qaspen_migrations.migrations.versioner import MigrationsVersioner
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
class MigrationsWriter:
    migrations_versioner: MigrationsVersioner
    to_migrate_operations: list[BaseOperation]
    to_rollback_operations: list[BaseOperation]

    def generate_migration_name(
        self,
        created_datetime: str,
        version: str,
    ) -> str:
        return f"{created_datetime}_{version}.py"

    async def save_migration(self) -> str:
        new_migration_version: typing.Final = uuid.uuid4().hex[:10]
        new_migration_created_datetime: typing.Final = datetime.datetime.now(
            tz=pytz.UTC,
        ).strftime(MIGRATION_CREATED_DATETIME_FORMAT)
        previous_migration_version: typing.Final = (
            self.migrations_versioner.get_latest_local_migration_version()
        )

        migration_template: typing.Final = jinja_environment.get_template(
            QASPEN_MIGRATION_TEMPLATE_NAME,
        )
        rendered_migration_template: typing.Final = (
            await migration_template.render_async(
                version=new_migration_version,
                created_datetime=new_migration_created_datetime,
                previous_version=previous_migration_version,
                elements_to_migrate=self.to_migrate_operations,
                elements_to_rollback=self.to_rollback_operations,
            )
        )

        async with aiofile.async_open(
            pathlib.Path(
                self.migrations_versioner.migrations_loader.migrations_path,
            )
            / self.generate_migration_name(
                new_migration_created_datetime,
                new_migration_version,
            ),
            "w",
        ) as new_migration_file:
            await new_migration_file.write(rendered_migration_template)

        return new_migration_version
