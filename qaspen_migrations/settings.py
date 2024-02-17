from __future__ import annotations
import pathlib
import typing

from pydantic import BaseModel
from qaspen import BaseTable, fields


QASPEN_MIGRATIONS_TOML_KEY: typing.Final = "qaspen-migrations"
QASPEN_MIGRATION_TEMPLATE_PATH: typing.Final = (
    pathlib.Path(__file__).parent / "migrations"
)
QASPEN_MIGRATION_TEMPLATE_NAME: typing.Final = "template.j2"
MIGRATION_CREATED_DATETIME_FORMAT: typing.Final = "%Y-%m-%d %H:%M:%S"


class QaspenMigrationsSettings(BaseModel):
    tables: list[str] = []
    migrations_path: str
    engine_path: str


class QaspenMigrationTable(BaseTable):
    version = fields.VarCharField(max_length=32)
    created_at = fields.TimestampField()
