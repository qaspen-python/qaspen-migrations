from __future__ import annotations
import dataclasses
import pathlib
import typing

from qaspen import BaseTable, columns


QASPEN_MIGRATIONS_TOML_KEY: typing.Final = "qaspen-migrations"
QASPEN_MIGRATION_TEMPLATE_PATH: typing.Final = (
    pathlib.Path(__file__).parent / "migrations"
)
QASPEN_MIGRATION_TEMPLATE_NAME: typing.Final = "template.j2"
MIGRATION_CREATED_DATETIME_FORMAT: typing.Final = "%Y-%m-%d %H:%M:%S"


@dataclasses.dataclass(slots=True, frozen=True)
class QaspenMigrationsSettings:
    migrations_path: str
    engine_path: str
    tables: list[str] = dataclasses.field(default_factory=list)

    def to_dict(self) -> dict[str, typing.Any]:
        return {
            field.name: getattr(self, field.name)
            for field in dataclasses.fields(self)
        }


class QaspenMigrationTable(BaseTable):
    version = columns.VarCharColumn(max_length=32)
    created_at = columns.TimestampColumn()
