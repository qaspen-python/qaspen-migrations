from __future__ import annotations
import dataclasses
import datetime
import typing

import pytz

from qaspen_migrations.exceptions import (
    MigrationCorruptionError,
    MigrationVersionError,
)
from qaspen_migrations.settings import (
    MIGRATION_CREATED_DATETIME_FORMAT,
    QaspenMigrationTable,
)


if typing.TYPE_CHECKING:
    from qaspen_migrations.migrations.base import BaseMigration
    from qaspen_migrations.utils.loaders import MigrationLoader


@dataclasses.dataclass
class MigrationsVersioner:
    migrations_loader: MigrationLoader
    __sorted_migrations: list[BaseMigration] = dataclasses.field(
        init=False,
        default_factory=list,
    )

    def __post_init__(self) -> None:
        self.__sorted_migrations = self.__sort_migrations_by_created_at(
            self.migrations_loader.load_migrations(),
        )

    @staticmethod
    def __sort_migrations_by_created_at(
        migrations_to_sort: list[BaseMigration],
    ) -> list[BaseMigration]:
        try:
            return sorted(
                migrations_to_sort,
                key=lambda migration: datetime.datetime.strptime(
                    migration.created_datetime,
                    MIGRATION_CREATED_DATETIME_FORMAT,
                ).replace(tzinfo=pytz.UTC),
            )
        except AttributeError as exception:
            raise MigrationCorruptionError(
                "Cannot parse created at migration attribute.",
            ) from exception

    def get_latest_local_migration_version(self) -> str | None:
        if not self.__sorted_migrations:
            return None

        return self.__sorted_migrations[-1].version

    async def is_version_in_database_up_to_date(self) -> None:
        version_in_database: typing.Final = (
            await self.fetch_current_migration_version_in_database()
        )
        version_locally: typing.Final = (
            self.get_latest_local_migration_version()
        )
        if version_locally != version_in_database:
            raise MigrationVersionError(
                "Database is not up to date, please, run 'migrate' command",
            )

    @staticmethod
    async def fetch_current_migration_version_in_database() -> str | None:
        try:
            migration_version_result = await QaspenMigrationTable.select()
        except Exception:  # noqa: BLE001
            return None
        else:
            if migration_version_result is None:
                return None
            latest_migration_version: typing.Final = (
                migration_version_result.result()
            )
            if len(latest_migration_version) == 0:
                return None

            corruption_error_message: typing.Final = (
                "Database version is corrupted. "
                "Consider dropping version table and "
                "applying all migrations once again."
            )
            if len(latest_migration_version) > 1:
                raise MigrationVersionError(
                    corruption_error_message,
                )
            if latest_migration_version[0].get("version") is None:
                raise MigrationVersionError(
                    corruption_error_message,
                )
            return latest_migration_version[0].get("version")

    async def get_not_applyed_migrations(self) -> list[BaseMigration]:
        version_in_database: typing.Final = (
            await self.fetch_current_migration_version_in_database()
        )

        if version_in_database is None:
            return self.__sorted_migrations

        for migration_idx, migration in enumerate(self.__sorted_migrations):
            if migration.version == version_in_database:
                return self.__sorted_migrations[migration_idx + 1 :]

        raise MigrationVersionError(
            "Version from database is missing in existing migrations.",
        )
