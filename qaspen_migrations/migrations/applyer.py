from __future__ import annotations
import dataclasses
import typing

from qaspen_migrations.settings import QaspenMigrationTable


if typing.TYPE_CHECKING:
    from qaspen.abc.db_engine import BaseEngine
    from qaspen.abc.db_transaction import BaseTransaction

    from qaspen_migrations.migrations.base import BaseMigration
    from qaspen_migrations.migrations.versioner import MigrationsVersioner


@dataclasses.dataclass
class MigrationsApplyer:
    engine: BaseEngine[
        typing.Any,
        typing.Any,
        typing.Any,
    ]
    migrations_versioner: MigrationsVersioner

    @staticmethod
    async def bump_version_in_database(version_to_bump: str) -> None:
        version_from_database = await QaspenMigrationTable.select()

        if version_from_database is None or not version_from_database.result():
            await QaspenMigrationTable.insert(
                [QaspenMigrationTable.version],
                ([version_to_bump],),
            )
        else:
            await QaspenMigrationTable.update(
                {QaspenMigrationTable.version: version_to_bump},
            ).force()

    @staticmethod
    async def apply_migration(
        transaction: BaseTransaction[typing.Any, typing.Any],
        migration: BaseMigration,
    ) -> None:
        for migration_ddl_element in migration.migrate():
            await transaction.execute(
                migration_ddl_element.to_database_expression(),
                [],
                fetch_results=False,
            )

    async def apply_changes(self) -> None:
        migrations_to_apply: typing.Final = (
            await self.migrations_versioner.get_not_applyed_migrations()
        )

        if not migrations_to_apply:
            return

        async with self.engine.transaction() as transaction:
            for migration_to_apply in migrations_to_apply:
                await self.apply_migration(transaction, migration_to_apply)

            await self.bump_version_in_database(migration_to_apply.version)
