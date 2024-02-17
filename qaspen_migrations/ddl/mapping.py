from __future__ import annotations
import typing

from qaspen_migrations.ddl.base import BaseDDLGenerator
from qaspen_migrations.ddl.postgres import PostgresDDLGenerator
from qaspen_migrations.exceptions import ConfigurationError
from qaspen_migrations.schema.migration_changes import MigrationChangesSchema


if typing.TYPE_CHECKING:
    from qaspen_migrations.ddl.base import BaseDDLGenerator
    from qaspen_migrations.schema.migration_changes import (
        MigrationChangesSchema,
    )


if typing.TYPE_CHECKING:
    from qaspen.abc.db_engine import BaseEngine


DDL_GENERATOR_ENGINE_MAPPING: typing.Final[
    dict[str, type[BaseDDLGenerator]]
] = {
    "PSQLPsycopg": PostgresDDLGenerator,
}


def map_ddl_generator(
    engine: BaseEngine[typing.Any, typing.Any, typing.Any],
    migration_changes: list[MigrationChangesSchema],
) -> BaseDDLGenerator:
    try:
        return DDL_GENERATOR_ENGINE_MAPPING[engine.engine_type](
            migration_changes,
        )
    except LookupError as exc:
        raise ConfigurationError(
            f"Invalid engine type {engine.engine_type}"
            f"Valid schemes: {', '.join(DDL_GENERATOR_ENGINE_MAPPING.keys())}",
        ) from exc
