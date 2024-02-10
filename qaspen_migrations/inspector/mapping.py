from __future__ import annotations

import typing

from qaspen_migrations.exceptions import ConfigurationError
from qaspen_migrations.inspector.base import BaseInspector
from qaspen_migrations.inspector.postgres import PostgresInspector

if typing.TYPE_CHECKING:
    from qaspen import BaseTable
    from qaspen.abc.db_engine import BaseEngine

    from qaspen_migrations.inspector.base import BaseInspector

INSPECTOR_ENGINE_MAPPING: typing.Final[
    dict[str, type[BaseInspector[typing.Any, typing.Any]]]
] = {
    "PSQLPsycopg": PostgresInspector,
}


def map_inspector(
    engine: BaseEngine[typing.Any, typing.Any, typing.Any],
    tables: list[type[BaseTable]],
) -> BaseInspector[typing.Any, BaseEngine[typing.Any, typing.Any, typing.Any]]:
    try:
        return INSPECTOR_ENGINE_MAPPING[engine.engine_type](engine, tables)
    except LookupError as exc:
        raise ConfigurationError(
            f"Invalid engine type {engine.engine_type}"
            f"Valid schemes: {', '.join(INSPECTOR_ENGINE_MAPPING.keys())}",
        ) from exc
