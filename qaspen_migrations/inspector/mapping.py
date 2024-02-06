from __future__ import annotations

import typing

from qaspen_migrations.exceptions import ConfigurationError
from qaspen_migrations.inspector.base import BaseInspector
from qaspen_migrations.inspector.postgres import PostgresInspector

if typing.TYPE_CHECKING:
    from qaspen.abc.db_engine import BaseEngine

    from qaspen_migrations.inspector.base import BaseInspector

INSPECTOR_ENGINE_MAPPING: typing.Final[
    dict[str, type[BaseInspector[typing.Any]]]
] = {
    "PSQLPsycopg": PostgresInspector,
}


def map_inspector(
    engine: BaseEngine[typing.Any, typing.Any, typing.Any],
) -> BaseInspector[BaseEngine[typing.Any, typing.Any, typing.Any]]:
    try:
        return INSPECTOR_ENGINE_MAPPING[engine.engine_type](engine)
    except LookupError as exc:
        raise ConfigurationError(
            f"Invalid engine type {engine.engine_type}"
            f"Valid scheme types: {', '.join(INSPECTOR_ENGINE_MAPPING.keys())}",
        ) from exc
