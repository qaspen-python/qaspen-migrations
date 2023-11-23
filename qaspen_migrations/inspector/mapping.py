import typing

from qaspen.abc.db_engine import BaseEngine

from qaspen_migrations.exceptions import ConfigurationError
from qaspen_migrations.inspector.base import BaseInspector
from qaspen_migrations.inspector.postgres import PostgresInspector

INSPECTOR_ENGINE_MAPPING: typing.Final[
    typing.Dict[str, typing.Type[BaseInspector[typing.Any]]]
] = {
    "PSQLPsycopg": PostgresInspector,
}


def map_inspector(
    engine: BaseEngine,
) -> BaseInspector:
    try:
        return INSPECTOR_ENGINE_MAPPING[engine.engine_type](engine)
    except LookupError as exc:
        raise ConfigurationError(
            f"Invalid engine type {engine.engine_type}"
            f"Valid scheme types: {', '.join(INSPECTOR_ENGINE_MAPPING.keys())}",
        ) from exc
