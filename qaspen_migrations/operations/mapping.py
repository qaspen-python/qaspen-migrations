from __future__ import annotations
import typing

from qaspen_migrations.exceptions import ConfigurationError
from qaspen_migrations.operations.postgres import PostgresOperationsImplementer


if typing.TYPE_CHECKING:
    from qaspen_migrations.operations.base import BaseOperationsImplementer


IMPLEMENTER_ENGINE_MAPPING: typing.Final[
    dict[
        str,
        type[
            BaseOperationsImplementer[
                typing.Any,
                typing.Any,
                typing.Any,
                typing.Any,
                typing.Any,
            ]
        ],
    ]
] = {
    "PSQLPsycopg": PostgresOperationsImplementer,
}


def map_operations_implementer(
    engine_type: str,
) -> BaseOperationsImplementer[
    typing.Any,
    typing.Any,
    typing.Any,
    typing.Any,
    typing.Any,
]:
    try:
        return IMPLEMENTER_ENGINE_MAPPING[engine_type]()
    except LookupError as exc:
        raise ConfigurationError(
            f"Invalid engine type {engine_type}\n"
            f"Valid schemes: {', '.join(IMPLEMENTER_ENGINE_MAPPING.keys())}",
        ) from exc
