from __future__ import annotations
import importlib
import typing

import toml
from qaspen.abc.db_engine import BaseEngine

from qaspen_migrations.exceptions import ConfigurationError
from qaspen_migrations.settings import (
    QASPEN_MIGRATIONS_TOML_KEY,
    QaspenMigrationsSettings,
)


if typing.TYPE_CHECKING:
    import pathlib


T = typing.TypeVar("T")


def load_config(config_path: pathlib.Path) -> QaspenMigrationsSettings:
    if config_path.exists():
        content = config_path.read_text()
        settings: typing.Final = toml.loads(content)
    else:
        raise ConfigurationError(
            f"No config file by path {config_path.name} found",
        )
    try:
        migrations_settings: typing.Final = settings["tool"][
            QASPEN_MIGRATIONS_TOML_KEY
        ]
    except LookupError as exc:
        raise ConfigurationError(
            "No qaspen migrations settings found in config file."
            "Please, run 'init' one more time and try again.",
        ) from exc

    return QaspenMigrationsSettings(**migrations_settings)


def load_engine(
    engine_path: str,
) -> BaseEngine[typing.Any, typing.Any, typing.Any]:
    engine_path, engine_object = engine_path.split(":")
    engine_module: typing.Final = importlib.import_module(engine_path)

    try:
        engine: typing.Final = getattr(engine_module, engine_object)
    except AttributeError as exc:
        raise ConfigurationError("No engine object found.") from exc
    if not issubclass(type(engine), BaseEngine):
        raise ConfigurationError("No engine object found.")

    return typing.cast(
        BaseEngine[typing.Any, typing.Any, typing.Any],
        engine,
    )
