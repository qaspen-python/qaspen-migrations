from __future__ import annotations
import asyncio
import functools
import importlib
import os
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


def convert_abs_path_to_relative(path_to_convert: str | None) -> str:
    if path_to_convert is None:
        return "./"

    if os.path.isabs(path_to_convert):  # noqa: PTH117
        path_to_convert = os.path.relpath(
            path_to_convert,
            os.getcwd(),  # noqa: PTH109
        )
    if not path_to_convert.startswith("./"):
        path_to_convert = "./" + path_to_convert

    return path_to_convert


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


def as_coroutine(
    func: typing.Callable[..., typing.Any],
) -> typing.Callable[..., typing.Any]:
    @functools.wraps(func)
    def wrapper(*args: typing.Any, **kwargs: typing.Any) -> None:
        asyncio.get_event_loop().run_until_complete(func(*args, **kwargs))

    return wrapper


def get_attribute_value_or_none_and_cast(
    object_to_get_from: typing.Any,
    attribute_name: str,
    cast_type: typing.Callable[[typing.Any], T],
) -> T | None:
    try:
        attribute_value: typing.Final = getattr(
            object_to_get_from,
            attribute_name,
        )
        return cast_type(attribute_value)
    except Exception:  # noqa: BLE001
        return None


def get_int_attribute(
    object_to_get_from: typing.Any,
    attribute_name: str,
) -> int | None:
    return get_attribute_value_or_none_and_cast(
        object_to_get_from,
        attribute_name,
        cast_type=int,
    )


def get_float_attribute(
    object_to_get_from: typing.Any,
    attribute_name: str,
) -> float | None:
    return get_attribute_value_or_none_and_cast(
        object_to_get_from,
        attribute_name,
        cast_type=float,
    )
