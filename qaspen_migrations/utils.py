import asyncio
import functools
import importlib.util
import os
import pathlib
import types
import typing

import toml

from qaspen_migrations.exceptions import ConfigurationError
from qaspen_migrations.settings import QaspenMigrationsSettings


def convert_abs_path_to_relative(path_to_convert: typing.Optional[str]) -> str:
    if path_to_convert is None:
        return "./"

    if os.path.isabs(path_to_convert):
        path_to_convert = os.path.relpath(path_to_convert, os.getcwd())
    if not path_to_convert.startswith("./"):
        path_to_convert = "./" + path_to_convert

    return path_to_convert


def import_module_by_path(
    module_path: str,
) -> types.ModuleType:
    module_name: typing.Final = module_path.split("/")[-1].strip(".py")
    spec: typing.Final = importlib.util.spec_from_file_location(
        module_name,
        module_path,
    )
    if spec is None:
        raise ConfigurationError(
            f"No module via path {module_path} found.",
        )

    imported_module: typing.Final = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(imported_module)

    return imported_module


def load_config(config_path: pathlib.Path) -> QaspenMigrationsSettings:
    if config_path.exists():
        content = config_path.read_text()
        settings: typing.Final = toml.loads(content)
    else:
        raise ConfigurationError(
            f"No config file by path {config_path.name} found",
        )

    try:
        migrations_settings: typing.Final = settings["tool"]["qaspen"]["migrations"]
    except LookupError as exc:
        raise ConfigurationError(
            "No qaspen migrations settings found in config file. "
            "Please, run 'init' one more time and try again.",
        ) from exc

    return QaspenMigrationsSettings(**migrations_settings)


def as_coroutine(
    func: typing.Callable[..., typing.Any],
) -> typing.Callable[..., typing.Any]:
    @functools.wraps(func)
    def wrapper(*args: typing.Any, **kwargs: typing.Any) -> None:
        asyncio.get_event_loop().run_until_complete(func(*args, **kwargs))

    return wrapper
