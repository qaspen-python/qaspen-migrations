from __future__ import annotations
import asyncio
import functools
import os
import typing


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


def as_coroutine(
    func: typing.Callable[..., typing.Any],
) -> typing.Callable[..., typing.Any]:
    @functools.wraps(func)
    def wrapper(*args: typing.Any, **kwargs: typing.Any) -> None:
        asyncio.get_event_loop().run_until_complete(func(*args, **kwargs))

    return wrapper
