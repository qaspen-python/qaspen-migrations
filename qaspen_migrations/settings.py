from __future__ import annotations

import typing

from pydantic import BaseModel

QASPEN_MIGRATIONS_TOML_KEY: typing.Final = "qaspen-migrations"


class QaspenMigrationsSettings(BaseModel):
    models: list[str] = []
    migrations_path: str
    engine_path: str
