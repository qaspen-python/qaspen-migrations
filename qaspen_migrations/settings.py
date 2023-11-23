import typing

from pydantic import BaseModel


class QaspenMigrationsSettings(BaseModel):
    models: typing.List[str] = []
    migrations_path: str
    engine_path: str
