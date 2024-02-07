from __future__ import annotations

import typing
from abc import ABC, abstractmethod

if typing.TYPE_CHECKING:
    from qaspen import BaseTable

    from qaspen_migrations.inspector.schema import TableDumpSchema

Engine = typing.TypeVar(
    "Engine",
)


class BaseInspector(ABC, typing.Generic[Engine]):
    inspect_query: str

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    @abstractmethod
    async def inspect_database(
        self,
        tables: list[type[BaseTable]],
    ) -> list[TableDumpSchema]:
        raise NotImplementedError
