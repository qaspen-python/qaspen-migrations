import typing
from abc import ABC, abstractmethod

from qaspen import BaseTable

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
        models: typing.List[typing.Type[BaseTable]],
    ) -> None:
        raise NotImplementedError
