from __future__ import annotations
import abc
import dataclasses
import typing


if typing.TYPE_CHECKING:
    from qaspen_migrations.ddl.base import BaseDDLElement


@dataclasses.dataclass
class BaseMigration(abc.ABC):
    current_version: str
    previous_version: str | None
    created_datetime: str

    @abc.abstractmethod
    def migrate(self) -> list[BaseDDLElement]:
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self) -> list[BaseDDLElement]:
        raise NotImplementedError
