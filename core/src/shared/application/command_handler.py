from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from shared.application.command import Command

TCommand = TypeVar("TCommand", bound=Command)


class CommandHandler(Generic[TCommand], ABC):
    @abstractmethod
    async def handle(self, command: TCommand) -> None: ...
