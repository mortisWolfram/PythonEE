from __future__ import annotations

from abc import ABC, abstractmethod

from shared.application.command import Command


class CommandHandler[TCommand: Command](ABC):
    @abstractmethod
    async def handle(self, command: TCommand) -> None: ...
