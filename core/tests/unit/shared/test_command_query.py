from dataclasses import dataclass

from shared.application.command import Command
from shared.application.command_handler import CommandHandler
from shared.application.query import Query
from shared.application.query_handler import QueryHandler


@dataclass(frozen=True)
class _CreateThing(Command):
    name: str


@dataclass(frozen=True)
class _GetThing(Query):
    thing_id: str


class _CreateThingHandler(CommandHandler[_CreateThing]):
    def __init__(self) -> None:
        self.calls: list[_CreateThing] = []

    async def handle(self, command: _CreateThing) -> None:
        self.calls.append(command)


class _GetThingHandler(QueryHandler[_GetThing, str]):
    async def handle(self, query: _GetThing) -> str:
        return f"thing:{query.thing_id}"


async def test_command_handler_receives_typed_command() -> None:
    h = _CreateThingHandler()
    await h.handle(_CreateThing(name="x"))
    assert h.calls == [_CreateThing(name="x")]


async def test_query_handler_returns_result() -> None:
    h = _GetThingHandler()
    assert await h.handle(_GetThing(thing_id="42")) == "thing:42"
