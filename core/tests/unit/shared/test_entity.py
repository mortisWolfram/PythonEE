import uuid

from shared.domain.entity import Entity


class _User(Entity[uuid.UUID]):
    def __init__(self, id: uuid.UUID, name: str) -> None:
        self.id = id
        self.name = name


class _Product(Entity[uuid.UUID]):
    def __init__(self, id: uuid.UUID) -> None:
        self.id = id


def test_equal_when_same_type_and_id() -> None:
    uid = uuid.uuid4()
    assert _User(uid, "alice") == _User(uid, "bob")


def test_not_equal_when_different_id() -> None:
    assert _User(uuid.uuid4(), "a") != _User(uuid.uuid4(), "a")


def test_not_equal_when_different_type_and_same_id() -> None:
    uid = uuid.uuid4()
    assert _User(uid, "a") != _Product(uid)


def test_hashable() -> None:
    uid = uuid.uuid4()
    assert len({_User(uid, "a"), _User(uid, "b")}) == 1
    assert len({_User(uuid.uuid4(), "a"), _User(uuid.uuid4(), "a")}) == 2
