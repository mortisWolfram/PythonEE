from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Single declarative root for all SQLAlchemy models across shared packages.

    Modules that define their own ORM models MUST subclass this Base so that
    Alembic's ``target_metadata = Base.metadata`` sees every table.
    """
    pass
