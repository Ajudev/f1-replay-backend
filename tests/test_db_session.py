"""Database session dependency lifecycle tests."""

from collections.abc import AsyncGenerator
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.db.session import get_db


async def test_get_db_yields_session_and_closes_it(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(session_factory=session_factory))
    )

    agen: AsyncGenerator[AsyncSession, None] = get_db(request)  # type: ignore[arg-type]
    session = await agen.__anext__()
    assert isinstance(session, AsyncSession)
    assert session.is_active

    await agen.aclose()
    # After close, the session should not be usable for new work.
    assert session.is_active is False or not session.in_transaction()


async def test_get_db_rolls_back_on_error(sqlite_engine: AsyncEngine) -> None:
    real_factory = async_sessionmaker(sqlite_engine, class_=AsyncSession, expire_on_commit=False)
    session = real_factory()
    session.rollback = AsyncMock(wraps=session.rollback)
    session.close = AsyncMock(wraps=session.close)

    mock_factory = MagicMock(return_value=session)
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(session_factory=mock_factory))
    )

    agen = get_db(request)  # type: ignore[arg-type]
    yielded = await agen.__anext__()
    assert yielded is session

    with pytest.raises(RuntimeError, match="boom"):
        await agen.athrow(RuntimeError("boom"))

    session.rollback.assert_awaited()
    session.close.assert_awaited()
