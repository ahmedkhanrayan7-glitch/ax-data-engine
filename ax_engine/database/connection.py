"""
AX Engine — Database Connection Pool

Uses SQLAlchemy async engine with asyncpg driver.
Single pool instance shared across all API workers.
"""
from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from ax_engine.config import settings


class DatabasePool:
    def __init__(self):
        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker | None = None

    async def connect(self) -> None:
        self._engine = create_async_engine(
            settings.DATABASE_URL,
            pool_size=settings.DATABASE_POOL_SIZE,
            max_overflow=settings.DATABASE_MAX_OVERFLOW,
            pool_timeout=settings.DATABASE_POOL_TIMEOUT,
            pool_pre_ping=True,
            echo=settings.DEBUG,
        )
        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def disconnect(self) -> None:
        if self._engine:
            await self._engine.dispose()

    async def is_connected(self) -> bool:
        if not self._engine:
            return False
        try:
            async with self._engine.connect() as conn:
                await conn.execute(__import__("sqlalchemy").text("SELECT 1"))
            return True
        except Exception:
            return False

    def get_session(self) -> AsyncSession:
        if not self._session_factory:
            raise RuntimeError("Database pool not initialized.")
        return self._session_factory()


db_pool = DatabasePool()
