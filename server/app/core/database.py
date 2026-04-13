from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from app.core.config import get_settings

engine = create_async_engine(
    get_settings().DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_recycle=300,       # recycle connections every 5 min (Neon closes idle after ~5 min)
    pool_size=5,
    max_overflow=10,
    connect_args={
        "server_settings": {"application_name": "gen-chatbot"},
        "command_timeout": 30,
    },
)
async_session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with async_session() as session:
        try:
            yield session
        finally:
            try:
                await session.close()
            except Exception:
                pass  # connection may already be closed (Neon idle timeout)
