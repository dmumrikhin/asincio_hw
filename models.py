import os
import greenlet
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, JSON



POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'secret')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'swapi')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'swapi')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRS_PORT = os.getenv('POSTGRES_PORT', '5431')

PG_DSN = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRS_PORT}/{POSTGRES_DB}"

engine = create_async_engine(PG_DSN)
Session = async_sessionmaker(engine, expire_on_commit=False)

class Base(AsyncAttrs, DeclarativeBase):
    pass

class People(Base):
    __tablename__ = 'swapi_people'

    id: Mapped[int] = mapped_column(primary_key=True)
    birth_year: Mapped[str] = mapped_column(String(10), nullable=True)
    eye_color: Mapped[str] = mapped_column(String(15), nullable=True)
    films: Mapped[str] = mapped_column(String(10150), nullable=True)
    gender: Mapped[str] = mapped_column(String(50), nullable=True)
    hair_color: Mapped[str] = mapped_column(String(20), nullable=True)
    height: Mapped[str] = mapped_column(String(10), nullable=True)
    homeworld: Mapped[str] = mapped_column(String(150), nullable=True)
    mass: Mapped[str] = mapped_column(String(10), nullable=True)
    name: Mapped[str] = mapped_column(String(50), nullable=True)
    skin_color: Mapped[str] = mapped_column(String(50), nullable=True)
    species: Mapped[str] = mapped_column(String(150), nullable=True)
    starships: Mapped[str] = mapped_column(String(500), nullable=True)
    vehicles: Mapped[str] = mapped_column(String(150), nullable=True)
    





async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)