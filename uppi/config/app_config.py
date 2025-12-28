from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from decouple import config


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    name: str
    user: str
    password: str
    ssl_mode: str


@dataclass(frozen=True)
class VisuraCacheConfig:
    ttl_days: Optional[int]


@dataclass(frozen=True)
class AppConfig:
    database: DatabaseConfig
    visura_cache: VisuraCacheConfig


    @staticmethod
    def _parse_int(value: str | None) -> Optional[int]:
        if value is None:
            return None
        value = str(value).strip()
        if value == "":
            return None
        return int(value)

    @classmethod
    def from_env(cls) -> "AppConfig":
        ttl_days = cls._parse_int(config("VISURA_TTL_DAYS", default=""))

        db = DatabaseConfig(
            host=config("DB_HOST", default="localhost"),
            port=int(config("DB_PORT", default="5432")),
            name=config("DB_NAME", default="uppi_db"),
            user=config("DB_USER", default="uppi_user"),
            password=config("DB_PASSWORD", default="uppi_password"),
            ssl_mode=config("DB_SSL_MODE", default="prefer"),
        )

        if ttl_days is not None and ttl_days < 0:
            raise ValueError("VISURA_TTL_DAYS must be >= 0")

        return cls(database=db, visura_cache=VisuraCacheConfig(ttl_days=ttl_days))
