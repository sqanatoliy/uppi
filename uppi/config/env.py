from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from decouple import config


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: str
    name: str
    user: str
    password: str
    ssl_mode: str


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool
    visure_bucket: str
    attestazioni_bucket: str


@dataclass(frozen=True)
class VisuraPolicyConfig:
    ttl_days: Optional[int]


@dataclass(frozen=True)
class AeConfig:
    login_url: str
    servizi_url: str
    logout_url: str
    username: str
    password: str
    pin: str
    two_captcha_api_key: str


def load_db_config() -> DbConfig:
    return DbConfig(
        host=config("DB_HOST", default="localhost"),
        port=config("DB_PORT", default="5432"),
        name=config("DB_NAME", default="uppi_db"),
        user=config("DB_USER", default="uppi_user"),
        password=config("DB_PASSWORD", default="uppi_password"),
        ssl_mode=config("DB_SSL_MODE", default="prefer"),
    )


def load_minio_config() -> MinioConfig:
    return MinioConfig(
        endpoint=config("MINIO_ENDPOINT", default="localhost:9000"),
        access_key=config("MINIO_ACCESS_KEY", default="minioadmin"),
        secret_key=config("MINIO_SECRET_KEY", default="minioadmin"),
        secure=config("MINIO_SECURE", default="False").strip().lower() == "true",
        visure_bucket=config("VISURE_BUCKET", default=config("MINIO_BUCKET", default="uppi-bucket")),
        attestazioni_bucket=config("ATTESTAZIONI_BUCKET", default="attestazioni"),
    )


def load_visura_policy_config() -> VisuraPolicyConfig:
    raw = config("VISURA_TTL_DAYS", default="").strip()
    ttl = int(raw) if raw.isdigit() else None
    return VisuraPolicyConfig(ttl_days=ttl)


def load_ae_config() -> AeConfig:
    return AeConfig(
        login_url=config("AE_LOGIN_URL"),
        servizi_url=config("AE_URL_SERVIZI"),
        logout_url=config("SISTER_LOGOUT_URL"),
        username=config("AE_USERNAME"),
        password=config("AE_PASSWORD"),
        pin=config("AE_PIN"),
        two_captcha_api_key=config("TWO_CAPTCHA_API_KEY"),
    )
