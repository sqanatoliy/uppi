import logging

import psycopg2
from decouple import config

logger = logging.getLogger(__name__)

DB_HOST = config("DB_HOST", default="localhost")
DB_PORT = config("DB_PORT", default="5432")
DB_NAME = config("DB_NAME", default="uppi_db")
DB_USER = config("DB_USER", default="uppi_user")
DB_PASSWORD = config("DB_PASSWORD", default="uppi_password")
DB_SSL_MODE = config("DB_SSL_MODE", default="prefer")


def _get_pg_connection():
    """
    Отримати новий конекшн до PostgreSQL.

    Очікувана схема БД:

        CREATE TABLE visure (
            cf TEXT PRIMARY KEY,
            pdf_bucket TEXT NOT NULL,
            pdf_object TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE immobili (
            id BIGSERIAL PRIMARY KEY,
            visura_cf TEXT NOT NULL REFERENCES visure(cf) ON DELETE CASCADE,
            immobile_comune   TEXT,
            via_name          TEXT,
            via_num           TEXT,
            piano             TEXT,
            interno           TEXT,
            foglio            TEXT,
            numero            TEXT,
            sub               TEXT,
            rendita           TEXT,
            superficie_totale NUMERIC,
            categoria         TEXT
        );
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            sslmode=DB_SSL_MODE,
        )
        return conn
    except psycopg2.Error as e:
        logger.exception("[DB] Не вдалося підключитися до PostgreSQL: %s", e)
        raise


def db_has_visura(cf: str) -> bool:
    """Повертає True, якщо візура для заданого CF існує в таблиці visure."""
    conn = None
    try:
        conn = _get_pg_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM visure WHERE cf = %s LIMIT 1;",
                (cf,),
            )
            exists = cur.fetchone() is not None
            logger.debug("[DB] db_has_visura(%s) → %s", cf, exists)
            return exists
    except psycopg2.Error as e:
        logger.exception("[DB] Помилка при перевірці visura для %s: %s", cf, e)
        return False
    finally:
        if conn is not None:
            conn.close()