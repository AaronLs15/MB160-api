import urllib.parse
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from mb160_service.config import DBSettings, get_db_settings


def build_engine(settings: Optional[DBSettings] = None) -> Engine:
    """
    Cross-platform (macOS + Windows) SQL Server connection using ODBC + SQLAlchemy.
    Uses odbc_connect to handle:
      - DB names with spaces (e.g., "db_name")
      - host/port formatting
      - consistent behavior across OS
    """
    s = settings or get_db_settings()

    odbc_str = (
        f"DRIVER={{{s.driver}}};"
        f"SERVER={s.host},{s.port};"
        f"DATABASE={s.database};"
        f"UID={s.user};PWD={s.password};"
        f"Encrypt={s.encrypt};"
        f"TrustServerCertificate={s.trust_cert};"
        f"Connection Timeout=10;"
    )

    connect_str = urllib.parse.quote_plus(odbc_str)
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={connect_str}",
        pool_pre_ping=True,
        pool_recycle=1800,  # helps with VPN/network drops
        future=True,
    )
    return engine


def test_connection(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
