import os
import urllib.parse
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()


@dataclass(frozen=True)
class Settings:
    sqlserver_host: str
    sqlserver_port: str
    sqlserver_db: str
    sqlserver_user: str
    sqlserver_password: str

    sqlserver_driver: str = "ODBC Driver 18 for SQL Server"
    sqlserver_encrypt: str = "yes"
    sqlserver_trust_cert: str = "yes"

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            sqlserver_host=os.environ["SQLSERVER_HOST"],
            sqlserver_port=os.environ.get("SQLSERVER_PORT", "1433"),
            sqlserver_db=os.environ["SQLSERVER_DB"],
            sqlserver_user=os.environ["SQLSERVER_USER"],
            sqlserver_password=os.environ["SQLSERVER_PASSWORD"],
            sqlserver_driver=os.environ.get("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server"),
            sqlserver_encrypt=os.environ.get("SQLSERVER_ENCRYPT", "yes"),
            sqlserver_trust_cert=os.environ.get("SQLSERVER_TRUST_CERT", "yes"),
        )


def build_engine(settings: Optional[Settings] = None) -> Engine:
    """
    Cross-platform (macOS + Windows) SQL Server connection using ODBC + SQLAlchemy.
    Uses odbc_connect to handle:
      - DB names with spaces (e.g., "db_name")
      - host/port formatting
      - consistent behavior across OS
    """
    s = settings or Settings.from_env()

    odbc_str = (
        f"DRIVER={{{s.sqlserver_driver}}};"
        f"SERVER={s.sqlserver_host},{s.sqlserver_port};"
        f"DATABASE={s.sqlserver_db};"
        f"UID={s.sqlserver_user};PWD={s.sqlserver_password};"
        f"Encrypt={s.sqlserver_encrypt};"
        f"TrustServerCertificate={s.sqlserver_trust_cert};"
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
