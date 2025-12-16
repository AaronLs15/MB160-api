import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _env_int(var: str, default: int) -> int:
    try:
        return int(os.environ.get(var, default))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class DBSettings:
    host: str
    port: str
    database: str
    user: str
    password: str
    driver: str = "ODBC Driver 18 for SQL Server"
    encrypt: str = "yes"
    trust_cert: str = "yes"


@dataclass(frozen=True)
class DeviceSettings:
    ip: str
    port: int
    pull_interval_seconds: int = 60
    user_sync_interval_seconds: int = 10
    user_sync_batch_size: int = 20


@dataclass(frozen=True)
class ApiSettings:
    port: int = 8000


def get_db_settings() -> DBSettings:
    return DBSettings(
        host=os.environ["SQLSERVER_HOST"],
        port=os.environ.get("SQLSERVER_PORT", "1433"),
        database=os.environ["SQLSERVER_DB"],
        user=os.environ["SQLSERVER_USER"],
        password=os.environ["SQLSERVER_PASSWORD"],
        driver=os.environ.get("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server"),
        encrypt=os.environ.get("SQLSERVER_ENCRYPT", "yes"),
        trust_cert=os.environ.get("SQLSERVER_TRUST_CERT", "yes"),
    )


def get_device_settings() -> DeviceSettings:
    return DeviceSettings(
        ip=os.environ.get("MB160_IP", ""),
        port=_env_int("MB160_PORT", 4370),
        pull_interval_seconds=_env_int("PULL_INTERVAL_SECONDS", 60),
        user_sync_interval_seconds=_env_int("USER_SYNC_INTERVAL_SECONDS", 10),
        user_sync_batch_size=_env_int("USER_SYNC_BATCH_SIZE", 20),
    )


def get_api_settings() -> ApiSettings:
    return ApiSettings(port=_env_int("API_PORT", 8000))
