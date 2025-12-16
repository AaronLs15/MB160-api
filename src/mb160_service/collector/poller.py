import time
import logging
from datetime import datetime
from typing import Optional, Dict

from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, OperationalError

from mb160_service.config import get_device_settings
from mb160_service.db import build_engine

log = logging.getLogger("mb160.collector")

device_settings = get_device_settings()

MB160_IP = device_settings.ip
MB160_PORT = device_settings.port
PULL_INTERVAL_SECONDS = device_settings.pull_interval_seconds


def _get_last_ts(dbconn, device_serial: str) -> Optional[datetime]:
    q = text("""
        SELECT MAX(EventoFechaHora) AS MaxTs
        FROM dbo.AsistenciaMarcaje
        WHERE DispositivoSerial = :DeviceSerial
    """)
    row = dbconn.execute(q, {"DeviceSerial": device_serial}).mappings().first()
    return row["MaxTs"] if row and row["MaxTs"] is not None else None


def _insert_mark(
    dbconn,
    *,
    device_serial: str,
    device_ip: str,
    user_id: str,
    user_name: Optional[str],
    ts_local: datetime,
    punch: int,
    estado: int,
    workcode: Optional[int],
) -> None:
    ins = text("""
        INSERT INTO dbo.AsistenciaMarcaje
        (DispositivoSerial, DispositivoIP, UsuarioDispositivo, UsuarioNombre,
         EventoFechaHora, Punch, Estado, WorkCode)
        VALUES
        (:DispositivoSerial, :DispositivoIP, :UsuarioDispositivo, :UsuarioNombre,
         :EventoFechaHora, :Punch, :Estado, :WorkCode)
    """)
    dbconn.execute(ins, {
        "DispositivoSerial": device_serial,
        "DispositivoIP": device_ip,
        "UsuarioDispositivo": str(user_id),
        "UsuarioNombre": (user_name or None),
        "EventoFechaHora": ts_local.replace(microsecond=0),  # guardar HORA LOCAL
        "Punch": int(punch or 0),
        "Estado": int(estado or 0),
        "WorkCode": workcode,
    })


def _build_user_map(conn_dev) -> Dict[str, str]:
    """
    Construye un mapa user_id -> name desde el dispositivo.
    Nota: el nombre no viene en cada log, se obtiene desde get_users().
    """
    user_map: Dict[str, str] = {}
    try:
        users = conn_dev.get_users() or []
        for u in users:
            uid = str(getattr(u, "user_id", "")).strip()
            name = str(getattr(u, "name", "")).strip()
            if uid:
                user_map[uid] = name
    except Exception as e:
        log.warning("No se pudo obtener lista de usuarios (get_users). Continuando sin nombres. Error=%s", e)
    return user_map


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type((OperationalError, OSError, TimeoutError)),
    reraise=True,
)
def poll_once(engine) -> None:
    if not MB160_IP:
        raise RuntimeError("MB160_IP no está definido en .env")

    # Import local para no romper si aún no instalas pyzk en algunos ambientes
    from zk import ZK  # type: ignore

    zk = ZK(MB160_IP, port=MB160_PORT, timeout=10, password=0)
    conn_dev = None

    try:
        conn_dev = zk.connect()

        try:
            conn_dev.disable_device()
        except Exception:
            pass

        try:
            device_serial = conn_dev.get_serialnumber() or MB160_IP
        except Exception:
            device_serial = MB160_IP

        # ===== NUEVO: mapa user_id -> name =====
        user_map = _build_user_map(conn_dev)

        logs = conn_dev.get_attendance() or []

        with engine.begin() as dbconn:
            last_ts = _get_last_ts(dbconn, device_serial)

            inserted = 0
            dup_skipped = 0

            for a in logs:
                ts = getattr(a, "timestamp", None)
                if ts is None:
                    continue

                # incremental
                if last_ts is not None and ts <= last_ts:
                    continue

                user_id = str(getattr(a, "user_id", "")).strip()
                user_name = user_map.get(user_id)  # puede ser None

                estado = getattr(a, "status", 0)
                punch = getattr(a, "punch", 0)
                workcode = getattr(a, "workcode", None)

                try:
                    _insert_mark(
                        dbconn,
                        device_serial=device_serial,
                        device_ip=MB160_IP,
                        user_id=user_id,
                        user_name=user_name,
                        ts_local=ts,
                        punch=int(punch or 0),
                        estado=int(estado or 0),
                        workcode=workcode,
                    )
                    inserted += 1
                except IntegrityError:
                    dup_skipped += 1

            log.info(
                "Poll OK | device=%s | logs=%d | inserted=%d | dup_skipped=%d | last_ts=%s",
                device_serial, len(logs), inserted, dup_skipped, last_ts
            )

    finally:
        if conn_dev:
            try:
                conn_dev.enable_device()
            except Exception:
                pass
            try:
                conn_dev.disconnect()
            except Exception:
                pass


def run_forever() -> None:
    engine = build_engine()
    log.info("Collector iniciado | MB160=%s:%s | interval=%ss", MB160_IP, MB160_PORT, PULL_INTERVAL_SECONDS)

    while True:
        try:
            poll_once(engine)
        except Exception as e:
            log.exception("Error en poll_once: %s", e)
        time.sleep(PULL_INTERVAL_SECONDS)
