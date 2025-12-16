import time
import logging
import inspect
from typing import Dict, Any, List

from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from mb160_service.config import get_device_settings

log = logging.getLogger("mb160.user_sync")

device_settings = get_device_settings()

MB160_IP = device_settings.ip
MB160_PORT = device_settings.port
USER_SYNC_INTERVAL_SECONDS = device_settings.user_sync_interval_seconds
USER_SYNC_BATCH_SIZE = device_settings.user_sync_batch_size


def _dequeue_batch(dbconn, batch_size: int) -> List[Dict[str, Any]]:
    """
    Toma un batch de pendientes y los marca como 'Procesando' dentro de la misma transacción.
    Evita que 2 workers agarren lo mismo.
    """
    q = text(f"""
        ;WITH cte AS (
            SELECT TOP ({batch_size}) MB160UserSyncQueueID
            FROM dbo.MB160UserSyncQueue WITH (READPAST, UPDLOCK, ROWLOCK)
            WHERE Estatus IN (0, 3) -- Pendiente o Error (reintento)
            ORDER BY MB160UserSyncQueueID
        )
        UPDATE q
        SET
            Estatus = 1, -- Procesando
            Intentos = Intentos + 1,
            UltimoCambio = SYSDATETIME()
        OUTPUT
            inserted.MB160UserSyncQueueID,
            inserted.EmpresaID,
            inserted.PersonaID,
            inserted.UsuarioDispositivo,
            inserted.UsuarioNombre,
            inserted.Intentos
        FROM dbo.MB160UserSyncQueue q
        INNER JOIN cte ON cte.MB160UserSyncQueueID = q.MB160UserSyncQueueID;
    """)
    rows = dbconn.execute(q).mappings().all()
    return [dict(r) for r in rows]


def _mark_done(dbconn, queue_id: int) -> None:
    dbconn.execute(
        text("""
            UPDATE dbo.MB160UserSyncQueue
            SET Estatus = 2,
                UltimoError = NULL,
                ProcesadoEn = SYSDATETIME(),
                UltimoCambio = SYSDATETIME()
            WHERE MB160UserSyncQueueID = :id
        """),
        {"id": queue_id},
    )


def _mark_error(dbconn, queue_id: int, err: str) -> None:
    dbconn.execute(
        text("""
            UPDATE dbo.MB160UserSyncQueue
            SET Estatus = 3,
                UltimoError = :err,
                UltimoCambio = SYSDATETIME()
            WHERE MB160UserSyncQueueID = :id
        """),
        {"id": queue_id, "err": err[:4000]},
    )


def _set_user_compat(conn_dev, *, user_id: str, name: str) -> None:
    """
    Llama conn_dev.set_user() de forma compatible con distintas versiones de pyzk.
    Normalmente el MB160 requiere 'user_id' (enroll) y 'name'.
    """
    fn = conn_dev.set_user
    sig = inspect.signature(fn)
    params = sig.parameters

    # Intento 1: usar nombres de parámetros si existen
    kwargs = {}
    if "user_id" in params:
        kwargs["user_id"] = str(user_id)
    if "name" in params:
        kwargs["name"] = str(name)
    if "privilege" in params:
        kwargs["privilege"] = 0
    if "password" in params:
        kwargs["password"] = ""
    if "group_id" in params:
        kwargs["group_id"] = ""
    if "card" in params:
        kwargs["card"] = 0
    if "uid" in params:
        # uid interno: usa 0 para que el device lo asigne o deriva un hash si quisieras
        kwargs["uid"] = 0

    if kwargs:
        fn(**kwargs)
        return

    # Intento 2: fallback posicional común (varía por lib)
    # set_user(uid, name, privilege, password, group_id, user_id, card)
    fn(0, str(name), 0, "", "", str(user_id), 0)


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type((OperationalError, OSError, TimeoutError)),
    reraise=True,
)
def sync_users_once(engine) -> None:
    if not MB160_IP:
        raise RuntimeError("MB160_IP no está definido en .env")

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

        with engine.begin() as dbconn:
            batch = _dequeue_batch(dbconn, USER_SYNC_BATCH_SIZE)

        if not batch:
            return

        ok_count = 0
        err_count = 0

        # Procesa 1x1 para guardar error por fila
        for item in batch:
            qid = int(item["MB160UserSyncQueueID"])
            user_id = str(item["UsuarioDispositivo"])
            name = str(item["UsuarioNombre"])

            try:
                _set_user_compat(conn_dev, user_id=user_id, name=name)

                with engine.begin() as dbconn:
                    _mark_done(dbconn, qid)

                ok_count += 1
            except Exception as e:
                with engine.begin() as dbconn:
                    _mark_error(dbconn, qid, str(e))
                err_count += 1

        log.info(
            "UserSync OK | device=%s | processed=%d | ok=%d | error=%d",
            device_serial, len(batch), ok_count, err_count
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


def run_user_sync_forever(engine) -> None:
    log.info("UserSync iniciado | MB160=%s:%s | interval=%ss", MB160_IP, MB160_PORT, USER_SYNC_INTERVAL_SECONDS)
    while True:
        try:
            sync_users_once(engine)
        except Exception as e:
            log.exception("Error en sync_users_once: %s", e)
        time.sleep(USER_SYNC_INTERVAL_SECONDS)
