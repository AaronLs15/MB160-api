import os
import time
import logging
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from db import build_engine

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

IP = os.environ.get("MB160_IP", "192.168.2.13")
PORT = int(os.environ.get("MB160_PORT", "4370"))

def insert_mark(dbconn, device_serial, device_ip, evt):
    ts = getattr(evt, "timestamp", None)
    if ts is None:
        return False

    sql = text("""
        INSERT INTO dbo.AsistenciMarcaje
        (DispositivoSerial, DispositivoIP, UsuarioDispositivo, EventoFechaHora, Punch, Estado, WorkCode)
        VALUES
        (:serial, :ip, :user_id, :ts, :punch, :estado, :workcode)
    """)

    params = {
        "serial": device_serial,
        "ip": device_ip,
        "user_id": str(getattr(evt, "user_id", "")),
        "ts": ts.replace(microsecond=0),
        "punch": int(getattr(evt, "punch", 0) or 0),
        "estado": int(getattr(evt, "status", 0) or 0),
        "workcode": getattr(evt, "workcode", None),
    }

    try:
        dbconn.execute(sql, params)
        return True
    except IntegrityError:
        return False

def main():
    from zk import ZK

    engine = build_engine()
    zk = ZK(IP, port=PORT, timeout=10, password=0)
    conn = zk.connect()

    try:
        try:
            device_serial = conn.get_serialnumber() or IP
        except Exception:
            device_serial = IP

        logging.info(f"Escuchando eventos en vivo MB160 {IP}:{PORT} serial={device_serial}")
        logging.info("Haz una checada en el MB160 para que se inserte en la DB. Ctrl+C para salir.")

        with engine.begin() as dbconn:
            for evt in conn.live_capture(new_timeout=10):
                if evt is None:
                    continue

                ok = insert_mark(dbconn, device_serial, IP, evt)
                logging.info(
                    "EVENT user_id=%s ts=%s punch=%s status=%s | inserted=%s",
                    getattr(evt, "user_id", None),
                    getattr(evt, "timestamp", None),
                    getattr(evt, "punch", None),
                    getattr(evt, "status", None),
                    ok
                )

    except KeyboardInterrupt:
        logging.info("Saliendo...")
    finally:
        try:
            conn.disconnect()
        except Exception:
            pass

if __name__ == "__main__":
    main()
