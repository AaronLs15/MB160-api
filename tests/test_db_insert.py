from datetime import datetime
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import bootstrap
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

bootstrap.add_src_to_path()

from mb160_service.db import build_engine, test_connection


def main():
    engine = build_engine()

    # 1) Conexión
    test_connection(engine)
    print("OK: conexión a SQL Server funciona")

    # 2) Insert 1
    ts = datetime.now().replace(microsecond=0)  # DATETIME2(0)

    insert_sql = text("""
        INSERT INTO dbo.AsistenciaMarcaje
        (DispositivoSerial, DispositivoIP, UsuarioDispositivo, EventoFechaHora, Punch, Estado, WorkCode)
        VALUES
        (:serial, :ip, :user_id, :ts, :punch, :estado, :workcode)
    """)

    params = {
        "serial": "TEST-SIM-001",
        "ip": "127.0.0.1",
        "user_id": "1001",
        "ts": ts,
        "punch": 0,
        "estado": 0,
        "workcode": None,
    }

    with engine.begin() as conn:
        conn.execute(insert_sql, params)

    print(f"OK: insert 1 realizado (ts={ts})")

    # 3) Insert duplicado (debe fallar por UNIQUE)
    try:
        with engine.begin() as conn:
            conn.execute(insert_sql, params)
        print("ERROR: el duplicado se insertó; revisa el UNIQUE UQ_AsistenciaMarcaje_Dedupe")
    except IntegrityError:
        print("OK: deduplicación funciona (IntegrityError por UNIQUE)")


if __name__ == "__main__":
    main()
