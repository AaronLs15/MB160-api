import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import bootstrap
from sqlalchemy import text

bootstrap.add_src_to_path()

from mb160_service.db import build_engine, test_connection


def main():
    engine = build_engine()
    test_connection(engine)

    with engine.connect() as conn:
        dbname = conn.execute(text("SELECT DB_NAME()")).scalar_one()
        now = conn.execute(text("SELECT SYSDATETIME()")).scalar_one()

    print(f"OK: DB={dbname} | SYSDATETIME={now}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
