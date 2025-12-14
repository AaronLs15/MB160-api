import sys
from sqlalchemy import text

from db import build_engine, test_connection


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
