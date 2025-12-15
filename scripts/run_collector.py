import threading
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import bootstrap
bootstrap.add_src_to_path()

from mb160_service.collector.poller import run_forever as run_attendance_forever
from mb160_service.collector.user_sync import run_user_sync_forever
from mb160_service.db import build_engine
from mb160_service.logging import setup_logging


def main() -> None:
    setup_logging()
    engine = build_engine()

    t1 = threading.Thread(target=run_attendance_forever, name="attendance", daemon=True)
    t2 = threading.Thread(target=run_user_sync_forever, args=(engine,), name="user_sync", daemon=True)

    t1.start()
    t2.start()

    t1.join()
    t2.join()


if __name__ == "__main__":
    main()
