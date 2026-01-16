import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import bootstrap

bootstrap.add_src_to_path()

from mb160_service.collector.poller import poll_once
from mb160_service.db import build_engine
from mb160_service.logging import setup_logging


def main() -> int:
    setup_logging()
    engine = build_engine()
    poll_once(engine)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
