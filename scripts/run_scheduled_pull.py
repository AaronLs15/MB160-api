import os
import sys
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import bootstrap

bootstrap.add_src_to_path()

from mb160_service.collector.poller import poll_once
from mb160_service.db import build_engine
from mb160_service.logging import setup_logging

log = logging.getLogger("mb160.scheduler")


def _env_int(var: str, default: int) -> int:
    try:
        return int(os.environ.get(var, default))
    except (TypeError, ValueError):
        return default


def _next_run(now: datetime, *, hour: int, minute: int) -> datetime:
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


def main() -> int:
    setup_logging()

    hour = _env_int("DAILY_PULL_HOUR", 20)
    minute = _env_int("DAILY_PULL_MINUTE", 0)

    log.info("Scheduler iniciado | hora=%02d:%02d", hour, minute)

    while True:
        now = datetime.now()
        next_run = _next_run(now, hour=hour, minute=minute)
        sleep_seconds = max(1, int((next_run - now).total_seconds()))

        log.info(
            "Siguiente ejecucion: %s | en %ss",
            next_run.strftime("%Y-%m-%d %H:%M:%S"),
            sleep_seconds,
        )

        time.sleep(sleep_seconds)

        engine = build_engine()
        try:
            poll_once(engine)
        except Exception as e:
            log.exception("Error en poll_once: %s", e)
        finally:
            engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
