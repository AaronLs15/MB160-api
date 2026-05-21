import os
import sys
import time
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import bootstrap

bootstrap.add_src_to_path()

from mb160_service.collector.poller import poll_once
from mb160_service.config import get_device_settings
from mb160_service.db import build_engine
from mb160_service.logging import setup_logging

try:
    from zk.exception import ZKNetworkError, ZKErrorResponse  # type: ignore
except Exception:  # pragma: no cover
    ZKNetworkError = ZKErrorResponse = ()  # type: ignore

log = logging.getLogger("mb160.collector.multi")


def _env_int(var: str, default: int) -> int:
    try:
        return int(os.environ.get(var, default))
    except (TypeError, ValueError):
        return default


def _parse_ips() -> list[str]:
    raw = os.environ.get("MB160_IPS", "")
    ips = [value.strip() for value in raw.split(",") if value.strip()]
    unique_ips = list(dict.fromkeys(ips))
    if unique_ips:
        return unique_ips

    fallback_ip = (os.environ.get("MB160_IP", "") or "").strip()
    if fallback_ip:
        return [fallback_ip]

    raise RuntimeError("Define MB160_IPS en .env (ej: MB160_IPS=192.168.1.10,192.168.1.11)")


def _poll_device(engine, ip: str, port: int) -> None:
    poll_once(engine, device_ip=ip, device_port=port)


def main() -> int:
    setup_logging()
    settings = get_device_settings()

    device_ips = _parse_ips()
    interval_seconds = _env_int("MULTI_PULL_INTERVAL_SECONDS", 300)
    max_workers = max(1, _env_int("MULTI_PULL_MAX_WORKERS", min(6, len(device_ips))))

    engine = build_engine()

    log.info(
        "Multi collector iniciado | devices=%d | port=%s | interval=%ss | workers=%d",
        len(device_ips),
        settings.port,
        interval_seconds,
        max_workers,
    )

    while True:
        cycle_start = time.monotonic()
        log.info("Iniciando ciclo de pull para %d dispositivos", len(device_ips))

        unreachable = 0
        errors = 0

        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="mb160") as executor:
            future_to_ip = {
                executor.submit(_poll_device, engine, ip, settings.port): ip
                for ip in device_ips
            }
            for future in as_completed(future_to_ip):
                ip = future_to_ip[future]
                try:
                    future.result()
                except ZKNetworkError:
                    unreachable += 1
                    log.warning("Device offline | ip=%s", ip)
                except ZKErrorResponse as e:
                    errors += 1
                    log.error("Device error | ip=%s | %s", ip, e)
                except Exception as e:
                    errors += 1
                    log.error("Pull failed | ip=%s | %s: %s", ip, type(e).__name__, e)

        elapsed = time.monotonic() - cycle_start
        sleep_seconds = max(1, interval_seconds - int(elapsed))
        log.info(
            "Ciclo completado | elapsed=%.1fs | offline=%d | errors=%d | next_in=%ss",
            elapsed, unreachable, errors, sleep_seconds,
        )
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
