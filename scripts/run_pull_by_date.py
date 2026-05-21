import argparse
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

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

log = logging.getLogger("mb160.pull_by_date")


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


def _parse_args() -> tuple[datetime, datetime]:
    parser = argparse.ArgumentParser(
        description="Pull marcajes por rango de fechas desde múltiples MB160.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", help="Día único en formato YYYY-MM-DD")
    group.add_argument("--start", help="Inicio del rango YYYY-MM-DD (requiere --end)")
    parser.add_argument("--end", help="Fin del rango YYYY-MM-DD inclusivo (con --start)")
    args = parser.parse_args()

    if args.date:
        start = datetime.strptime(args.date, "%Y-%m-%d")
        end_exclusive = start + timedelta(days=1)
    else:
        if not args.end:
            parser.error("--start requiere --end")
        start = datetime.strptime(args.start, "%Y-%m-%d")
        end_inclusive = datetime.strptime(args.end, "%Y-%m-%d")
        if end_inclusive < start:
            parser.error("--end debe ser >= --start")
        end_exclusive = end_inclusive + timedelta(days=1)

    return start, end_exclusive


def _poll_device(engine, ip: str, port: int, start: datetime, end_exclusive: datetime) -> None:
    poll_once(
        engine,
        device_ip=ip,
        device_port=port,
        min_ts=start,
        max_ts=end_exclusive,
        use_last_ts=False,
    )


def main() -> int:
    setup_logging()
    settings = get_device_settings()

    start, end_exclusive = _parse_args()
    device_ips = _parse_ips()
    max_workers = max(1, _env_int("MULTI_PULL_MAX_WORKERS", min(6, len(device_ips))))

    engine = build_engine()

    log.info(
        "Pull rango | start=%s | end_exclusive=%s | devices=%d | port=%s | workers=%d",
        start, end_exclusive, len(device_ips), settings.port, max_workers,
    )

    unreachable = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="mb160-pull") as executor:
        future_to_ip = {
            executor.submit(_poll_device, engine, ip, settings.port, start, end_exclusive): ip
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

    log.info("Pull completado | offline=%d | errors=%d", unreachable, errors)
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
