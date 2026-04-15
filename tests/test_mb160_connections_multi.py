import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

from dotenv import load_dotenv

load_dotenv()


def _env_int(var: str, default: int) -> int:
    try:
        return int(os.environ.get(var, default))
    except (TypeError, ValueError):
        return default


def _parse_ips() -> List[str]:
    raw = os.environ.get("MB160_IPS", "")
    ips = [ip.strip() for ip in raw.split(",") if ip.strip()]
    ips = list(dict.fromkeys(ips))
    if ips:
        return ips

    fallback = (os.environ.get("MB160_IP", "") or "").strip()
    if fallback:
        return [fallback]

    raise SystemExit("ERROR: define MB160_IPS o MB160_IP en .env")


def _try_connect(ip: str, *, port: int, timeout: int, force_udp: bool) -> Dict[str, Any]:
    from zk import ZK

    mode = "UDP" if force_udp else "TCP"
    started = time.monotonic()
    conn = None

    try:
        zk = ZK(ip, port=port, timeout=timeout, password=0, force_udp=force_udp)
        conn = zk.connect()

        try:
            serial = conn.get_serialnumber() or ip
        except Exception:
            serial = ip

        return {
            "ip": ip,
            "ok": True,
            "mode": mode,
            "serial": serial,
            "elapsed": round(time.monotonic() - started, 2),
            "error": "",
        }
    except Exception as e:
        return {
            "ip": ip,
            "ok": False,
            "mode": mode,
            "serial": "",
            "elapsed": round(time.monotonic() - started, 2),
            "error": str(e),
        }
    finally:
        if conn:
            try:
                conn.disconnect()
            except Exception:
                pass


def _test_one_ip(ip: str, *, port: int, timeout: int) -> Dict[str, Any]:
    tcp_result = _try_connect(ip, port=port, timeout=timeout, force_udp=False)
    if tcp_result["ok"]:
        return tcp_result

    udp_result = _try_connect(ip, port=port, timeout=timeout, force_udp=True)
    if udp_result["ok"]:
        return udp_result

    return {
        "ip": ip,
        "ok": False,
        "mode": "TCP+UDP",
        "serial": "",
        "elapsed": round(tcp_result["elapsed"] + udp_result["elapsed"], 2),
        "error": f"TCP: {tcp_result['error']} | UDP: {udp_result['error']}",
    }


def main() -> None:
    ips = _parse_ips()
    port = _env_int("MB160_PORT", 4370)
    timeout = _env_int("MB160_TEST_TIMEOUT_SECONDS", 10)
    workers = max(1, _env_int("MB160_TEST_MAX_WORKERS", min(8, len(ips))))

    print(f"Probando {len(ips)} checadores | port={port} | timeout={timeout}s | workers={workers}")

    results: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="mb160-test") as executor:
        futures = [executor.submit(_test_one_ip, ip, port=port, timeout=timeout) for ip in ips]
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda x: x["ip"])
    failures = [r for r in results if not r["ok"]]

    print("\nResultado por IP:")
    for item in results:
        if item["ok"]:
            print(
                f"OK   ip={item['ip']} mode={item['mode']} serial={item['serial']} elapsed={item['elapsed']}s"
            )
        else:
            print(
                f"FAIL ip={item['ip']} mode={item['mode']} elapsed={item['elapsed']}s error={item['error']}"
            )

    print(f"\nResumen: total={len(results)} ok={len(results) - len(failures)} fail={len(failures)}")

    if failures:
        failed_ips = ", ".join(item["ip"] for item in failures)
        raise SystemExit(f"Sin conexion en estas IPs: {failed_ips}")


if __name__ == "__main__":
    main()
