import os
from dotenv import load_dotenv

load_dotenv()

MB160_IP = os.environ.get("MB160_IP", "192.168.2.3")
MB160_PORT = int(os.environ.get("MB160_PORT", "4370"))

def try_mode(force_udp: bool) -> bool:
    from zk import ZK

    mode = "UDP" if force_udp else "TCP"
    print(f"\n== Probando {mode} ==  {MB160_IP}:{MB160_PORT}")

    zk = ZK(MB160_IP, port=MB160_PORT, timeout=10, password=0, force_udp=force_udp)
    conn = None
    try:
        conn = zk.connect()
        print("OK: connect()")

        # serial
        try:
            serial = conn.get_serialnumber()
            print(f"OK: serial = {serial}")
        except Exception as e:
            print(f"WARNING: no pude leer serial: {e}")

        # hora
        try:
            dt = conn.get_time()
            print(f"OK: device time = {dt}")
        except Exception as e:
            print(f"WARNING: no pude leer hora: {e}")

        return True

    except Exception as e:
        print(f"ERROR: fallo conexión {mode}: {e}")
        return False

    finally:
        if conn:
            try:
                conn.disconnect()
                print("OK: disconnect()")
            except Exception:
                pass

def main():
    ok = try_mode(force_udp=False)   # TCP primero
    if not ok:
        ok = try_mode(force_udp=True)  # luego UDP

    if not ok:
        raise SystemExit("No se pudo conectar por TCP ni UDP. Revisa red/firewall/VLAN.")

if __name__ == "__main__":
    main()