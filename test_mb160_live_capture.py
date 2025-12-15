import os
import time
from dotenv import load_dotenv
from zk import ZK

load_dotenv()

IP = os.environ.get("MB160_IP", "192.168.2.13")
PORT = int(os.environ.get("MB160_PORT", "4370"))

def main():
    zk = ZK(IP, port=PORT, timeout=10, password=0)
    conn = zk.connect()

    print(f"Escuchando eventos en vivo en {IP}:{PORT}. Haz un marcaje en el MB160...")
    print("Ctrl+C para salir.\n")

    start = time.time()
    try:
        # live_capture() va “yield” eventos; a veces devuelve None por timeout
        for evt in conn.live_capture(new_timeout=10):
            if evt is None:
                # timeout sin evento, seguimos
                if time.time() - start > 120:
                    print("Sin eventos en 120s. Terminando.")
                    break
                continue

            # evt es un Attendance object: user_id, timestamp, status, punch, uid
            print(
                "EVENT:",
                "user_id=", getattr(evt, "user_id", None),
                "timestamp=", getattr(evt, "timestamp", None),
                "status=", getattr(evt, "status", None),
                "punch=", getattr(evt, "punch", None),
            )

            # corta después de 1 evento (puedes cambiarlo)
            break

    except KeyboardInterrupt:
        print("\nSaliendo...")
    finally:
        try:
            conn.end_live_capture = True  # recomendado para parar limpio
        except Exception:
            pass
        conn.disconnect()

if __name__ == "__main__":
    main()
