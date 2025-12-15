import os
from dotenv import load_dotenv
from zk import ZK

load_dotenv()

IP = os.environ.get("MB160_IP", "192.168.2.13")
PORT = int(os.environ.get("MB160_PORT", "4370"))

def main():
    zk = ZK(IP, port=PORT, timeout=10, password=0)
    conn = zk.connect()
    try:
        conn.disable_device()

        serial = conn.get_serialnumber()
        dt = conn.get_time()

        users = conn.get_users() or []
        logs = conn.get_attendance() or []

        print(f"serial: {serial}")
        print(f"device time: {dt}")
        print(f"users count: {len(users)}")
        print(f"attendance logs count: {len(logs)}")

        # muestra 5 logs recientes (si el objeto expone timestamp)
        logs_sorted = sorted(
            [l for l in logs if getattr(l, "timestamp", None) is not None],
            key=lambda x: x.timestamp,
            reverse=True
        )
        for l in logs_sorted[:5]:
            print("log:", getattr(l, "user_id", None), getattr(l, "timestamp", None),
                  "punch=", getattr(l, "punch", None), "status=", getattr(l, "status", None))
    finally:
        try:
            conn.enable_device()
        except Exception:
            pass
        conn.disconnect()

if __name__ == "__main__":
    main()
