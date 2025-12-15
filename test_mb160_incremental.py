import os
from datetime import datetime
from dotenv import load_dotenv
from zk import ZK

load_dotenv()

IP = os.environ.get("MB160_IP", "192.168.2.13")
PORT = int(os.environ.get("MB160_PORT", "4370"))

# Simula “último timestamp guardado”
LAST_TS = datetime(2025, 12, 1, 0, 0, 0)

def main():
    zk = ZK(IP, port=PORT, timeout=10, password=0)
    conn = zk.connect()
    try:
        logs = conn.get_attendance() or []
        newer = [l for l in logs if getattr(l, "timestamp", None) and l.timestamp > LAST_TS]
        newer.sort(key=lambda x: x.timestamp)

        print(f"total logs: {len(logs)}")
        print(f"newer than {LAST_TS}: {len(newer)}")
        for l in newer[:10]:
            print(getattr(l, "user_id", None), l.timestamp, getattr(l, "punch", None), getattr(l, "status", None))
    finally:
        conn.disconnect()

if __name__ == "__main__":
    main()
