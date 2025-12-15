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
        users = conn.get_users() or []
        print("users:", len(users))
        for u in users[:10]:
            print("----")
            print("repr:", u)
            print("uid:", getattr(u, "uid", None))
            print("user_id:", getattr(u, "user_id", None))
            print("name:", getattr(u, "name", None))
            print("privilege:", getattr(u, "privilege", None))
            print("card:", getattr(u, "card", None))
    finally:
        conn.disconnect()

if __name__ == "__main__":
    main()
