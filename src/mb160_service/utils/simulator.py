# simulator.py
import os
import random
from datetime import datetime, timedelta

def simulated_attendance_batch():
    serial = os.environ.get("SIMULATED_DEVICE_SERIAL", "SIM-MB160-001")
    users = os.environ.get("SIMULATED_USERS", "1001,1002").split(",")

    now = datetime.now()
    batch = []
    for _ in range(random.randint(3, 10)):
        user_id = random.choice(users).strip()
        ts = now - timedelta(seconds=random.randint(0, 120))
        punch = random.choice([0, 1])
        status = 0
        workcode = None
        batch.append({
            "device_serial": serial,
            "user_id": user_id,
            "timestamp": ts,
            "punch": punch,
            "status": status,
            "workcode": workcode,
        })
    return batch
