import os
import requests
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
import time

# ======================================================
# CONFIG
# ======================================================

API_KEY = os.getenv("API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

BASE_URL = "https://api.oriondata.io/api"

REQUEST_TIMEOUT = 30
MAX_WORKERS = 4

# ======================================================
# RATE LIMIT
# ======================================================

API_MIN_INTERVAL = 0.6
api_lock = threading.Lock()
ultimo_request = 0


def aguardar_rate_limit():

    global ultimo_request

    with api_lock:

        agora = time.time()
        delta = agora - ultimo_request

        if delta < API_MIN_INTERVAL:
            time.sleep(API_MIN_INTERVAL - delta)

        ultimo_request = time.time()


# ======================================================
# SESSION
# ======================================================

session = requests.Session()

retries = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[500,502,503,504],
    allowed_methods=["GET"]
)

adapter = HTTPAdapter(max_retries=retries)

session.mount("https://", adapter)


# ======================================================
# DB POOL
# ======================================================

db_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=MAX_WORKERS+2,
    dsn=DATABASE_URL
)

pool_lock = threading.Lock()


def get_conn():
    with pool_lock:
        return db_pool.getconn()


def release_conn(conn):
    with pool_lock:
        db_pool.putconn(conn)


# ======================================================
# TOKEN
# ======================================================

def obter_token():

    print("🔐 Obtendo token...")

    aguardar_rate_limit()

    r = session.get(
        f"{BASE_URL}/token",
        params={"apiKey": API_KEY},
        timeout=REQUEST_TIMEOUT
    )

    r.raise_for_status()

    print("✅ Token obtido")

    return r.json()["token"]


# ======================================================
# DEVICES
# ======================================================

def cadastrar_devices(token):

    conn = get_conn()
    cur = conn.cursor()

    print("📡 Atualizando devices...")

    aguardar_rate_limit()

    r = session.get(
        f"{BASE_URL}/UserDevices",
        headers={"Authorization": f"Bearer {token}"},
        timeout=REQUEST_TIMEOUT
    )

    r.raise_for_status()

    devices = r.json()

    print(f"📊 Devices recebidos da API: {len(devices)}")

    for device in devices:

        reference = device.get("reference")

        print(
            f"DEBUG DEVICE | "
            f"id={device['deviceId']} "
            f"name={device.get('deviceName')} "
            f"reference={reference}"
        )

        cur.execute("""
            INSERT INTO devices(
                device_id,
                device_name,
                serial_number,
                status,
                latitude,
                longitude,
                last_upload,
                battery_percentage,
                reference
            )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(device_id) DO UPDATE SET
                device_name = EXCLUDED.device_name,
                serial_number = EXCLUDED.serial_number,
                status = EXCLUDED.status,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                last_upload = EXCLUDED.last_upload,
                battery_percentage = EXCLUDED.battery_percentage,
                reference = EXCLUDED.reference;
        """,(
            device["deviceId"],
            device.get("deviceName"),
            device.get("serialNumber"),
            device.get("status"),
            device.get("latitude"),
            device.get("longitude"),
            device.get("lastUpload"),
            device.get("batteryPercentage"),
            reference
        ))

        print(
            f"💾 Device salvo | id={device['deviceId']} reference={reference}"
        )

    conn.commit()

    cur.close()
    release_conn(conn)

    print("✅ Devices sincronizados")


# ======================================================
# MAIN
# ======================================================

if __name__ == "__main__":

    print("🚀 ORION INGEST ENGINE START")

    token = obter_token()

    cadastrar_devices(token)

    print("🏁 FINALIZADO")
