import os
import requests
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
import time
from concurrent.futures import ThreadPoolExecutor

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
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET"]
)

adapter = HTTPAdapter(max_retries=retries)

session.mount("https://", adapter)

# ======================================================
# DB POOL
# ======================================================

db_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=MAX_WORKERS + 2,
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

    print(f"📊 Devices recebidos: {len(devices)}")

    for device in devices:

        reference = device.get("reference")

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

    conn.commit()

    cur.close()
    release_conn(conn)

    print("✅ Devices sincronizados")


# ======================================================
# ULTIMA DATA
# ======================================================

def obter_ultima_data():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT MAX(timestamp)
        FROM device_data
    """)

    result = cur.fetchone()

    cur.close()
    release_conn(conn)

    if result[0] is None:
        return datetime.utcnow() - timedelta(days=30)

    return result[0]


# ======================================================
# BUSCAR DADOS
# ======================================================

def buscar_dados(token, device_id, data_inicio):

    aguardar_rate_limit()

    r = session.get(
        f"{BASE_URL}/DeviceData",
        headers={"Authorization": f"Bearer {token}"},
        params={
            "deviceId": device_id,
            "from": data_inicio.isoformat()
        },
        timeout=REQUEST_TIMEOUT
    )

    r.raise_for_status()

    return r.json()


# ======================================================
# SALVAR DADOS
# ======================================================

def salvar_dados(device_id, dados):

    if not dados:
        return

    conn = get_conn()
    cur = conn.cursor()

    for d in dados:

        cur.execute("""
            INSERT INTO device_data(
                device_id,
                timestamp,
                parameter,
                value
            )
            VALUES(%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """,(
            device_id,
            d.get("timestamp"),
            d.get("parameter"),
            d.get("value")
        ))

    conn.commit()

    cur.close()
    release_conn(conn)


# ======================================================
# PROCESSAR DEVICE
# ======================================================

def processar_device(token, device_id, data_inicio):

    print(f"⬇️ Baixando dados device {device_id}")

    dados = buscar_dados(token, device_id, data_inicio)

    salvar_dados(device_id, dados)

    print(f"✅ Device {device_id} atualizado")


# ======================================================
# LISTAR DEVICES
# ======================================================

def listar_devices():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT device_id
        FROM devices
    """)

    rows = cur.fetchall()

    cur.close()
    release_conn(conn)

    return [r[0] for r in rows]


# ======================================================
# MAIN
# ======================================================

if __name__ == "__main__":

    print("🚀 ORION INGEST ENGINE START")

    token = obter_token()

    cadastrar_devices(token)

    data_inicio = obter_ultima_data()

    print(f"📅 Buscando dados desde {data_inicio}")

    devices = listar_devices()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = []

        for device_id in devices:

            futures.append(
                executor.submit(
                    processar_device,
                    token,
                    device_id,
                    data_inicio
                )
            )

        for f in futures:
            f.result()

    print("🏁 INGEST FINALIZADO")
