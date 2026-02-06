import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from alert_engine import processar_alertas_status
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

# ======================================================
# CONFIG
# ======================================================
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.oriondata.io/api"
DATABASE_URL = os.getenv("DATABASE_URL")

DATA_INICIAL_HISTORICO = "2026-01-01T00:00:00"

REQUEST_TIMEOUT = 30
SENSOR_BATCH_SIZE = 50
MAX_WORKERS = 8
SLEEP_BETWEEN_CALLS = 0.03

# ======================================================
# HTTP SESSION GLOBAL
# ======================================================
session = requests.Session()

retries = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)

adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)

# ======================================================
# CONNECTION POOL (🔥 DIFERENCIAL)
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
    r = session.get(
        f"{BASE_URL}/token",
        params={"apiKey": API_KEY},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    print("✅ Token obtido")
    return r.json()["token"]

# ======================================================
# SYNC STATE
# ======================================================
def carregar_sync_state():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_state (
            sensor_id BIGINT PRIMARY KEY,
            last_timestamp TIMESTAMP
        );
    """)

    cur.execute("SELECT sensor_id, last_timestamp FROM sync_state;")
    dados = cur.fetchall()

    cur.close()
    release_conn(conn)

    mapa = {}

    for sid, ts in dados:
        if ts:
            mapa[sid] = (ts - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S")

    print(f"🧠 Sync_state carregado: {len(mapa)} sensores")

    return mapa

# ======================================================
# DEVICES
# ======================================================
def cadastrar_devices_e_sensores(token):

    conn = get_conn()
    cur = conn.cursor()

    r = session.get(
        f"{BASE_URL}/UserDevices",
        headers={"Authorization": f"Bearer {token}"},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()

    sensor_ids = []

    for device in r.json():

        cur.execute("""
            INSERT INTO devices (
                device_id, device_name, serial_number, status,
                latitude, longitude, last_upload, battery_percentage
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (device_id) DO UPDATE SET
                device_name = EXCLUDED.device_name,
                status = EXCLUDED.status,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                last_upload = EXCLUDED.last_upload,
                battery_percentage = EXCLUDED.battery_percentage;
        """, (
            device["deviceId"],
            device["deviceName"],
            device.get("serialNumber"),
            device.get("status"),
            device.get("latitude"),
            device.get("longitude"),
            device.get("lastUpload"),
            device.get("batteryPercentage")
        ))

        processar_alertas_status(
            conn,
            device["deviceId"],
            device.get("status")
        )

        for sensor in device.get("sensors", []):
            sensor_ids.append(sensor["sensorId"])

    conn.commit()
    cur.close()
    release_conn(conn)

    print(f"✅ Sensores encontrados: {len(sensor_ids)}")

    return sorted(set(sensor_ids))

# ======================================================
# WORKER GALÁCTICO
# ======================================================
def worker_download_insert(token, lote, inicio_lote, fim):

    conn = get_conn()
    cur = conn.cursor()

    headers = {"Authorization": f"Bearer {token}"}
    sensor_param = ",".join(map(str, lote))

    offset = 0
    total_local = 0

    while True:

        r = session.get(
            f"{BASE_URL}/SensorData",
            headers=headers,
            params={
                "version": "1.3",
                "startDate": inicio_lote,
                "endDate": fim,
                "offset": offset,
                "sensorIds": sensor_param
            },
            timeout=REQUEST_TIMEOUT
        )

        r.raise_for_status()
        dados = r.json()

        if not dados:
            break

        registros = [
            (d["sensorId"], d["readingDate"], d["sensorValue"])
            for d in dados
        ]

        execute_batch(cur, """
            INSERT INTO leituras (
                sensor_id,
                data_leitura,
                valor_sensor
            )
            VALUES (%s,%s,%s)
            ON CONFLICT (sensor_id, data_leitura) DO NOTHING
        """, registros, page_size=1500)

        execute_batch(cur, """
            INSERT INTO sync_state(sensor_id, last_timestamp)
            VALUES (%s,%s)
            ON CONFLICT(sensor_id)
            DO UPDATE SET last_timestamp = EXCLUDED.last_timestamp
        """, [(r[0], r[1]) for r in registros])

        conn.commit()

        total_local += len(registros)
        offset += len(dados)

        print(f"⚡ Worker {lote[0]}.. +{len(registros)}")

        time.sleep(SLEEP_BETWEEN_CALLS)

    cur.close()
    release_conn(conn)

    return total_local

# ======================================================
# INGESTÃO GALÁCTICA
# ======================================================
def baixar_e_salvar_leituras(token, sensor_ids):

    sync_map = carregar_sync_state()

    agora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    lotes = [
        sensor_ids[i:i + SENSOR_BATCH_SIZE]
        for i in range(0, len(sensor_ids), SENSOR_BATCH_SIZE)
    ]

    total = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = []

        for lote in lotes:

            inicio_lote = min(
                sync_map.get(s, DATA_INICIAL_HISTORICO)
                for s in lote
            )

            futures.append(
                executor.submit(
                    worker_download_insert,
                    token,
                    lote,
                    inicio_lote,
                    agora
                )
            )

        for future in as_completed(futures):
            total += future.result()
            print(f"📊 TOTAL GLOBAL: {total}")

# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":

    print("🚀 ORION GALACTIC INGESTION START")

    token = obter_token()

    sensor_ids = cadastrar_devices_e_sensores(token)

    baixar_e_salvar_leituras(token, sensor_ids)

    print("\n🏁 FINALIZADO")
