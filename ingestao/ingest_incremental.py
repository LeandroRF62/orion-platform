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
# CONFIGURAÇÕES
# ======================================================
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.oriondata.io/api"
DATABASE_URL = os.getenv("DATABASE_URL")

DATA_INICIAL_HISTORICO = "2026-01-01T00:00:00"

REQUEST_TIMEOUT = 30
MAX_WORKERS = 8
SLEEP_BETWEEN_CALLS = 0.05

# ======================================================
# SESSION HTTP GLOBAL
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
# CONNECTION POOL POSTGRES
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
# DEVICES E SENSORES
# ======================================================
def cadastrar_devices_e_sensores(token):

    conn = get_conn()
    cur = conn.cursor()

    print("📡 Atualizando devices...")

    r = session.get(
        f"{BASE_URL}/UserDevices",
        headers={"Authorization": f"Bearer {token}"},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()

    sensor_ids = []

    for device in r.json():

        # 🔥 INSERT DEVICE
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

        # 🔥 INSERT SENSORES (CANAIS 1,2,3)
        for sensor in device.get("sensors", []):

            channel_number = str(sensor.get("channelNumber")).strip()

            if channel_number not in ("1", "2", "3"):
                continue

            sensor_id = sensor["sensorId"]
            sensor_ids.append(sensor_id)

            cur.execute("""
                INSERT INTO sensores (
                    sensor_id,
                    device_id,
                    nome_customizado,
                    tipo_sensor,
                    unidade_medida
                )
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (sensor_id) DO UPDATE SET
                    device_id = EXCLUDED.device_id,
                    nome_customizado = EXCLUDED.nome_customizado,
                    tipo_sensor = EXCLUDED.tipo_sensor,
                    unidade_medida = EXCLUDED.unidade_medida;
            """, (
                sensor_id,
                device["deviceId"],
                sensor.get("customName") or f"Sensor {sensor_id}",
                sensor.get("sensorType"),
                sensor.get("uom")
            ))

    conn.commit()
    cur.close()
    release_conn(conn)

    print(f"✅ Sensores tiltímetro válidos: {len(sensor_ids)}")

    return sorted(set(sensor_ids))

# ======================================================
# WORKER SENSOR
# ======================================================
def worker_sensor(token, sensor_id, inicio, fim):

    conn = get_conn()
    cur = conn.cursor()

    headers = {"Authorization": f"Bearer {token}"}

    offset = 0
    total_local = 0

    print(f"🛰️ Sensor {sensor_id} iniciando em {inicio}")

    while True:

        r = session.get(
            f"{BASE_URL}/SensorData",
            headers=headers,
            params={
                "version": "1.3",
                "startDate": inicio,
                "endDate": fim,
                "offset": offset,
                "sensorIds": sensor_id
            },
            timeout=REQUEST_TIMEOUT
        )

        r.raise_for_status()
        dados = r.json()

        qtd = len(dados)

        if qtd == 0:
            print(f"✅ Sensor {sensor_id} finalizado | Total: {total_local}")
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
        """, registros, page_size=500)

        execute_batch(cur, """
            INSERT INTO sync_state(sensor_id, last_timestamp)
            VALUES (%s,%s)
            ON CONFLICT(sensor_id)
            DO UPDATE SET last_timestamp = EXCLUDED.last_timestamp
        """, [(r[0], r[1]) for r in registros])

        conn.commit()

        total_local += qtd
        offset += qtd

        print(f"📡 Sensor {sensor_id} offset {offset}")

        time.sleep(SLEEP_BETWEEN_CALLS)

    cur.close()
    release_conn(conn)

    return total_local

# ======================================================
# INGESTÃO
# ======================================================
def baixar_e_salvar_leituras(token, sensor_ids):

    sync_map = carregar_sync_state()

    agora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    total = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = []

        for sensor_id in sensor_ids:

            inicio = sync_map.get(sensor_id, DATA_INICIAL_HISTORICO)

            futures.append(
                executor.submit(
                    worker_sensor,
                    token,
                    sensor_id,
                    inicio,
                    agora
                )
            )

        for future in as_completed(futures):
            total += future.result()
            print(f"🌌 TOTAL GLOBAL: {total}")

# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":

    print("🚀 ORION COSMIC ENGINE START")

    token = obter_token()

    sensor_ids = cadastrar_devices_e_sensores(token)

    baixar_e_salvar_leituras(token, sensor_ids)

    print("\n🏁 FINALIZADO")
