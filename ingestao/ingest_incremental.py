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

DATA_INICIAL_HISTORICO = "2026-01-30T00:00:00"

REQUEST_TIMEOUT = 30
MAX_WORKERS = 6
SLEEP_BETWEEN_CALLS = 0.05

TIPOS_VALIDOS = (
    "A-Axis Delta Angle",
    "B-Axis Delta Angle",
    "Air Temperature",
    "Device Temperature"
)

# ======================================================
# SESSION
# ======================================================
session = requests.Session()

retries = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429,500,502,503,504],
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

    conn=get_conn()
    cur=conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_state(
            sensor_id BIGINT PRIMARY KEY,
            last_timestamp TIMESTAMP
        );
    """)

    cur.execute("SELECT sensor_id,last_timestamp FROM sync_state;")
    rows=cur.fetchall()

    cur.close()
    release_conn(conn)

    mapa={}
    for sid,ts in rows:
        if ts:
            mapa[sid]=(ts-timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S")

    print(f"🧠 Sync_state carregado: {len(mapa)} sensores")
    return mapa

# ======================================================
# DEVICES + SENSORES (🔥 BLINDADO)
# ======================================================
def cadastrar_devices_e_sensores(token):

    conn=get_conn()
    cur=conn.cursor()

    print("📡 Atualizando devices...")

    r=session.get(
        f"{BASE_URL}/UserDevices",
        headers={"Authorization":f"Bearer {token}"},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()

    mapa_devices={}

    for device in r.json():

        cur.execute("""
            INSERT INTO devices(
                device_id,device_name,serial_number,status,
                latitude,longitude,last_upload,battery_percentage
            )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(device_id) DO UPDATE SET
                device_name=EXCLUDED.device_name,
                status=EXCLUDED.status,
                latitude=EXCLUDED.latitude,
                longitude=EXCLUDED.longitude,
                last_upload=EXCLUDED.last_upload,
                battery_percentage=EXCLUDED.battery_percentage;
        """,(
            device["deviceId"],
            device["deviceName"],
            device.get("serialNumber"),
            device.get("status"),
            device.get("latitude"),
            device.get("longitude"),
            device.get("lastUpload"),
            device.get("batteryPercentage")
        ))

        processar_alertas_status(conn,device["deviceId"],device.get("status"))

        sensores_validos=[]

        for sensor in device.get("sensors",[]):

            canal=str(sensor.get("channelNumber")).strip()
            tipo=(sensor.get("sensorType") or "").strip()

            # 🔥 FILTRO DEFINITIVO
            if canal not in ("1","2","3"):
                continue

            if tipo not in TIPOS_VALIDOS:
                continue

            sid=sensor["sensorId"]
            sensores_validos.append(sid)

            cur.execute("""
                INSERT INTO sensores(
                    sensor_id,device_id,nome_customizado,
                    tipo_sensor,unidade_medida
                )
                VALUES(%s,%s,%s,%s,%s)
                ON CONFLICT(sensor_id) DO UPDATE SET
                    device_id=EXCLUDED.device_id,
                    nome_customizado=EXCLUDED.nome_customizado,
                    tipo_sensor=EXCLUDED.tipo_sensor,
                    unidade_medida=EXCLUDED.unidade_medida;
            """,(
                sid,
                device["deviceId"],
                sensor.get("customName") or f"Sensor {sid}",
                tipo,
                sensor.get("uom")
            ))

        if sensores_validos:
            mapa_devices[device["deviceId"]]=sensores_validos

    conn.commit()
    cur.close()
    release_conn(conn)

    print(f"✅ Devices tiltímetro válidos: {len(mapa_devices)}")
    return mapa_devices

# ======================================================
# WORKER DEVICE
# ======================================================
def worker_device(token,device_id,sensor_ids,sync_map,agora):

    conn=get_conn()
    cur=conn.cursor()

    headers={"Authorization":f"Bearer {token}"}

    inicio=min([
        sync_map.get(s,DATA_INICIAL_HISTORICO)
        for s in sensor_ids
    ])

    sensor_param=",".join(map(str,sensor_ids))

    offset=0
    total_local=0

    print(f"🛰️ Device {device_id} iniciando")

    while True:

        r=session.get(
            f"{BASE_URL}/SensorData",
            headers=headers,
            params={
                "version":"1.3",
                "startDate":inicio,
                "endDate":agora,
                "offset":offset,
                "sensorIds":sensor_param
            },
            timeout=REQUEST_TIMEOUT
        )

        r.raise_for_status()
        dados=r.json()

        qtd=len(dados)

        if qtd==0:
            break

        registros=[
            (d["sensorId"],d["readingDate"],d["sensorValue"])
            for d in dados
        ]

        execute_batch(cur,"""
            INSERT INTO leituras(sensor_id,data_leitura,valor_sensor)
            VALUES(%s,%s,%s)
            ON CONFLICT(sensor_id,data_leitura) DO NOTHING
        """,registros,page_size=500)

        execute_batch(cur,"""
            INSERT INTO sync_state(sensor_id,last_timestamp)
            VALUES(%s,%s)
            ON CONFLICT(sensor_id)
            DO UPDATE SET last_timestamp=EXCLUDED.last_timestamp
        """,[(r[0],r[1]) for r in registros])

        conn.commit()

        total_local+=qtd
        offset+=1

        print(f"📡 Device {device_id} offset {offset}")

        time.sleep(SLEEP_BETWEEN_CALLS)

    cur.close()
    release_conn(conn)

    return total_local

# ======================================================
# INGESTÃO
# ======================================================
def baixar_e_salvar_leituras(token,mapa_devices):

    sync_map=carregar_sync_state()
    agora=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    total=0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures=[]

        for device_id,sensors in mapa_devices.items():

            futures.append(
                executor.submit(
                    worker_device,
                    token,
                    device_id,
                    sensors,
                    sync_map,
                    agora
                )
            )

        for f in as_completed(futures):
            total+=f.result()
            print(f"🌌 TOTAL GLOBAL: {total}")

# ======================================================
# MAIN
# ======================================================
if __name__=="__main__":

    print("🚀 ORION COSMIC ENGINE V4 START")

    token=obter_token()
    mapa_devices=cadastrar_devices_e_sensores(token)
    baixar_e_salvar_leituras(token,mapa_devices)

    print("\n🏁 FINALIZADO")
