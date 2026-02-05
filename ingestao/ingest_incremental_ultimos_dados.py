import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

# ======================================================
# CONFIG
# ======================================================
API_KEY = "QLClykq5tBlhO9ebP5PLDzKvYkyo2p3LlTVhqPxirRY="
BASE_URL = "https://api.oriondata.io/api"
DATABASE_URL = os.getenv("DATABASE_URL")

REQUEST_TIMEOUT = 30
SENSOR_BATCH_SIZE = 50
SLEEP_BETWEEN_CALLS = 0.3

# fallback caso device não tenha last_upload
FALLBACK_HORAS = 6

# ======================================================
# SESSION COM RETRY
# ======================================================
session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429,500,502,503,504],
    allowed_methods=["GET"]
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://",adapter)

# ======================================================
# TOKEN
# ======================================================
def obter_token():
    r=session.get(
        f"{BASE_URL}/token",
        params={"apiKey":API_KEY},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    return r.json()["token"]

# ======================================================
# 🔥 BUSCAR DEVICES NO BANCO
# ======================================================
def obter_devices_db(conn):

    cur=conn.cursor()

    cur.execute("""
        SELECT device_id,last_upload
        FROM devices
    """)

    rows=cur.fetchall()
    cur.close()

    return rows

# ======================================================
# 🔥 BUSCAR SENSORES DO DEVICE
# ======================================================
def obter_sensores_device(conn,device_id):

    cur=conn.cursor()

    cur.execute("""
        SELECT sensor_id
        FROM sensores
        WHERE device_id=%s
    """,(device_id,))

    sensores=[r[0] for r in cur.fetchall()]
    cur.close()

    return sensores

# ======================================================
# 🔥 CALCULAR JANELA POR DEVICE
# ======================================================
def calcular_janela(last_upload):

    agora=datetime.now(timezone.utc)

    if last_upload:
        inicio=last_upload.replace(tzinfo=timezone.utc)
    else:
        inicio=agora-timedelta(hours=FALLBACK_HORAS)

    return (
        inicio.strftime("%Y-%m-%dT%H:%M:%S"),
        agora.strftime("%Y-%m-%dT%H:%M:%S")
    )

# ======================================================
# 🔥 BAIXAR LEITURAS POR DEVICE
# ======================================================
def baixar_device(token,device_id,last_upload,conn):

    sensores=obter_sensores_device(conn,device_id)

    if not sensores:
        print(f"⚠️ Device {device_id} sem sensores")
        return

    data_inicio,data_fim=calcular_janela(last_upload)

    print(f"\n📡 Device {device_id}")
    print(f"🕒 {data_inicio} → {data_fim}")

    headers={"Authorization":f"Bearer {token}"}
    cur=conn.cursor()

    for i in range(0,len(sensores),SENSOR_BATCH_SIZE):

        lote=sensores[i:i+SENSOR_BATCH_SIZE]
        sensor_param=",".join(map(str,lote))

        offset=0

        while True:

            r=session.get(
                f"{BASE_URL}/SensorData",
                headers=headers,
                params={
                    "version":"1.3",
                    "startDate":data_inicio,
                    "endDate":data_fim,
                    "offset":offset,
                    "sensorIds":sensor_param
                },
                timeout=REQUEST_TIMEOUT
            )

            r.raise_for_status()
            dados=r.json()

            if not dados:
                break

            registros=[
                (d["sensorId"],d["readingDate"],d["sensorValue"])
                for d in dados
            ]

            execute_batch(cur,"""
                INSERT INTO leituras (
                    sensor_id,
                    data_leitura,
                    valor_sensor
                )
                VALUES (%s,%s,%s)
                ON CONFLICT (sensor_id,data_leitura) DO NOTHING
            """,registros,page_size=500)

            conn.commit()

            offset+=1
            time.sleep(SLEEP_BETWEEN_CALLS)

    cur.close()

# ======================================================
# MAIN
# ======================================================
if __name__=="__main__":

    print("🚀 Ingestão incremental por device")

    conn=psycopg2.connect(DATABASE_URL)

    token=obter_token()

    devices=obter_devices_db(conn)

    print(f"📦 Total devices: {len(devices)}")

    for device_id,last_upload in devices:
        baixar_device(token,device_id,last_upload,conn)

    conn.close()

    print("\n🏁 Finalizado")
