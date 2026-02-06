import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from alert_engine import processar_alertas_status
import time

# ======================================================
# CONFIGURAÇÕES GERAIS
# ======================================================
API_KEY = "QLClykq5tBlhO9ebP5PLDzKvYkyo2p3LlTVhqPxirRY="
BASE_URL = "https://api.oriondata.io/api"

DATABASE_URL = os.getenv("DATABASE_URL")

MODO_HISTORICO = False
DATA_INICIAL_HISTORICO = "2026-01-15T00:00:00"
JANELA_HORAS = 1

BACKFILL_EM_BLOCOS = True
BLOCO_DIAS = 7

REQUEST_TIMEOUT = 30
SENSOR_BATCH_SIZE = 50
SLEEP_BETWEEN_CALLS = 0.3

# ======================================================
# SESSION COM RETRY
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
# TOKEN
# ======================================================
def obter_token():
    r = session.get(
        f"{BASE_URL}/token",
        params={"apiKey": API_KEY},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    return r.json()["token"]

# ======================================================
# DEVICES E SENSORES
# ======================================================
def cadastrar_devices_e_sensores(token, conn):

    r = session.get(
        f"{BASE_URL}/UserDevices",
        headers={"Authorization": f"Bearer {token}"},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()

    cur = conn.cursor()
    sensor_ids = []

    for device in r.json():

        # ======================================================
        # INSERT DEVICE
        # ======================================================
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

        # ======================================================
        # 🔥 ALERTA DE MUDANÇA DE STATUS
        # ======================================================
        processar_alertas_status(
            conn,
            device["deviceId"],
            device.get("status")
        )

        # ======================================================
        # SENSORES
        # ======================================================
        for sensor in device.get("sensors", []):
            sensor_ids.append(sensor["sensorId"])

            cur.execute("""
                INSERT INTO sensores (
                    sensor_id, device_id, nome_customizado,
                    tipo_sensor, unidade_medida
                )
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (sensor_id) DO UPDATE SET
                    device_id = EXCLUDED.device_id,
                    nome_customizado = EXCLUDED.nome_customizado,
                    tipo_sensor = EXCLUDED.tipo_sensor,
                    unidade_medida = EXCLUDED.unidade_medida;
            """, (
                sensor["sensorId"],
                device["deviceId"],
                sensor.get("customName") or f"Sensor {sensor['sensorId']}",
                sensor.get("sensorType"),
                sensor.get("uom")
            ))

    conn.commit()
    cur.close()

    return sorted(set(sensor_ids))

# ======================================================
# GERADOR DE BLOCOS DE TEMPO
# ======================================================
def gerar_blocos_tempo(data_inicio_str, data_fim_dt, dias_bloco):

    inicio = datetime.fromisoformat(
        data_inicio_str.replace("Z", "")
    ).replace(tzinfo=timezone.utc)

    while inicio < data_fim_dt:
        fim = min(inicio + timedelta(days=dias_bloco), data_fim_dt)
        yield (
            inicio.strftime("%Y-%m-%dT%H:%M:%S"),
            fim.strftime("%Y-%m-%dT%H:%M:%S")
        )
        inicio = fim

# ======================================================
# JANELA TEMPO REAL
# ======================================================
def calcular_janela_tempo():
    agora = datetime.now(timezone.utc)
    data_fim = agora.strftime("%Y-%m-%dT%H:%M:%S")
    data_inicio = (agora - timedelta(hours=JANELA_HORAS)).strftime("%Y-%m-%dT%H:%M:%S")
    return data_inicio, data_fim

# ======================================================
# INGESTÃO PRINCIPAL
# ======================================================
def baixar_e_salvar_leituras(token, sensor_ids, conn):

    headers = {"Authorization": f"Bearer {token}"}
    cur = conn.cursor()

    agora = datetime.now(timezone.utc)

    if MODO_HISTORICO and BACKFILL_EM_BLOCOS:
        janelas = list(
            gerar_blocos_tempo(DATA_INICIAL_HISTORICO, agora, BLOCO_DIAS)
        )
    else:
        janelas = [calcular_janela_tempo()]

    print(f"🧱 Total de blocos: {len(janelas)}")

    for idx, (data_inicio, data_fim) in enumerate(janelas, start=1):

        print(f"\n🧱 Bloco {idx}/{len(janelas)} | {data_inicio} → {data_fim}")

        for i in range(0, len(sensor_ids), SENSOR_BATCH_SIZE):

            lote = sensor_ids[i:i + SENSOR_BATCH_SIZE]
            sensor_param = ",".join(map(str, lote))

            offset = 0

            while True:

                r = session.get(
                    f"{BASE_URL}/SensorData",
                    headers=headers,
                    params={
                        "version": "1.3",
                        "startDate": data_inicio,
                        "endDate": data_fim,
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
                """, registros, page_size=500)

                conn.commit()

                offset += 1
                time.sleep(SLEEP_BETWEEN_CALLS)

    cur.close()

# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":

    print("🚀 Iniciando sincronização")

    conn = psycopg2.connect(DATABASE_URL)

    token = obter_token()

    sensor_ids = cadastrar_devices_e_sensores(token, conn)

    baixar_e_salvar_leituras(token, sensor_ids, conn)

    conn.close()

    print("\n🏁 Processo finalizado com sucesso")
