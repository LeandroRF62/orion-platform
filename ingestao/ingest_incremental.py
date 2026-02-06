import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from alert_engine import processar_alertas_status
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# ======================================================
# CONFIGURAÇÕES
# ======================================================
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.oriondata.io/api"
DATABASE_URL = os.getenv("DATABASE_URL")

DATA_INICIAL_HISTORICO = "2026-01-25T00:00:00"

BACKFILL_EM_BLOCOS = True
BLOCO_DIAS = 7

REQUEST_TIMEOUT = 30
SENSOR_BATCH_SIZE = 50
SLEEP_BETWEEN_CALLS = 0.1
MAX_WORKERS = 5

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
# DESCOBRIR ÚLTIMO TIMESTAMP
# ======================================================
def obter_ultimo_timestamp(conn):

    cur = conn.cursor()
    cur.execute("SELECT MAX(data_leitura) FROM leituras;")
    resultado = cur.fetchone()[0]
    cur.close()

    if resultado:
        ultimo = resultado - timedelta(minutes=5)

        print(f"🧠 Último timestamp no banco: {resultado}")
        print(f"▶️ Reiniciando a partir de: {ultimo}")

        return ultimo.strftime("%Y-%m-%dT%H:%M:%S")

    else:
        print("⚠️ Banco vazio. Usando DATA_INICIAL_HISTORICO")
        return DATA_INICIAL_HISTORICO

# ======================================================
# DEVICES E SENSORES
# ======================================================
def cadastrar_devices_e_sensores(token, conn):

    print("📡 Atualizando devices e sensores...")

    r = session.get(
        f"{BASE_URL}/UserDevices",
        headers={"Authorization": f"Bearer {token}"},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()

    cur = conn.cursor()
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

    print(f"✅ Sensores encontrados: {len(sensor_ids)}")

    return sorted(set(sensor_ids))

# ======================================================
# GERADOR DE BLOCOS
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
# DOWNLOAD PARALELO
# ======================================================
def baixar_lote(token, data_inicio, data_fim, lote):

    headers = {"Authorization": f"Bearer {token}"}
    sensor_param = ",".join(map(str, lote))
    offset = 0
    registros_total = []

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

        registros_total.extend([
            (d["sensorId"], d["readingDate"], d["sensorValue"])
            for d in dados
        ])

        offset += len(dados)
        time.sleep(SLEEP_BETWEEN_CALLS)

    return registros_total

# ======================================================
# INGESTÃO TURBO COM TIMESTAMP AUTOMÁTICO
# ======================================================
def baixar_e_salvar_leituras(token, sensor_ids, conn):

    cur = conn.cursor()
    agora = datetime.now(timezone.utc)

    data_inicio_auto = obter_ultimo_timestamp(conn)

    janelas = list(
        gerar_blocos_tempo(data_inicio_auto, agora, BLOCO_DIAS)
    )

    print(f"🧱 Total de blocos: {len(janelas)}")

    total_registros = 0

    for idx, (data_inicio, data_fim) in enumerate(janelas, start=1):

        print(f"\n🧱 Bloco {idx}/{len(janelas)} | {data_inicio} → {data_fim}")

        lotes = [
            sensor_ids[i:i + SENSOR_BATCH_SIZE]
            for i in range(0, len(sensor_ids), SENSOR_BATCH_SIZE)
        ]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

            futures = [
                executor.submit(baixar_lote, token, data_inicio, data_fim, lote)
                for lote in lotes
            ]

            for future in as_completed(futures):

                registros = future.result()

                if not registros:
                    continue

                execute_batch(cur, """
                    INSERT INTO leituras (
                        sensor_id,
                        data_leitura,
                        valor_sensor
                    )
                    VALUES (%s,%s,%s)
                    ON CONFLICT (sensor_id, data_leitura) DO NOTHING
                """, registros, page_size=1000)

                conn.commit()

                total_registros += len(registros)

                print(f"📥 Inseridos agora: {len(registros)} | Total geral: {total_registros}")

    cur.close()

# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":

    print("🚀 Iniciando sincronização ORION TURBO")

    conn = psycopg2.connect(DATABASE_URL)

    token = obter_token()

    sensor_ids = cadastrar_devices_e_sensores(token, conn)

    baixar_e_salvar_leituras(token, sensor_ids, conn)

    conn.close()

    print("\n🏁 Processo finalizado com sucesso")
