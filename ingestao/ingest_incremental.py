import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import argparse

# ======================================================
# CONFIG
# ======================================================

API_KEY      = os.getenv("API_KEY")
BASE_URL     = "https://api.oriondata.io/api"
DATABASE_URL = os.getenv("DATABASE_URL")

# Modo normal: incremental a partir da última leitura real
# Modo gap-fill: varre desde DATA_GAP_FILL para detectar buracos
DATA_INICIAL_HISTORICO = "2026-01-01T00:00:00"
DATA_GAP_FILL          = "2026-01-01T00:00:00"

REQUEST_TIMEOUT = 45
MAX_WORKERS     = 4
PAGE_SIZE       = 500

TIPOS_VALIDOS = (
    "A-Axis Delta Angle",
    "B-Axis Delta Angle",
    "Air Temperature",
    "Device Temperature",
)

API_MIN_INTERVAL = 0.5
api_lock         = threading.Lock()
ultimo_request   = 0

def aguardar_rate_limit():
    global ultimo_request
    with api_lock:
        agora = time.time()
        delta = agora - ultimo_request
        if delta < API_MIN_INTERVAL:
            time.sleep(API_MIN_INTERVAL - delta)
        ultimo_request = time.time()

# ======================================================
# SESSION HTTP
# ======================================================

session = requests.Session()
retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# ======================================================
# CONNECTION POOL
# ======================================================

db_pool   = SimpleConnectionPool(minconn=1, maxconn=MAX_WORKERS + 4, dsn=DATABASE_URL)
pool_lock = threading.Lock()

def get_conn():
    with pool_lock:
        return db_pool.getconn()

def release_conn(conn):
    with pool_lock:
        db_pool.putconn(conn)

# ======================================================
# SCHEMA
# ======================================================

def garantir_schema():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("ALTER TABLE devices ADD COLUMN IF NOT EXISTS reference TEXT;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_state (
            sensor_id      BIGINT PRIMARY KEY,
            last_timestamp TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    release_conn(conn)

# ======================================================
# CURSOR POR SENSOR — fonte real: tabela leituras
# ======================================================

def carregar_cursores(sensor_ids: list, gap_fill: bool = False) -> dict:
    """
    Retorna {sensor_id: "YYYY-MM-DDTHH:MM:SS"} com o ponto de partida
    para cada sensor.

    Modo normal  (gap_fill=False):
      → usa MAX(data_leitura) de cada sensor em `leituras`, menos 1h de margem.
        Se não há leitura, usa DATA_INICIAL_HISTORICO.

    Modo gap-fill (gap_fill=True):
      → força todos os sensores a iniciarem em DATA_GAP_FILL,
        independentemente do que já existe no banco.
        O ON CONFLICT DO NOTHING na inserção garante que dados duplicados
        sejam ignorados — apenas os buracos serão preenchidos.
    """
    if gap_fill:
        print(f"🔍 Modo GAP-FILL: todos os sensores serão varridos desde {DATA_GAP_FILL}")
        return {sid: DATA_GAP_FILL for sid in sensor_ids}

    if not sensor_ids:
        return {}

    conn = get_conn()
    cur  = conn.cursor()
    placeholders = ",".join(["%s"] * len(sensor_ids))
    cur.execute(
        f"SELECT sensor_id, MAX(data_leitura) FROM leituras "
        f"WHERE sensor_id IN ({placeholders}) GROUP BY sensor_id",
        sensor_ids,
    )
    rows = {sid: ts for sid, ts in cur.fetchall()}
    cur.close()
    release_conn(conn)

    cursores = {}
    for sid in sensor_ids:
        ts = rows.get(sid)
        if ts:
            cursores[sid] = (ts - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")
        else:
            cursores[sid] = DATA_INICIAL_HISTORICO
    return cursores

# ======================================================
# WORKER POR SENSOR  ← mudança principal
# ======================================================

def worker_sensor(token, device_id, sensor_id, inicio, agora):
    """
    Baixa e salva leituras de UM único sensor com paginação completa.

    Ao usar um sensor por worker:
    - O cursor é exato para aquele sensor
    - O offset não é contaminado por dados de outros sensores
    - Gaps individuais são detectados e preenchidos corretamente
    """
    conn    = get_conn()
    cur     = conn.cursor()
    headers = {"Authorization": f"Bearer {token}"}

    current_offset = 0
    total_sensor   = 0

    while True:
        aguardar_rate_limit()

        try:
            r = session.get(
                f"{BASE_URL}/SensorData",
                headers=headers,
                params={
                    "version":   "1.3",
                    "startDate": inicio,
                    "endDate":   agora,
                    "offset":    current_offset,
                    "limit":     PAGE_SIZE,
                    "sensorIds": str(sensor_id),   # ← UM sensor por request
                },
                timeout=REQUEST_TIMEOUT,
            )
            r.raise_for_status()
            dados = r.json()
        except Exception as e:
            print(f"  ⚠️  sensor {sensor_id} (dev {device_id}) offset={current_offset}: {e}")
            break

        qtd = len(dados)
        if qtd == 0:
            break

        registros = [
            (d["sensorId"], d["readingDate"], d["sensorValue"])
            for d in dados
        ]

        execute_batch(cur, """
            INSERT INTO leituras (sensor_id, data_leitura, valor_sensor)
            VALUES (%s, %s, %s)
            ON CONFLICT (sensor_id, data_leitura) DO NOTHING
        """, registros)

        # Atualiza sync_state (diagnóstico)
        max_ts = max(d["readingDate"] for d in dados)
        cur.execute("""
            INSERT INTO sync_state (sensor_id, last_timestamp)
            VALUES (%s, %s)
            ON CONFLICT (sensor_id) DO UPDATE
                SET last_timestamp = EXCLUDED.last_timestamp
            WHERE sync_state.last_timestamp IS NULL
               OR sync_state.last_timestamp < EXCLUDED.last_timestamp
        """, (sensor_id, max_ts))

        conn.commit()
        total_sensor   += qtd
        current_offset += qtd

        if qtd < PAGE_SIZE:
            break

    if total_sensor > 0:
        print(f"  ✅ sensor {sensor_id} (dev {device_id}): {total_sensor} leituras desde {inicio}")

    cur.close()
    release_conn(conn)
    return total_sensor

# ======================================================
# TOKEN
# ======================================================

def obter_token() -> str:
    aguardar_rate_limit()
    r = session.get(
        f"{BASE_URL}/token",
        params={"apiKey": API_KEY},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()["token"]

# ======================================================
# DEVICES E SENSORES
# ======================================================

def cadastrar_devices_e_sensores(token) -> dict:
    """Retorna {device_id: [sensor_id, ...]} apenas com sensores de tipos válidos."""
    conn = get_conn()
    cur  = conn.cursor()
    aguardar_rate_limit()

    r = session.get(
        f"{BASE_URL}/UserDevices",
        headers={"Authorization": f"Bearer {token}"},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()

    mapa_devices = {}
    for device in r.json():
        cur.execute("""
            INSERT INTO devices (
                device_id, device_name, serial_number, status,
                latitude, longitude, last_upload,
                battery_percentage, last_status, reference
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (device_id) DO UPDATE SET
                device_name        = EXCLUDED.device_name,
                status             = EXCLUDED.status,
                latitude           = EXCLUDED.latitude,
                longitude          = EXCLUDED.longitude,
                last_upload        = EXCLUDED.last_upload,
                battery_percentage = EXCLUDED.battery_percentage,
                last_status        = EXCLUDED.last_status,
                reference          = EXCLUDED.reference;
        """, (
            device["deviceId"], device["deviceName"],
            device.get("serialNumber"), device.get("status"),
            device.get("latitude"), device.get("longitude"),
            device.get("lastUpload"), device.get("batteryPercentage"),
            device.get("lastStatus"), device.get("reference"),
        ))

        sensores_validos = []
        for sensor in device.get("sensors", []):
            tipo = (sensor.get("sensorType") or "").strip()
            if tipo in TIPOS_VALIDOS:
                sid = sensor["sensorId"]
                sensores_validos.append(sid)
                cur.execute("""
                    INSERT INTO sensores (sensor_id, device_id, nome_customizado, tipo_sensor, unidade_medida)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (sensor_id) DO NOTHING;
                """, (sid, device["deviceId"], sensor.get("customName"), tipo, sensor.get("uom")))

        if sensores_validos:
            mapa_devices[device["deviceId"]] = sensores_validos

    conn.commit()
    cur.close()
    release_conn(conn)
    return mapa_devices

# ======================================================
# ORQUESTRADOR
# ======================================================

def baixar_e_salvar_leituras(token, mapa_devices, gap_fill: bool = False):
    # Achata todos os (device_id, sensor_id) em uma lista plana
    tarefas = [
        (did, sid)
        for did, sids in mapa_devices.items()
        for sid in sids
    ]
    todos_sensor_ids = [sid for _, sid in tarefas]

    cursores = carregar_cursores(todos_sensor_ids, gap_fill=gap_fill)
    agora    = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    total    = 0

    print(f"\n📡 {len(tarefas)} sensores para processar | endDate={agora}")
    if gap_fill:
        print(f"🔁 GAP-FILL ativo: varrendo desde {DATA_GAP_FILL} em todos os sensores\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                worker_sensor, token, did, sid, cursores[sid], agora
            ): (did, sid)
            for did, sid in tarefas
        }
        for f in as_completed(futures):
            did, sid = futures[f]
            try:
                total += f.result()
            except Exception as e:
                print(f"  💥 Falha sensor {sid} (dev {did}): {e}")

    print(f"\n✅ TOTAL DE LEITURAS PROCESSADAS: {total}")

# ======================================================
# MAIN
# ======================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestão incremental Orion → Supabase")
    parser.add_argument(
        "--gap-fill",
        action="store_true",
        help=(
            f"Varre todos os sensores desde {DATA_GAP_FILL} para detectar e "
            "preencher buracos. Dados já existentes são ignorados (ON CONFLICT DO NOTHING)."
        ),
    )
    args = parser.parse_args()

    try:
        garantir_schema()
        tk     = obter_token()
        m_devs = cadastrar_devices_e_sensores(tk)
        baixar_e_salvar_leituras(tk, m_devs, gap_fill=args.gap_fill)
    except Exception as e:
        print(f"💥 ERRO FATAL: {e}")
        raise
