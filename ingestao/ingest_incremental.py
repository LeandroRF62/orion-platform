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

# ======================================================
# CONFIG
# ======================================================

API_KEY      = os.getenv("API_KEY")
BASE_URL     = "https://api.oriondata.io/api"
DATABASE_URL = os.getenv("DATABASE_URL")

DATA_INICIAL_HISTORICO = "2025-01-01T00:00:00"
REQUEST_TIMEOUT        = 45
MAX_WORKERS            = 4
PAGE_SIZE              = 500

TIPOS_VALIDOS = (
    "A-Axis Delta Angle",
    "B-Axis Delta Angle",
    "Air Temperature",
    "Device Temperature"
)

# Rate-limit: mínimo de 0.5 s entre requests
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

db_pool   = SimpleConnectionPool(minconn=1, maxconn=MAX_WORKERS + 2, dsn=DATABASE_URL)
pool_lock = threading.Lock()

def get_conn():
    with pool_lock:
        return db_pool.getconn()

def release_conn(conn):
    with pool_lock:
        db_pool.putconn(conn)

# ======================================================
# GARANTIR ESTRUTURA DO BANCO
# ======================================================

def garantir_schema():
    """Cria/ajusta tabelas necessárias antes de qualquer operação."""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("ALTER TABLE devices ADD COLUMN IF NOT EXISTS reference TEXT;")
    # sync_state mantida apenas como log/diagnóstico — não é mais a fonte primária
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
# CURSOR DE SINCRONIZAÇÃO — FONTE: tabela leituras
# ======================================================

def carregar_cursores(sensor_ids: list[int]) -> dict[int, str]:
    """
    Para cada sensor_id, retorna o timestamp da ÚLTIMA leitura já salva
    em `leituras`, subtraindo 1 hora como margem de segurança.

    Se não houver nenhuma leitura, usa DATA_INICIAL_HISTORICO.
    Dessa forma, mesmo que sync_state esteja desatualizado ou corrompido,
    o cursor sempre reflete o estado real do banco.
    """
    if not sensor_ids:
        return {}

    conn = get_conn()
    cur  = conn.cursor()

    placeholders = ",".join(["%s"] * len(sensor_ids))
    cur.execute(f"""
        SELECT sensor_id, MAX(data_leitura)
        FROM leituras
        WHERE sensor_id IN ({placeholders})
        GROUP BY sensor_id
    """, sensor_ids)

    rows = {sid: ts for sid, ts in cur.fetchall()}
    cur.close()
    release_conn(conn)

    cursores = {}
    for sid in sensor_ids:
        if sid in rows and rows[sid]:
            # Subtrai 1 hora para cobrir oscilações de segundos na API
            inicio = (rows[sid] - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")
        else:
            inicio = DATA_INICIAL_HISTORICO
        cursores[sid] = inicio

    return cursores

# ======================================================
# WORKER POR DEVICE
# ======================================================

def worker_device(token, device_id, sensor_ids, cursores, agora):
    """
    Baixa leituras para um device específico.

    Cada sensor tem seu próprio cursor calculado a partir da última
    leitura real no banco. O request usa o cursor MAIS ANTIGO do grupo
    para garantir que nenhum sensor fique para trás.
    """
    conn    = get_conn()
    cur     = conn.cursor()
    headers = {"Authorization": f"Bearer {token}"}

    # Cursor mais antigo entre os sensores do device
    inicio = min(cursores.get(s, DATA_INICIAL_HISTORICO) for s in sensor_ids)
    sensor_param   = ",".join(map(str, sensor_ids))
    current_offset = 0
    total_device   = 0

    print(f"🛰️  Device {device_id} | sensores={sensor_ids} | desde={inicio}")

    while True:
        aguardar_rate_limit()

        try:
            r = session.get(
                f"{BASE_URL}/SensorData",
                headers=headers,
                params={
                    "version":    "1.3",
                    "startDate":  inicio,
                    "endDate":    agora,
                    "offset":     current_offset,
                    "limit":      PAGE_SIZE,
                    "sensorIds":  sensor_param,
                },
                timeout=REQUEST_TIMEOUT,
            )
            r.raise_for_status()
            dados = r.json()
        except Exception as e:
            print(f"⚠️  Erro no request do device {device_id}: {e}")
            break

        qtd = len(dados)
        if qtd == 0:
            break

        registros = [
            (d["sensorId"], d["readingDate"], d["sensorValue"])
            for d in dados
        ]

        # Upsert das leituras — conflito ignorado (já existe = já processado)
        execute_batch(cur, """
            INSERT INTO leituras(sensor_id, data_leitura, valor_sensor)
            VALUES (%s, %s, %s)
            ON CONFLICT (sensor_id, data_leitura) DO NOTHING
        """, registros)

        # Atualiza sync_state por sensor (apenas para diagnóstico/monitoramento)
        # A fonte real de verdade continua sendo a tabela leituras acima.
        for sid in sensor_ids:
            leituras_sensor = [d["readingDate"] for d in dados if d["sensorId"] == sid]
            if leituras_sensor:
                max_ts = max(leituras_sensor)
                cur.execute("""
                    INSERT INTO sync_state (sensor_id, last_timestamp)
                    VALUES (%s, %s)
                    ON CONFLICT (sensor_id) DO UPDATE
                        SET last_timestamp = EXCLUDED.last_timestamp
                    WHERE sync_state.last_timestamp IS DISTINCT FROM EXCLUDED.last_timestamp
                      AND (sync_state.last_timestamp IS NULL
                           OR sync_state.last_timestamp < EXCLUDED.last_timestamp)
                """, (sid, max_ts))

        conn.commit()
        total_device   += qtd
        current_offset += qtd

        print(f"   ↳ device {device_id}: +{qtd} leituras (offset={current_offset})")

        if qtd < PAGE_SIZE:
            break

    cur.close()
    release_conn(conn)
    return total_device

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

def cadastrar_devices_e_sensores(token) -> dict[int, list[int]]:
    """Sincroniza devices/sensores e retorna mapa {device_id: [sensor_ids]}."""
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            device["deviceId"],
            device["deviceName"],
            device.get("serialNumber"),
            device.get("status"),
            device.get("latitude"),
            device.get("longitude"),
            device.get("lastUpload"),
            device.get("batteryPercentage"),
            device.get("lastStatus"),
            device.get("reference"),
        ))

        sensores_validos = []
        for sensor in device.get("sensors", []):
            tipo = (sensor.get("sensorType") or "").strip()
            if tipo in TIPOS_VALIDOS:
                sid = sensor["sensorId"]
                sensores_validos.append(sid)
                cur.execute("""
                    INSERT INTO sensores (
                        sensor_id, device_id, nome_customizado,
                        tipo_sensor, unidade_medida
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (sensor_id) DO NOTHING;
                """, (
                    sid,
                    device["deviceId"],
                    sensor.get("customName"),
                    tipo,
                    sensor.get("uom"),
                ))

        if sensores_validos:
            mapa_devices[device["deviceId"]] = sensores_validos

    conn.commit()
    cur.close()
    release_conn(conn)
    return mapa_devices

# ======================================================
# ORQUESTRADOR DE LEITURAS
# ======================================================

def baixar_e_salvar_leituras(token, mapa_devices):
    # Coleta todos os sensor_ids para consultar cursores de uma vez
    todos_sensor_ids = [sid for sids in mapa_devices.values() for sid in sids]
    cursores = carregar_cursores(todos_sensor_ids)

    agora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    total = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(worker_device, token, did, sids, cursores, agora): did
            for did, sids in mapa_devices.items()
        }
        for f in as_completed(futures):
            try:
                total += f.result()
            except Exception as e:
                did = futures[f]
                print(f"💥 Falha no device {did}: {e}")

    print(f"\n✅ TOTAL DE LEITURAS PROCESSADAS: {total}")

# ======================================================
# MAIN
# ======================================================

if __name__ == "__main__":
    try:
        garantir_schema()
        tk     = obter_token()
        m_devs = cadastrar_devices_e_sensores(tk)
        baixar_e_salvar_leituras(tk, m_devs)
    except Exception as e:
        print(f"💥 ERRO FATAL: {e}")
        raise
