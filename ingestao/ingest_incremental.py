"""
Ingestão incremental Orion -> Supabase/Postgres.

Mudanças em relação à versão anterior
-------------------------------------
1. Coleta TODOS os canais do instrumento (raw e soft sensors). Antes havia
   uma lista de tipos aceitos que descartava em silêncio tudo que fosse novo
   -- foi assim que "Pore Pressure" e "Water Level (RL)" ficaram de fora.

2. Grava todos os campos que a API devolve, inclusive os que ainda não têm
   coluna própria, dentro de uma coluna JSONB chamada "raw". Nada se perde.

3. ON CONFLICT DO UPDATE na tabela sensores. Antes era DO NOTHING, então
   renomear uma TAG na Orion nunca chegava ao banco.

4. Cursor de sincronização por SENSOR, não por device. Dois bugs resolvidos:
   - um sensor novo não força mais o device inteiro a reprocessar desde 2025;
   - um sensor sem dados no lote não tem mais o cursor empurrado para frente
     pelo colega que tinha dados (isso pulava leituras de forma permanente).
"""

import os
import json
import time
import threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.pool import SimpleConnectionPool
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ======================================================
# CONFIG
# ======================================================
API_KEY = os.getenv("API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
BASE_URL = "https://api.oriondata.io/api"

DATA_INICIAL_HISTORICO = "2025-01-01T00:00:00"
REQUEST_TIMEOUT = 45
MAX_WORKERS = 4
PAGE_SIZE = 500
SENSOR_BATCH_SIZE = 50          # limite de sensorIds por chamada (tamanho da URL)

# Vazio = coleta tudo. Preencha apenas se quiser excluir algum tipo, por
# exemplo telemetria de sinal celular:
#   TIPOS_IGNORADOS = ("RSRP (Cellular)", "RSRQ (Cellular)",
#                      "RSSI (Cellular)", "SINR (Cellular)")
TIPOS_IGNORADOS = ()

API_MIN_INTERVAL = 0.5
api_lock = threading.Lock()
ultimo_request = 0.0


def aguardar_rate_limit():
    global ultimo_request
    with api_lock:
        agora = time.time()
        delta = agora - ultimo_request
        if delta < API_MIN_INTERVAL:
            time.sleep(API_MIN_INTERVAL - delta)
        ultimo_request = time.time()


# ======================================================
# SESSION / POOL
# ======================================================
session = requests.Session()
retries = Retry(total=5, backoff_factor=2,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"])
session.mount("https://", HTTPAdapter(max_retries=retries))

if not DATABASE_URL:
    raise SystemExit("DATABASE_URL não definida (secret do GitHub).")
if not API_KEY:
    raise SystemExit("API_KEY não definida (secret do GitHub).")

db_pool = SimpleConnectionPool(minconn=1, maxconn=MAX_WORKERS + 2, dsn=DATABASE_URL)
pool_lock = threading.Lock()


def get_conn():
    with pool_lock:
        return db_pool.getconn()


def release_conn(conn):
    with pool_lock:
        db_pool.putconn(conn)


# ======================================================
# HELPERS
# ======================================================
def primeiro(d, *chaves, padrao=None):
    """Primeiro valor não nulo entre as chaves. A Orion varia a grafia de
    alguns campos entre endpoints, então tentamos mais de um nome."""
    for k in chaves:
        v = d.get(k)
        if v is not None:
            return v
    return padrao


def em_lotes(seq, tamanho):
    for i in range(0, len(seq), tamanho):
        yield seq[i:i + tamanho]


def garantir_schema(cur):
    """Cria colunas e índices que o script usa. Idempotente."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_state(
            sensor_id BIGINT PRIMARY KEY,
            last_timestamp TIMESTAMP
        );
    """)
    for coluna, tipo in [
        ("reference", "TEXT"), ("modelo", "TEXT"), ("firmware", "TEXT"),
        ("fuso_horario", "TEXT"), ("projeto", "TEXT"), ("site", "TEXT"),
        ("intervalo_min", "INTEGER"), ("atualizado_em", "TIMESTAMPTZ"),
        ("raw", "JSONB"),
    ]:
        cur.execute(f"ALTER TABLE devices ADD COLUMN IF NOT EXISTS {coluna} {tipo};")

    for coluna, tipo in [
        ("canal", "INTEGER"), ("canal_device", "INTEGER"), ("scada_tag", "TEXT"),
        ("sensor_base_id", "BIGINT"), ("eh_soft", "BOOLEAN"), ("oculto", "BOOLEAN"),
        ("notas", "TEXT"), ("ultimo_valor", "DOUBLE PRECISION"),
        ("atualizado_em", "TIMESTAMPTZ"), ("raw", "JSONB"),
    ]:
        cur.execute(f"ALTER TABLE sensores ADD COLUMN IF NOT EXISTS {coluna} {tipo};")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_sensores_device ON sensores(device_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_leituras_sensor_data "
                "ON leituras(sensor_id, data_leitura DESC);")


def carregar_sync_state():
    conn = get_conn()
    cur = conn.cursor()
    garantir_schema(cur)
    cur.execute("SELECT sensor_id, last_timestamp FROM sync_state;")
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    release_conn(conn)
    # 1h de sobreposição protege contra leitura chegando fora de ordem
    return {
        sid: (ts - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")
        for sid, ts in rows if ts
    }


# ======================================================
# TOKEN
# ======================================================
def obter_token():
    aguardar_rate_limit()
    r = session.get(f"{BASE_URL}/token",
                    params={"apiKey": API_KEY},
                    timeout=REQUEST_TIMEOUT)
    if r.status_code in (401, 403):
        raise SystemExit(
            f"❌ A Orion recusou a API_KEY ({r.status_code}). Confira o secret "
            f"API_KEY no GitHub (chave revogada/trocada?). Resposta: {r.text[:200]}")
    r.raise_for_status()
    return r.json()["token"]


# O token da Orion expira. Antes ele era pedido uma única vez no início e
# reaproveitado até o fim; como o cadastro de devices/sensores leva alguns
# minutos, quando o download começava o token já tinha vencido e TODAS as
# chamadas ao /SensorData voltavam 401 Unauthorized.
TOKEN_TTL_SEG = 8 * 60          # renova preventivamente a cada 8 min
MAX_401_SEGUIDOS = 5            # depois disso aborta em vez de martelar a API


class GerenciadorToken:
    def __init__(self):
        self._lock = threading.Lock()
        self._token = None
        self._obtido_em = 0.0
        self.falhas_401 = 0
        self.abortar = False

    def _renovar(self):
        self._token = obter_token()
        self._obtido_em = time.time()
        print("🔑 Token Orion renovado")

    def get(self):
        with self._lock:
            if self._token is None or time.time() - self._obtido_em > TOKEN_TTL_SEG:
                self._renovar()
            return self._token

    def renovar_se_igual(self, token_usado):
        """Chamado após um 401. Só renova se outra thread ainda não renovou."""
        with self._lock:
            if self._token == token_usado:
                self._renovar()
            return self._token

    def headers(self):
        return {"Authorization": f"Bearer {self.get()}"}

    def registrar_401(self):
        with self._lock:
            self.falhas_401 += 1
            if self.falhas_401 >= MAX_401_SEGUIDOS:
                self.abortar = True

    def registrar_ok(self):
        with self._lock:
            self.falhas_401 = 0


tokens = GerenciadorToken()


def get_api(url, params=None):
    """GET autenticado. Em 401 renova o token e tenta mais uma vez."""
    for tentativa in range(2):
        token = tokens.get()
        aguardar_rate_limit()
        r = session.get(url, headers={"Authorization": f"Bearer {token}"},
                        params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code == 401 and tentativa == 0:
            tokens.renovar_se_igual(token)
            continue
        if r.status_code == 401:
            tokens.registrar_401()
            raise RuntimeError(f"401 mesmo com token novo: {r.text[:200]}")
        r.raise_for_status()
        tokens.registrar_ok()
        return r.json()


# ======================================================
# DEVICES E SENSORES
# ======================================================
SQL_DEVICE = """
    INSERT INTO devices(
        device_id, device_name, serial_number, status,
        latitude, longitude, last_upload, battery_percentage, last_status,
        reference, modelo, firmware, fuso_horario, projeto, site,
        intervalo_min, raw, atualizado_em
    )
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb, now())
    ON CONFLICT(device_id) DO UPDATE SET
        device_name        = EXCLUDED.device_name,
        serial_number      = EXCLUDED.serial_number,
        status             = EXCLUDED.status,
        latitude           = EXCLUDED.latitude,
        longitude          = EXCLUDED.longitude,
        last_upload        = EXCLUDED.last_upload,
        battery_percentage = EXCLUDED.battery_percentage,
        last_status        = EXCLUDED.last_status,
        reference          = EXCLUDED.reference,
        modelo             = EXCLUDED.modelo,
        firmware           = EXCLUDED.firmware,
        fuso_horario       = EXCLUDED.fuso_horario,
        projeto            = EXCLUDED.projeto,
        site               = EXCLUDED.site,
        intervalo_min      = EXCLUDED.intervalo_min,
        raw                = EXCLUDED.raw,
        atualizado_em      = now();
"""

SQL_SENSOR = """
    INSERT INTO sensores(
        sensor_id, device_id, nome_customizado, tipo_sensor, unidade_medida,
        canal, canal_device, scada_tag, sensor_base_id, eh_soft, oculto,
        notas, ultimo_valor, raw, atualizado_em
    )
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb, now())
    ON CONFLICT(sensor_id) DO UPDATE SET
        device_id        = EXCLUDED.device_id,
        nome_customizado = EXCLUDED.nome_customizado,
        tipo_sensor      = EXCLUDED.tipo_sensor,
        unidade_medida   = EXCLUDED.unidade_medida,
        canal            = EXCLUDED.canal,
        canal_device     = EXCLUDED.canal_device,
        scada_tag        = EXCLUDED.scada_tag,
        sensor_base_id   = EXCLUDED.sensor_base_id,
        eh_soft          = EXCLUDED.eh_soft,
        oculto           = EXCLUDED.oculto,
        notas            = EXCLUDED.notas,
        ultimo_valor     = EXCLUDED.ultimo_valor,
        raw              = EXCLUDED.raw,
        atualizado_em    = now();
"""


def cadastrar_devices_e_sensores():
    conn = get_conn()
    cur = conn.cursor()
    garantir_schema(cur)
    conn.commit()

    devices = get_api(f"{BASE_URL}/UserDevices")

    mapa_devices = {}
    total_sensores = 0
    linhas_devices = []
    linhas_sensores = []

    for device in devices:
        did = device["deviceId"]

        linhas_devices.append((
            did,
            device.get("deviceName"),
            primeiro(device, "serialNumber", "serial"),
            device.get("status"),
            device.get("latitude"),
            device.get("longitude"),
            device.get("lastUpload"),
            primeiro(device, "batteryPercentage", "batteryPercent"),
            device.get("lastStatus"),
            device.get("reference"),
            primeiro(device, "model", "deviceModel", "modelName"),
            primeiro(device, "firmwareVersion", "firmware"),
            primeiro(device, "timeZone", "timezone"),
            primeiro(device, "projectName", "project"),
            primeiro(device, "siteName", "site"),
            primeiro(device, "sampleIntervalMinutes", "intervalMinutes"),
            json.dumps(device, ensure_ascii=False, default=str),
        ))

        sensores_device = []
        for sensor in device.get("sensors", []):
            tipo = (sensor.get("sensorType") or "").strip()
            if tipo in TIPOS_IGNORADOS:
                continue

            sid = sensor["sensorId"]
            sensores_device.append(sid)

            base = primeiro(sensor, "baseSensorId", "parentSensorId")
            linhas_sensores.append((
                sid,
                did,
                sensor.get("customName"),
                tipo or None,
                primeiro(sensor, "uom", "unitOfMeasure"),
                primeiro(sensor, "channelNo", "channel", "channelNumber"),
                primeiro(sensor, "deviceChannelNo", "deviceChannel"),
                primeiro(sensor, "scadaTag", "scada_tag"),
                base,
                # Soft sensor é o canal calculado a partir de outro.
                primeiro(sensor, "isSoftSensor", "isSoft", padrao=(base is not None)),
                primeiro(sensor, "isHidden", "hidden"),
                primeiro(sensor, "notes", "note"),
                primeiro(sensor, "lastValue", "latestValue"),
                json.dumps(sensor, ensure_ascii=False, default=str),
            ))

        total_sensores += len(sensores_device)
        if sensores_device:
            mapa_devices[did] = sensores_device

    # Em lote: milhares de INSERTs um a um para o Supabase levavam minutos.
    execute_batch(cur, SQL_DEVICE, linhas_devices, page_size=200)
    execute_batch(cur, SQL_SENSOR, linhas_sensores, page_size=500)
    conn.commit()
    cur.close()
    release_conn(conn)

    print(f"📋 {len(devices)} devices | {total_sensores} sensores cadastrados/atualizados")
    return mapa_devices


# ======================================================
# LEITURAS
# ======================================================
SQL_LEITURA = """
    INSERT INTO leituras(sensor_id, data_leitura, valor_sensor)
    VALUES(%s, %s, %s)
    ON CONFLICT(sensor_id, data_leitura) DO NOTHING
"""

SQL_CURSOR = """
    INSERT INTO sync_state(sensor_id, last_timestamp)
    VALUES(%s, %s)
    ON CONFLICT(sensor_id) DO UPDATE
    SET last_timestamp = EXCLUDED.last_timestamp
    WHERE sync_state.last_timestamp IS NULL
       OR sync_state.last_timestamp < EXCLUDED.last_timestamp
"""


def baixar_lote(cur, conn, lote, inicio, agora):
    """Pagina um lote de sensores que compartilham a mesma data de início."""
    sensor_param = ",".join(map(str, lote))
    offset = 0
    total = 0

    while True:
        if tokens.abortar:
            break
        try:
            dados = get_api(
                f"{BASE_URL}/SensorData",
                params={
                    "version": "1.3",
                    "startDate": inicio,
                    "endDate": agora,
                    "offset": offset,
                    "limit": PAGE_SIZE,
                    "sensorIds": sensor_param,
                },
            )
        except Exception as e:
            print(f"⚠️ Erro no lote {lote[0]}..{lote[-1]}: {e}")
            break

        qtd = len(dados)
        if qtd == 0:
            break

        execute_batch(cur, SQL_LEITURA,
                      [(d["sensorId"], d["readingDate"], d["sensorValue"]) for d in dados],
                      page_size=500)

        # Cursor POR SENSOR: só avança quem realmente trouxe dado.
        ultimos = {}
        for d in dados:
            sid, ts = d["sensorId"], d["readingDate"]
            if sid not in ultimos or ts > ultimos[sid]:
                ultimos[sid] = ts
        for sid, ts in ultimos.items():
            cur.execute(SQL_CURSOR, (sid, ts))

        conn.commit()
        total += qtd
        offset += qtd          # avança pela quantidade recebida

        if qtd < PAGE_SIZE:
            break

    return total


def worker_device(device_id, sensor_ids, sync_map, agora):
    if tokens.abortar:
        return 0
    conn = get_conn()
    cur = conn.cursor()

    # Agrupa por data de início: sensores novos fazem o próprio backfill
    # sem arrastar os que já estão em dia.
    grupos = {}
    for sid in sensor_ids:
        grupos.setdefault(sync_map.get(sid, DATA_INICIAL_HISTORICO), []).append(sid)

    total = 0
    for inicio, ids in sorted(grupos.items()):
        print(f"🛰️ Device {device_id} | {len(ids):>3} sensor(es) desde {inicio}")
        for lote in em_lotes(ids, SENSOR_BATCH_SIZE):
            total += baixar_lote(cur, conn, lote, inicio, agora)

    cur.close()
    release_conn(conn)
    return total


def baixar_e_salvar_leituras(mapa_devices):
    sync_map = carregar_sync_state()
    agora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    total = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(worker_device, did, sids, sync_map, agora): did
            for did, sids in mapa_devices.items()
        }
        for f in as_completed(futures):
            did = futures[f]
            try:
                total += f.result()
            except Exception as e:
                print(f"💥 Device {did} falhou: {e}")

    print(f"🌌 TOTAL DE LEITURAS PROCESSADAS: {total}")

    if tokens.abortar:
        # Falha o job no Actions para você ver em vermelho, em vez de
        # terminar "verde" com centenas de 401 escondidos no log.
        raise SystemExit("❌ Abortado: a Orion continuou respondendo 401 mesmo "
                         "com token renovado. Verifique a API_KEY.")


# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    try:
        m_devs = cadastrar_devices_e_sensores()
        baixar_e_salvar_leituras(m_devs)
    except Exception as e:
        print(f"💥 ERRO: {e}")
        raise
