"""
Ingestão incremental "últimos dados" — varredura curta por device.

Correções em relação à versão anterior
--------------------------------------
1. A chave da API estava ESCRITA NO CÓDIGO, num repositório público.
   Passou a vir de os.getenv("API_KEY"), igual ao outro script.
   >>> A chave antiga precisa ser revogada na Orion. <<<

2. Paginação: o laço fazia "offset += 1", avançando de um em um e
   rebaixando quase a mesma página a cada volta. Agora soma a quantidade
   de registros recebidos e envia "limit" explícito.

3. A janela começava exatamente em last_upload. Qualquer leitura gravada
   com atraso na Orion ficava para trás para sempre. Agora recua
   SOBREPOSICAO_MIN minutos.
"""

import os
import time
from datetime import datetime, timedelta, timezone

import requests
import psycopg2
from psycopg2.extras import execute_batch
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ======================================================
# CONFIG
# ======================================================
API_KEY = os.getenv("API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
BASE_URL = "https://api.oriondata.io/api"

REQUEST_TIMEOUT = 30
SENSOR_BATCH_SIZE = 50
PAGE_SIZE = 500
SLEEP_BETWEEN_CALLS = 0.3

FALLBACK_HORAS = 6      # device sem last_upload
SOBREPOSICAO_MIN = 60   # recuo na janela, para pegar leitura atrasada

if not API_KEY:
    raise SystemExit("API_KEY não definida (secret do GitHub).")
if not DATABASE_URL:
    raise SystemExit("DATABASE_URL não definida (secret do GitHub).")

# ======================================================
# SESSION COM RETRY
# ======================================================
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(
    total=5, backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)))


def obter_token():
    r = session.get(f"{BASE_URL}/token",
                    params={"apiKey": API_KEY},
                    timeout=REQUEST_TIMEOUT)
    if r.status_code in (401, 403):
        raise SystemExit(f"❌ A Orion recusou a API_KEY ({r.status_code}): {r.text[:200]}")
    r.raise_for_status()
    return r.json()["token"]


# O token expira durante a execução; renovamos a cada 8 min e também
# sempre que a API responder 401.
TOKEN_TTL_SEG = 8 * 60
_token = {"valor": None, "em": 0.0}


def token_atual(forcar=False):
    if forcar or _token["valor"] is None or time.time() - _token["em"] > TOKEN_TTL_SEG:
        _token["valor"] = obter_token()
        _token["em"] = time.time()
        print("🔑 Token Orion renovado")
    return _token["valor"]


def get_api(url, params):
    for tentativa in range(2):
        r = session.get(url, params=params, timeout=REQUEST_TIMEOUT,
                        headers={"Authorization": f"Bearer {token_atual(forcar=tentativa == 1)}"})
        if r.status_code == 401 and tentativa == 0:
            continue
        if r.status_code == 401:
            raise SystemExit(f"❌ 401 mesmo com token novo — verifique a API_KEY: {r.text[:200]}")
        r.raise_for_status()
        return r.json()


# ======================================================
# BANCO
# ======================================================
def obter_devices_db(conn):
    cur = conn.cursor()
    cur.execute("SELECT device_id, last_upload FROM devices")
    rows = cur.fetchall()
    cur.close()
    return rows


def obter_sensores_device(conn, device_id):
    cur = conn.cursor()
    # Soft sensors (Water Level, Water Level (RL), Pore Pressure) não são coletados.
    cur.execute("""
        SELECT sensor_id FROM sensores
        WHERE device_id = %s
          AND lower(coalesce(tipo_sensor, '')) NOT IN
              ('water level', 'water level (rl)', 'pore pressure')
    """, (device_id,))
    sensores = [r[0] for r in cur.fetchall()]
    cur.close()
    return sensores


def calcular_janela(last_upload):
    agora = datetime.now(timezone.utc)
    if last_upload:
        inicio = last_upload.replace(tzinfo=timezone.utc) - timedelta(minutes=SOBREPOSICAO_MIN)
    else:
        inicio = agora - timedelta(hours=FALLBACK_HORAS)
    return (inicio.strftime("%Y-%m-%dT%H:%M:%S"),
            agora.strftime("%Y-%m-%dT%H:%M:%S"))


SQL_LEITURA = """
    INSERT INTO leituras (sensor_id, data_leitura, valor_sensor)
    VALUES (%s, %s, %s)
    ON CONFLICT (sensor_id, data_leitura) DO NOTHING
"""


# ======================================================
# DOWNLOAD
# ======================================================
def baixar_device(device_id, last_upload, conn):
    sensores = obter_sensores_device(conn, device_id)
    if not sensores:
        print(f"⚠️ Device {device_id} sem sensores")
        return 0

    data_inicio, data_fim = calcular_janela(last_upload)
    cur = conn.cursor()
    total = 0

    print(f"\n📡 Device {device_id}  |  {data_inicio} → {data_fim}  "
          f"({len(sensores)} sensores)")

    for i in range(0, len(sensores), SENSOR_BATCH_SIZE):
        lote = sensores[i:i + SENSOR_BATCH_SIZE]
        sensor_param = ",".join(map(str, lote))
        offset = 0

        while True:
            try:
                dados = get_api(
                    f"{BASE_URL}/SensorData",
                    params={
                        "version": "1.3",
                        "startDate": data_inicio,
                        "endDate": data_fim,
                        "offset": offset,
                        "limit": PAGE_SIZE,
                        "sensorIds": sensor_param,
                    },
                )
            except SystemExit:
                raise
            except Exception as e:
                print(f"⚠️ Erro no lote {lote[0]}..{lote[-1]}: {e}")
                break

            qtd = len(dados)
            if qtd == 0:
                break

            execute_batch(cur, SQL_LEITURA,
                          [(d["sensorId"], d["readingDate"], d["sensorValue"])
                           for d in dados],
                          page_size=500)
            conn.commit()

            total += qtd
            offset += qtd          # <- antes era "offset += 1"

            if qtd < PAGE_SIZE:
                break

            time.sleep(SLEEP_BETWEEN_CALLS)

    cur.close()
    return total


# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    print("🚀 Ingestão incremental por device")

    conn = psycopg2.connect(DATABASE_URL)
    token_atual()
    devices = obter_devices_db(conn)
    print(f"📦 Total devices: {len(devices)}")

    total_geral = 0
    for device_id, last_upload in devices:
        try:
            total_geral += baixar_device(device_id, last_upload, conn)
        except SystemExit:
            raise
        except Exception as e:
            print(f"💥 Device {device_id} falhou: {e}")

    conn.close()
    print(f"\n🏁 Finalizado — {total_geral} leituras processadas")
