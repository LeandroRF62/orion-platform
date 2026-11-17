"""
Backfill histórico — roda sob demanda (workflow_dispatch ou local).

Use quando precisar recuperar um período inteiro, por exemplo depois de a
ingestão ficar parada por dias. A ingestão normal (ingest_incremental.py)
já recupera sozinha pelo cursor, então o backfill é exceção.

Correções em relação à versão anterior:
1. O token era pedido uma vez só e não renovava. Num backfill de horas ele
   expira no meio e tudo passa a voltar 401.
2. Os erros eram só impressos e o job terminava verde.
3. Trazia TODOS os sensores da tabela, inclusive os canais que a ingestão
   não coleta. Agora respeita a mesma lista de tipos.
4. Não gravava o cursor em sync_state, então a ingestão normal repetia
   todo o período depois.

Variáveis de ambiente opcionais:
    DATA_INICIAL  padrão 2026-01-01T00:00:00
    DATA_FINAL    padrão agora
"""

import os
import time
from datetime import datetime, timedelta, timezone

from psycopg2.extras import execute_batch

from common import get_session, obter_token, get_db_conn, BASE_URL, REQUEST_TIMEOUT

DATA_INICIAL = os.getenv("DATA_INICIAL", "2026-01-01T00:00:00")
DATA_FINAL = os.getenv("DATA_FINAL")

BLOCO_DIAS = 7
MAX_JANELAS = 400   # voltas da janela deslizante dentro de um bloco
SENSOR_BATCH_SIZE = 50
PAGE_SIZE = 500
SLEEP = 0.3
TOKEN_TTL_SEG = 8 * 60

# Mesma lista do ingest_incremental.py
TIPOS_VALIDOS = (
    "A-Axis Delta Angle", "B-Axis Delta Angle", "Air Temperature",
    "Device Temperature", "Water Level", "Water Temperature", "Rainfall",
    "VWP (Frequency)", "VWP (Temp)",
)

SQL_LEITURA = """
    INSERT INTO leituras (sensor_id, data_leitura, valor_sensor)
    VALUES (%s, %s, %s)
    ON CONFLICT (sensor_id, data_leitura) DO NOTHING
"""

SQL_CURSOR = """
    INSERT INTO sync_state(sensor_id, last_timestamp)
    VALUES(%s, %s)
    ON CONFLICT(sensor_id) DO UPDATE
    SET last_timestamp = EXCLUDED.last_timestamp
    WHERE sync_state.last_timestamp IS NULL
       OR sync_state.last_timestamp < EXCLUDED.last_timestamp
"""

erros = []


def _parse_ts(txt):
    """readingDate da Orion. Tolera 'Z', fração de segundo e offset."""
    t = str(txt).strip().replace("Z", "+00:00")
    try:
        d = datetime.fromisoformat(t)
    except ValueError:
        t = t.split(".")[0].split("+")[0]
        d = datetime.fromisoformat(t)
    return d.replace(tzinfo=None)


def gerar_blocos(inicio_str, fim_dt):
    inicio = datetime.fromisoformat(inicio_str).replace(tzinfo=timezone.utc)
    while inicio < fim_dt:
        fim = min(inicio + timedelta(days=BLOCO_DIAS), fim_dt)
        yield inicio, fim
        inicio = fim


class Token:
    """Renova a cada 8 min e em qualquer 401."""

    def __init__(self, session):
        self.session = session
        self.valor = None
        self.em = 0.0

    def get(self, forcar=False):
        if forcar or self.valor is None or time.time() - self.em > TOKEN_TTL_SEG:
            self.valor = obter_token(self.session)
            self.em = time.time()
            print("🔑 Token Orion renovado")
        return self.valor

    def get_api(self, url, params):
        for tentativa in range(2):
            r = self.session.get(
                url, params=params, timeout=REQUEST_TIMEOUT,
                headers={"Authorization": f"Bearer {self.get(forcar=tentativa == 1)}"})
            if r.status_code == 401 and tentativa == 0:
                continue
            if r.status_code == 401:
                raise SystemExit("❌ 401 mesmo com token novo — verifique a API_KEY.")
            r.raise_for_status()
            return r.json()


def run_backfill():
    session = get_session()
    token = Token(session)

    conn = get_db_conn()
    cur = conn.cursor()

    cur.execute("SELECT sensor_id FROM sensores WHERE tipo_sensor = ANY(%s) "
                "ORDER BY sensor_id", (list(TIPOS_VALIDOS),))
    sensor_ids = [r[0] for r in cur.fetchall()]
    print(f"📦 {len(sensor_ids)} sensores")

    fim_geral = (datetime.fromisoformat(DATA_FINAL).replace(tzinfo=timezone.utc)
                 if DATA_FINAL else datetime.now(timezone.utc))
    total_geral = 0

    for ini, fim in gerar_blocos(DATA_INICIAL, fim_geral):
        print(f"\n🗓️ {ini.date()} → {fim.date()}")

        for i in range(0, len(sensor_ids), SENSOR_BATCH_SIZE):
            lote = sensor_ids[i:i + SENSOR_BATCH_SIZE]
            # A Orion não devolve o bloco inteiro de uma vez: a resposta para em
            # poucos dias e vem com menos linhas que o limit, o que parecia fim
            # dos dados. Por isso recomeçamos a janela na última leitura recebida
            # até a API não devolver mais nada dentro do bloco.
            janela = ini.strftime("%Y-%m-%dT%H:%M:%S")
            fim_txt = fim.strftime("%Y-%m-%dT%H:%M:%S")

            for _ in range(MAX_JANELAS):
                offset = 0
                maior = None
                qtd_janela = 0
                parar = False

                while True:
                    try:
                        dados = token.get_api(
                            f"{BASE_URL}/SensorData",
                            params={
                                "version": "1.3",
                                "startDate": janela,
                                "endDate": fim_txt,
                                "offset": offset,
                                "limit": PAGE_SIZE,
                                "sensorIds": ",".join(map(str, lote)),
                            },
                        )
                    except SystemExit:
                        raise
                    except Exception as e:
                        msg = f"lote {lote[0]}..{lote[-1]} em {ini.date()} (desde {janela}): {e}"
                        print(f"⚠️ Erro no {msg}")
                        erros.append(msg)
                        parar = True
                        break

                    qtd = len(dados)
                    if qtd == 0:
                        parar = True
                        break

                    execute_batch(cur, SQL_LEITURA,
                                  [(d["sensorId"], d["readingDate"], d["sensorValue"])
                                   for d in dados],
                                  page_size=500)

                    ultimos = {}
                    for d in dados:
                        sid, ts = d["sensorId"], d["readingDate"]
                        if sid not in ultimos or ts > ultimos[sid]:
                            ultimos[sid] = ts
                        if maior is None or ts > maior:
                            maior = ts
                    for sid, ts in ultimos.items():
                        cur.execute(SQL_CURSOR, (sid, ts))

                    conn.commit()
                    total_geral += qtd
                    qtd_janela += qtd
                    offset += qtd          # antes era "offset += 1"

                    if qtd < PAGE_SIZE:
                        break

                    time.sleep(SLEEP)

                if parar or qtd_janela == 0 or maior is None:
                    break
                try:
                    proxima = (_parse_ts(maior) + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S")
                except Exception:
                    break
                if proxima <= janela or proxima >= fim_txt:
                    break
                janela = proxima

    cur.close()
    conn.close()
    print(f"\n🏁 Backfill finalizado — {total_geral} leituras")

    if erros:
        print(f"\n❌ {len(erros)} lote(s) com erro. Primeiros:")
        for m in erros[:10]:
            print(f"   - {m}")
        raise SystemExit(f"❌ Backfill terminou com {len(erros)} erro(s).")


if __name__ == "__main__":
    run_backfill()
