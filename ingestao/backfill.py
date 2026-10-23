"""
Backfill histórico — roda sob demanda (workflow_dispatch ou local).

Correção: a paginação fazia "offset += 1", reconsultando quase a mesma
página a cada volta. Agora soma a quantidade recebida e manda "limit".
"""

import time
from datetime import datetime, timedelta, timezone

from psycopg2.extras import execute_batch

from common import get_session, obter_token, get_db_conn, BASE_URL

DATA_INICIAL = "2026-01-01T00:00:00"
BLOCO_DIAS = 7
SENSOR_BATCH_SIZE = 50
PAGE_SIZE = 500
SLEEP = 0.3
REQUEST_TIMEOUT = 45

SQL_LEITURA = """
    INSERT INTO leituras (sensor_id, data_leitura, valor_sensor)
    VALUES (%s, %s, %s)
    ON CONFLICT (sensor_id, data_leitura) DO NOTHING
"""


def gerar_blocos(inicio_str, fim_dt):
    inicio = datetime.fromisoformat(inicio_str).replace(tzinfo=timezone.utc)
    while inicio < fim_dt:
        fim = min(inicio + timedelta(days=BLOCO_DIAS), fim_dt)
        yield inicio, fim
        inicio = fim


def run_backfill():
    session = get_session()
    token = obter_token(session)
    headers = {"Authorization": f"Bearer {token}"}

    conn = get_db_conn()
    cur = conn.cursor()

    cur.execute("SELECT sensor_id FROM sensores ORDER BY sensor_id")
    sensor_ids = [r[0] for r in cur.fetchall()]
    print(f"📦 {len(sensor_ids)} sensores")

    agora = datetime.now(timezone.utc)
    total_geral = 0

    for ini, fim in gerar_blocos(DATA_INICIAL, agora):
        print(f"\n🗓️  {ini.date()} → {fim.date()}")

        for i in range(0, len(sensor_ids), SENSOR_BATCH_SIZE):
            lote = sensor_ids[i:i + SENSOR_BATCH_SIZE]
            offset = 0

            while True:
                try:
                    r = session.get(
                        f"{BASE_URL}/SensorData",
                        headers=headers,
                        params={
                            "version": "1.3",
                            "startDate": ini.strftime("%Y-%m-%dT%H:%M:%S"),
                            "endDate": fim.strftime("%Y-%m-%dT%H:%M:%S"),
                            "offset": offset,
                            "limit": PAGE_SIZE,
                            "sensorIds": ",".join(map(str, lote)),
                        },
                        timeout=REQUEST_TIMEOUT,
                    )
                    r.raise_for_status()
                    dados = r.json()
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

                total_geral += qtd
                offset += qtd          # <- antes era "offset += 1"

                if qtd < PAGE_SIZE:
                    break

                time.sleep(SLEEP)

    cur.close()
    conn.close()
    print(f"\n🏁 Backfill finalizado — {total_geral} leituras")


if __name__ == "__main__":
    run_backfill()
