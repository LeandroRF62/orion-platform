from datetime import datetime, timedelta, timezone
from psycopg2.extras import execute_batch
import time

from common import get_session, obter_token, get_db_conn, BASE_URL

DATA_INICIAL = "2026-01-01T00:00:00"
BLOCO_DIAS = 7
SENSOR_BATCH_SIZE = 50
SLEEP = 0.3

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

    cur.execute("SELECT sensor_id FROM sensores")
    sensor_ids = [r[0] for r in cur.fetchall()]

    agora = datetime.now(timezone.utc)

    for ini, fim in gerar_blocos(DATA_INICIAL, agora):
        for i in range(0, len(sensor_ids), SENSOR_BATCH_SIZE):
            lote = sensor_ids[i:i+SENSOR_BATCH_SIZE]
            offset = 0

            while True:
                r = session.get(
                    f"{BASE_URL}/SensorData",
                    headers=headers,
                    params={
                        "version": "1.3",
                        "startDate": ini.isoformat(),
                        "endDate": fim.isoformat(),
                        "offset": offset,
                        "sensorIds": ",".join(map(str, lote))
                    }
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
                    INSERT INTO leituras (sensor_id, data_leitura, valor_sensor)
                    VALUES (%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, registros, page_size=500)

                conn.commit()
                offset += 1
                time.sleep(SLEEP)

    cur.close()
    conn.close()
    print("🏁 Backfill finalizado")

if __name__ == "__main__":
    run_backfill()
