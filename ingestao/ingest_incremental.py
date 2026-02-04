from datetime import timedelta
from psycopg2.extras import execute_batch

from common import get_session, obter_token, get_db_conn, BASE_URL

FOLGA_MINUTOS = 5
SENSOR_BATCH_SIZE = 50

def run_incremental():
    session = get_session()
    token = obter_token(session)
    headers = {"Authorization": f"Bearer {token}"}

    conn = get_db_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT sensor_id, COALESCE(MAX(data_leitura), NOW() - INTERVAL '1 day')
        FROM leituras
        GROUP BY sensor_id
    """)
    sensores = cur.fetchall()

    for i in range(0, len(sensores), SENSOR_BATCH_SIZE):
        lote = sensores[i:i+SENSOR_BATCH_SIZE]

        start = min(s[1] for s in lote) - timedelta(minutes=FOLGA_MINUTOS)
        sensor_ids = ",".join(str(s[0]) for s in lote)

        r = session.get(
            f"{BASE_URL}/SensorData",
            headers=headers,
            params={
                "version": "1.3",
                "startDate": start.isoformat(),
                "sensorIds": sensor_ids
            }
        )
        r.raise_for_status()

        dados = r.json()
        if not dados:
            continue

        registros = [
            (d["sensorId"], d["readingDate"], d["sensorValue"])
            for d in dados
        ]

        execute_batch(cur, """
            INSERT INTO leituras (sensor_id, data_leitura, valor_sensor)
            VALUES (%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, registros, page_size=1000)

        conn.commit()

    cur.close()
    conn.close()
    print("⚡ Ingestão incremental concluída")

if __name__ == "__main__":
    run_incremental()
