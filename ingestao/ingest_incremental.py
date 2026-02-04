from datetime import datetime, timedelta, timezone
from psycopg2.extras import execute_batch
import time

from common import get_session, obter_token, get_db_conn, BASE_URL

# ======================================================
# CONFIGURAÇÕES
# ======================================================
FOLGA_MINUTOS = 5
SLEEP = 0.2

# ======================================================
# PIPELINE INCREMENTAL
# ======================================================
def run_incremental():
    session = get_session()
    token = obter_token(session)
    headers = {"Authorization": f"Bearer {token}"}

    conn = get_db_conn()
    cur = conn.cursor()

    # 🔑 sensores válidos + último dado ingerido
    cur.execute("""
        SELECT
            s.sensor_id,
            COALESCE(MAX(l.data_leitura), NOW() - INTERVAL '1 hour') AS last_ts
        FROM sensores s
        LEFT JOIN leituras l ON l.sensor_id = s.sensor_id
        GROUP BY s.sensor_id
        ORDER BY s.sensor_id
    """)
    sensores = cur.fetchall()

    print(f"🔎 {len(sensores)} sensores para ingestão incremental")

    erros = 0
    inseridos = 0

    # 🔑 agora COM timezone UTC
    agora = datetime.now(timezone.utc)

    for sensor_id, last_ts in sensores:

        # segurança: garantir timezone
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)

        # início = último dado - folga
        start_dt = last_ts - timedelta(minutes=FOLGA_MINUTOS)

        # fim = agora (UTC)
        end_dt = agora

        # proteção contra janela inválida
        if start_dt >= end_dt:
            continue

        # API NÃO aceita timezone → formatar sem offset
        start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%S")

        print(f"➡ Sensor {sensor_id} | {start_str} → {end_str}")

        r = session.get(
            f"{BASE_URL}/SensorData",
            headers=headers,
            params={
                "version": "1.3",
                "startDate": start_str,
                "endDate": end_str,
                "sensorIds": str(sensor_id)
            }
        )

        if r.status_code != 200:
            erros += 1
            continue

        dados = r.json()
        if not dados:
            continue

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
        """, registros, page_size=200)

        conn.commit()
        inseridos += len(registros)
        time.sleep(SLEEP)

    cur.close()
    conn.close()

    print(f"\n⚡ Ingestão concluída | Inseridos: {inseridos} | Erros API: {erros}")

# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    run_incremental()
