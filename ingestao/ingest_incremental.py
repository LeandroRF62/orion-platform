from datetime import datetime, timedelta
from psycopg2.extras import execute_batch

from common import get_session, obter_token, get_db_conn, BASE_URL

# ======================================================
# CONFIGURAÇÕES
# ======================================================
FOLGA_MINUTOS = 5
SENSOR_BATCH_SIZE = 50

# ======================================================
# PIPELINE INCREMENTAL
# ======================================================
def run_incremental():
    session = get_session()
    token = obter_token(session)
    headers = {"Authorization": f"Bearer {token}"}

    conn = get_db_conn()
    cur = conn.cursor()

    # 🔑 ESTADO: último timestamp por sensor
    cur.execute("""
        SELECT
            sensor_id,
            COALESCE(MAX(data_leitura), NOW() - INTERVAL '1 day') AS last_ts
        FROM leituras
        GROUP BY sensor_id
    """)
    sensores = cur.fetchall()

    if not sensores:
        print("⚠ Nenhum sensor encontrado na base.")
        return

    print(f"🔎 {len(sensores)} sensores para ingestão incremental")

    # 🔄 PROCESSA EM LOTES
    for i in range(0, len(sensores), SENSOR_BATCH_SIZE):
        lote = sensores[i:i + SENSOR_BATCH_SIZE]

        # menor timestamp do lote (com folga)
        start_dt = min(s[1] for s in lote) - timedelta(minutes=FOLGA_MINUTOS)

        # ⚠ API NÃO ACEITA TIMEZONE
        start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
        end_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

        sensor_ids = ",".join(str(s[0]) for s in lote)

        print(f"➡ Buscando dados de {start_str} até {end_str} | Sensores: {len(lote)}")

        r = session.get(
            f"{BASE_URL}/SensorData",
            headers=headers,
            params={
                "version": "1.3",
                "startDate": start_str,
                "endDate": end_str,
                "sensorIds": sensor_ids
            }
        )

        # DEBUG ÚTIL EM CASO DE ERRO
        if r.status_code != 200:
            print("❌ Erro na API SensorData")
            print("Status:", r.status_code)
            print("Resposta:", r.text)
            r.raise_for_status()

        dados = r.json()
        if not dados:
            print("   ↪ Nenhum dado novo")
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
        """, registros, page_size=1000)

        conn.commit()
        print(f"   ✔ {len(registros)} registros inseridos")

    cur.close()
    conn.close()
    print("\n⚡ Ingestão incremental concluída com sucesso")

# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    run_incremental()
