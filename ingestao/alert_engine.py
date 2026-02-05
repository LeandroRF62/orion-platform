from datetime import datetime
import psycopg2

# ======================================================
# DETECTAR MUDANÇA DE STATUS (ONLINE / OFFLINE)
# ======================================================

def processar_alertas_status(conn, device_id, status_atual):

    cur = conn.cursor()

    # pega status anterior salvo no banco
    cur.execute("""
        SELECT last_status
        FROM devices
        WHERE device_id = %s
    """, (device_id,))

    row = cur.fetchone()

    status_anterior = row[0] if row else None

    # --------------------------------------------------
    # SE MUDOU O STATUS → GERA EVENTO
    # --------------------------------------------------
    if status_anterior and status_anterior.lower() != str(status_atual).lower():

        print(f"🚨 Mudança de status detectada | Device {device_id} | {status_anterior} → {status_atual}")

        # salva histórico de evento
        cur.execute("""
            INSERT INTO alert_events (
                device_id,
                tipo_evento,
                valor,
                data_evento
            )
            VALUES (%s,%s,%s,%s)
        """, (
            device_id,
            "status_change",
            f"{status_anterior} -> {status_atual}",
            datetime.utcnow()
        ))

    # --------------------------------------------------
    # ATUALIZA last_status PARA PRÓXIMA EXECUÇÃO
    # --------------------------------------------------
    cur.execute("""
        UPDATE devices
        SET last_status = %s
        WHERE device_id = %s
    """, (status_atual, device_id))

    conn.commit()
    cur.close()
