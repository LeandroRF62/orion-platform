import time
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from sqlalchemy import create_engine, text
import os

# ======================================================
# CONFIGU
# ======================================================
DATABASE_URL = os.getenv("DATABASE_URL")

EMAIL_HOST = os.getenv("EMAIL_HOST")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", 587))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_FROM = os.getenv("EMAIL_FROM")

engine = create_engine(DATABASE_URL)

# ======================================================
# FUNÇÃO EMAIL
# ======================================================
def enviar_email(destinatario, assunto, mensagem):

    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = destinatario
        msg["Subject"] = assunto
        msg.attach(MIMEText(mensagem, "plain"))

        servidor = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
        servidor.starttls()
        servidor.login(EMAIL_USER, EMAIL_PASSWORD)
        servidor.send_message(msg)
        servidor.quit()

        print("Email enviado para", destinatario)

    except Exception as e:
        print("Erro envio:", e)

# ======================================================
# CLASSIFICADOR TARP
# ======================================================
def classificar_tarp(valor):

    if valor >= 20:
        return "Vermelho"
    elif valor >= 10:
        return "Laranja"
    elif valor >= 5:
        return "Amarelo"
    else:
        return "Verde"

# ======================================================
# LOOP PRINCIPAL
# ======================================================
print("🚨 Alert Engine iniciado...")

while True:

    try:

        # 🔎 BUSCAR ÚLTIMAS LEITURAS
        df = pd.read_sql("""
            SELECT 
                l.data_leitura,
                l.valor_sensor,
                s.tipo_sensor,
                s.device_id,
                d.device_name,
                d.status
            FROM leituras l
            JOIN sensores s ON l.sensor_id = s.sensor_id
            JOIN devices d ON s.device_id = d.device_id
            WHERE s.tipo_sensor IN ('A-Axis Delta Angle','B-Axis Delta Angle')
        """, engine)

        if df.empty:
            print("Sem dados...")
            time.sleep(60)
            continue

        # 🔥 PEGAR ÚLTIMO VALOR DE CADA EIXO
        ultimo = (
            df.sort_values("data_leitura")
            .groupby(["device_id","tipo_sensor"])
            .last()
            .reset_index()
        )

        for device_id, grupo in ultimo.groupby("device_id"):

            maior_valor = grupo["valor_sensor"].abs().max()
            nivel_tarp = classificar_tarp(maior_valor)

            device_name = grupo.iloc[0]["device_name"]
            status = str(grupo.iloc[0]["status"]).lower()

            print(device_name, nivel_tarp, status)

            # ======================================================
            # 🔥 ANTI-SPAM INTELIGENTE (NOVO)
            # ======================================================

            estado_anterior = pd.read_sql(
                text("""
                    SELECT *
                    FROM alert_status_log
                    WHERE device_id = :device_id
                """),
                engine,
                params={"device_id": int(device_id)}
            )

            ultimo_tarp_enviado = None
            ultimo_status_enviado = None

            if not estado_anterior.empty:
                ultimo_tarp_enviado = estado_anterior.iloc[0]["ultimo_tarp"]
                ultimo_status_enviado = estado_anterior.iloc[0]["ultimo_status"]

            precisa_enviar = False

            # 🚨 mudou para vermelho?
            if nivel_tarp == "Vermelho" and ultimo_tarp_enviado != "Vermelho":
                precisa_enviar = True

            # 🚨 mudou para offline?
            if status == "offline" and ultimo_status_enviado != "offline":
                precisa_enviar = True

            # ======================================================
            # 🚨 ENVIO CONTROLADO
            # ======================================================
            if precisa_enviar:

                contatos = pd.read_sql(
                    text("""
                        SELECT *
                        FROM alert_contacts
                        WHERE device_id = :device_id
                        AND receber_email = true
                    """),
                    engine,
                    params={"device_id": int(device_id)}
                )

                for _, contato in contatos.iterrows():

                    assunto = f"🚨 ALERTA ORION - {device_name}"

                    mensagem = f"""
Dispositivo: {device_name}
Status TARP: {nivel_tarp}
Status Equipamento: {status}

Verifique imediatamente no Orion.
"""

                    enviar_email(contato["email"], assunto, mensagem)

                # 🔥 SALVAR ESTADO ENVIADO
                with engine.begin() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO alert_status_log
                            (device_id, ultimo_tarp, ultimo_status, ultima_atualizacao)
                            VALUES (:device_id, :tarp, :status, now())
                            ON CONFLICT (device_id)
                            DO UPDATE SET
                                ultimo_tarp = :tarp,
                                ultimo_status = :status,
                                ultima_atualizacao = now()
                        """),
                        {
                            "device_id": int(device_id),
                            "tarp": nivel_tarp,
                            "status": status
                        }
                    )

    except Exception as e:
        print("Erro loop:", e)

    # ⏱ espera 60 segundos
    time.sleep(60)
