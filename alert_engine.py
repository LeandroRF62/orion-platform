"""
Motor de alertas Orion — uma execução por disparo do workflow.

Problemas da versão anterior e o que mudou:

1. O script terminava com "while True: ... time.sleep(60)", feito para
   rodar como serviço contínuo. Só que o workflow o disparava a cada 1
   minuto e não tinha bloco "concurrency": cada execução ficava viva até o
   limite de 6 h do Actions enquanto novas começavam a cada minuto. Dezenas
   de execuções simultâneas liam o banco e podiam mandar e-mail duplicado.
   >>> Agora roda UMA vez e encerra. O agendamento ficou a cada 15 min, com
   concurrency no workflow.

2. A consulta trazia o histórico INTEIRO da tabela leituras para dentro do
   pandas, a cada volta do laço, só para pegar o último valor de cada eixo.
   >>> Agora usa DISTINCT ON e traz uma linha por sensor.

3. Falha no envio de e-mail era engolida e o estado era gravado assim
   mesmo, ou seja, o alerta era dado como enviado sem ter saído.
   >>> Agora o estado só é gravado quando pelo menos um e-mail saiu, e o
   job termina vermelho se houve falha.

4. Faltava checar as variáveis de ambiente. Sem EMAIL_HOST o script
   quebrava no meio da varredura.

Limiares TARP: configuráveis por variável de ambiente (TARP_VERMELHO,
TARP_LARANJA, TARP_AMARELO). Atenção: eles são comparados com o valor
ABSOLUTO do ângulo, não com a variação desde a instalação. Com os padrões
atuais (20/10/5 graus) um tiltmetro em -1,35 grau fica sempre Verde.
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pandas as pd
from sqlalchemy import create_engine, text

# ======================================================
# CONFIG
# ======================================================
DATABASE_URL = os.getenv("DATABASE_URL")

EMAIL_HOST = os.getenv("EMAIL_HOST")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_FROM = os.getenv("EMAIL_FROM") or EMAIL_USER

TARP_VERMELHO = float(os.getenv("TARP_VERMELHO", "20"))
TARP_LARANJA = float(os.getenv("TARP_LARANJA", "10"))
TARP_AMARELO = float(os.getenv("TARP_AMARELO", "5"))

if not DATABASE_URL:
    raise SystemExit("DATABASE_URL não definida (secret do GitHub).")

faltando = [n for n, v in [("EMAIL_HOST", EMAIL_HOST), ("EMAIL_USER", EMAIL_USER),
                           ("EMAIL_PASSWORD", EMAIL_PASSWORD)] if not v]
if faltando:
    raise SystemExit(f"Secrets de e-mail não definidos: {', '.join(faltando)}")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

falhas = []


# ======================================================
# E-MAIL
# ======================================================
def enviar_email(servidor, destinatario, assunto, mensagem):
    """Devolve True quando o e-mail saiu. Reaproveita a conexão SMTP."""
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = destinatario
        msg["Subject"] = assunto
        msg.attach(MIMEText(mensagem, "plain"))
        servidor.send_message(msg)
        print(f"📧 Email enviado para {destinatario}")
        return True
    except Exception as e:
        print(f"⚠️ Erro no envio para {destinatario}: {e}")
        falhas.append(f"email {destinatario}: {e}")
        return False


def abrir_smtp():
    servidor = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT, timeout=30)
    servidor.starttls()
    servidor.login(EMAIL_USER, EMAIL_PASSWORD)
    return servidor


# ======================================================
# TARP
# ======================================================
def classificar_tarp(valor):
    if valor >= TARP_VERMELHO:
        return "Vermelho"
    if valor >= TARP_LARANJA:
        return "Laranja"
    if valor >= TARP_AMARELO:
        return "Amarelo"
    return "Verde"


# Uma linha por sensor: a leitura mais recente. Antes o script trazia a
# tabela leituras inteira e filtrava no pandas.
SQL_ULTIMAS = """
    SELECT DISTINCT ON (l.sensor_id)
           l.sensor_id, l.data_leitura, l.valor_sensor,
           s.tipo_sensor, s.device_id,
           d.device_name, d.status
    FROM leituras l
    JOIN sensores s ON s.sensor_id = l.sensor_id
    JOIN devices  d ON d.device_id = s.device_id
    WHERE s.tipo_sensor IN ('A-Axis Delta Angle', 'B-Axis Delta Angle')
    ORDER BY l.sensor_id, l.data_leitura DESC
"""

SQL_CRIAR_LOG = """
    CREATE TABLE IF NOT EXISTS alert_status_log (
        device_id BIGINT PRIMARY KEY,
        ultimo_tarp TEXT,
        ultimo_status TEXT,
        ultima_atualizacao TIMESTAMPTZ
    )
"""

SQL_SALVAR_ESTADO = """
    INSERT INTO alert_status_log
        (device_id, ultimo_tarp, ultimo_status, ultima_atualizacao)
    VALUES (:device_id, :tarp, :status, now())
    ON CONFLICT (device_id) DO UPDATE SET
        ultimo_tarp = :tarp,
        ultimo_status = :status,
        ultima_atualizacao = now()
"""


def main():
    print("🚨 Alert Engine — execução única")

    with engine.begin() as conn:
        conn.execute(text(SQL_CRIAR_LOG))

    ultimo = pd.read_sql(SQL_ULTIMAS, engine)
    if ultimo.empty:
        print("Sem leituras de inclinação no banco. Nada a avaliar.")
        return

    estados = pd.read_sql("SELECT * FROM alert_status_log", engine)
    estados = estados.set_index("device_id") if not estados.empty else estados

    contatos_todos = pd.read_sql(
        "SELECT * FROM alert_contacts WHERE receber_email = true", engine)

    servidor = None
    enviados = 0

    try:
        for device_id, grupo in ultimo.groupby("device_id"):
            maior_valor = grupo["valor_sensor"].abs().max()
            nivel_tarp = classificar_tarp(maior_valor)

            device_name = grupo.iloc[0]["device_name"]
            status = str(grupo.iloc[0]["status"]).lower()

            tarp_anterior = status_anterior = None
            if len(estados) and device_id in estados.index:
                linha = estados.loc[device_id]
                tarp_anterior = linha["ultimo_tarp"]
                status_anterior = linha["ultimo_status"]

            # Só avisa na MUDANÇA de estado, nunca a cada execução.
            virou_vermelho = nivel_tarp == "Vermelho" and tarp_anterior != "Vermelho"
            virou_offline = status == "offline" and status_anterior != "offline"
            if not (virou_vermelho or virou_offline):
                continue

            contatos = contatos_todos[contatos_todos["device_id"] == device_id]
            if contatos.empty:
                print(f"⚠️ {device_name}: mudança detectada, sem contato cadastrado")
                continue

            motivo = "TARP Vermelho" if virou_vermelho else "Equipamento offline"
            assunto = f"ALERTA ORION - {device_name} - {motivo}"
            mensagem = (
                f"Dispositivo: {device_name}\n"
                f"Status TARP: {nivel_tarp} (maior |leitura| = {maior_valor:.4f})\n"
                f"Status do equipamento: {status}\n"
                f"Última leitura: {grupo['data_leitura'].max()}\n\n"
                "Verifique no Orion."
            )

            if servidor is None:
                servidor = abrir_smtp()

            algum_saiu = False
            for _, contato in contatos.iterrows():
                if enviar_email(servidor, contato["email"], assunto, mensagem):
                    algum_saiu = True
                    enviados += 1

            # Estado só avança se o aviso realmente saiu; caso contrário a
            # próxima execução tenta de novo.
            if algum_saiu:
                with engine.begin() as conn:
                    conn.execute(text(SQL_SALVAR_ESTADO), {
                        "device_id": int(device_id),
                        "tarp": nivel_tarp,
                        "status": status,
                    })
    finally:
        if servidor is not None:
            try:
                servidor.quit()
            except Exception:
                pass

    print(f"🏁 Finalizado — {enviados} e-mail(s) enviado(s)")

    if falhas:
        raise SystemExit(f"❌ {len(falhas)} falha(s) de envio: {falhas[:5]}")


if __name__ == "__main__":
    main()
