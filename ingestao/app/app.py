import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from pathlib import Path

# ======================================================
# 🚨 FUNÇÃO DE CLASSIFICAÇÃO TARP
# ======================================================
def classificar_tarp(valor, limites):
    if valor >= limites.get("vermelho", float("inf")):
        return "Vermelho"
    elif valor >= limites.get("laranja", float("inf")):
        return "Laranja"
    elif valor >= limites.get("amarelo", float("inf")):
        return "Amarelo"
    else:
        return "Verde"

# ===============================
# AUTENTICAÇÃO
# ===============================
APP_PASSWORD = os.getenv("APP_PASSWORD", "orion123")

if "auth_ok" not in st.session_state:
    st.session_state.auth_ok = False

if not st.session_state.auth_ok:
    st.title("🔐 Acesso restrito")
    senha = st.text_input("Senha", type="password")

    if st.button("Entrar"):
        if senha == APP_PASSWORD:
            st.session_state.auth_ok = True
            st.rerun()
        else:
            st.error("Senha incorreta")
    st.stop()

# ===============================
# ENV
# ===============================
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")

engine = create_engine(DATABASE_URL)

st.set_page_config(
    page_title="Gestão Geotécnica Orion",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===============================
# 🔄 BOTÃO ATUALIZAR DADOS
# ===============================
if st.sidebar.button("🔄 Atualizar Dados"):
    st.cache_data.clear()
    st.rerun()

# ===============================
# QUERY BANCO
# ===============================
@st.cache_data(ttl=300)
def carregar_dados_db():
    query = """
    SELECT 
        l.data_leitura,
        l.valor_sensor,
        s.sensor_id,
        s.tipo_sensor,
        s.device_id,
        d.device_name,
        d.latitude,
        d.longitude,
        d.status,
        d.battery_percentage,
        d.last_upload
    FROM leituras l
    JOIN sensores s ON l.sensor_id = s.sensor_id
    JOIN devices d ON s.device_id = d.device_id
    WHERE s.tipo_sensor IN ('A-Axis Delta Angle','B-Axis Delta Angle')
    ORDER BY l.data_leitura
    """
    return pd.read_sql(query, engine)

df = carregar_dados_db()

if df.empty:
    st.warning("Sem dados ainda.")
    st.stop()

df["data_leitura"] = pd.to_datetime(df["data_leitura"]).dt.tz_localize(None)
df["last_upload"] = pd.to_datetime(df["last_upload"], errors="coerce")

# ===============================
# FILTRO EIXO
# ===============================
tipos_selecionados = st.sidebar.multiselect(
    "Variável do Dispositivo",
    sorted(df["tipo_sensor"].astype(str).unique()),
    default=sorted(df["tipo_sensor"].astype(str).unique())
)

df_tipo = df[df["tipo_sensor"].astype(str).isin(tipos_selecionados)]

# ===============================
# FILTRO DISPOSITIVOS
# ===============================
df_devices = df_tipo[["device_name","status"]].drop_duplicates()
df_devices["status_lower"] = df_devices["status"].astype(str).str.lower()

df_devices["status_str"] = df_devices["status_lower"].map({
    "online":"🟢 Online",
    "offline":"🔴 Offline"
}).fillna("⚪ Desconhecido")

df_devices["label"] = df_devices["device_name"]+" – "+df_devices["status_str"]

device_label_map = dict(zip(df_devices["label"],df_devices["device_name"]))

device_principal_label = st.sidebar.selectbox(
    "Selecionar Dispositivo Principal",
    sorted(device_label_map.keys())
)

device_principal = device_label_map[device_principal_label]

outros_labels = st.sidebar.multiselect(
    "Adicionar Outros Dispositivos",
    sorted(device_label_map.keys()),
    default=[]
)

devices_selecionados = list(dict.fromkeys(
    [device_principal]+[device_label_map[l] for l in outros_labels]
))

df_final = df_tipo[df_tipo["device_name"].isin(devices_selecionados)].copy()

# ===============================
# FILTRO PERÍODO
# ===============================
st.sidebar.subheader("📅 Período de Análise")

data_min = df_final["data_leitura"].min().date()
data_max = df_final["data_leitura"].max().date()

c1, c2 = st.sidebar.columns(2)
data_ini = c1.date_input("Data inicial", data_min)
data_fim = c2.date_input("Data final", data_max)

df_final = df_final[
    (df_final["data_leitura"] >= pd.to_datetime(data_ini)) &
    (df_final["data_leitura"] < pd.to_datetime(data_fim) + pd.Timedelta(days=1))
]

# ===============================
# ORDEM DOS EIXOS
# ===============================
ordem_series = sorted(
    df_final["tipo_sensor"].astype(str).unique(),
    key=lambda x: ("B" in x, x)
)

df_final["tipo_sensor"] = pd.Categorical(
    df_final["tipo_sensor"].astype(str),
    categories=ordem_series,
    ordered=True
)

df_final = df_final.sort_values(["tipo_sensor","data_leitura"])

# ===============================
# 🚨 TARPs (NADA ALTERADO)
# ===============================
st.sidebar.markdown("### 🚨 Limites de Alerta")

device_id_atual = int(df_final.iloc[-1]["device_id"])

try:
    limites_existentes = pd.read_sql(
        text("""
            SELECT *
            FROM alert_limits
            WHERE device_id = :device_id
            ORDER BY tipo_sensor ASC, limite_valor ASC
        """),
        engine,
        params={"device_id":device_id_atual}
    )
except:
    limites_existentes = pd.DataFrame()

tipos_ordenados = sorted(
    df_final["tipo_sensor"].astype(str).unique(),
    key=lambda x: ("A" not in x, x)
)

novo_alerta_tipo = st.sidebar.selectbox("Tipo de Sensor",tipos_ordenados)
novo_valor = st.sidebar.number_input("Valor do Limite",value=0.0,step=0.1)
mostrar_linha = st.sidebar.checkbox("Mostrar linha tracejada no gráfico",value=True)
mensagem_alerta = st.sidebar.text_input("Mensagem do alerta",value="Ex: Fazer inspeção")

if st.sidebar.button("➕ Adicionar Alerta"):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO alert_limits
                (device_id,tipo_sensor,limite_valor,mostrar_linha,mensagem)
                VALUES (:device_id,:tipo,:valor,:mostrar,:mensagem)
            """),
            {
                "device_id":device_id_atual,
                "tipo":novo_alerta_tipo,
                "valor":novo_valor,
                "mostrar":mostrar_linha,
                "mensagem":mensagem_alerta
            }
        )
    st.sidebar.success("Alerta criado!")

# ===============================
# 👥 CONTATOS DE ALERTA (NADA ALTERADO)
# ===============================
st.sidebar.markdown("### 👥 Contatos de Alerta")

nome_contato = st.sidebar.text_input("Nome do responsável")
email_contato = st.sidebar.text_input("Email")
telefone_contato = st.sidebar.text_input("Telefone (com DDD)")

receber_email = st.sidebar.checkbox("Receber Email", value=True)
receber_sms = st.sidebar.checkbox("Receber SMS", value=False)
receber_whatsapp = st.sidebar.checkbox("Receber WhatsApp", value=False)

if st.sidebar.button("➕ Adicionar Contato"):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO alert_contacts
                (device_id,nome,email,telefone,receber_email,receber_sms,receber_whatsapp)
                VALUES (:device_id,:nome,:email,:telefone,:email_ok,:sms_ok,:wpp_ok)
            """),
            {
                "device_id": device_id_atual,
                "nome": nome_contato,
                "email": email_contato,
                "telefone": telefone_contato,
                "email_ok": receber_email,
                "sms_ok": receber_sms,
                "wpp_ok": receber_whatsapp
            }
        )
    st.sidebar.success("Contato adicionado!")

# ===============================
# ZERO REFERÊNCIA
# ===============================
modo_escala = st.sidebar.radio(
    "Escala de Visualização",
    ["Absoluta","Relativa"]
)

if modo_escala=="Relativa":
    refs = (
        df_final.sort_values("data_leitura")
        .groupby("sensor_id")["valor_sensor"]
        .first()
    )
    df_final["valor_grafico"]=df_final["valor_sensor"]-df_final["sensor_id"].map(refs)
else:
    df_final["valor_grafico"]=df_final["valor_sensor"]

# ======================================================
# 🚨 MOTOR TARP CORRIGIDO (ÚNICA ALTERAÇÃO REAL)
# ======================================================
df_tipo_trigger = df_tipo.copy()

if modo_escala=="Relativa":
    refs_trigger = (
        df_tipo_trigger.sort_values("data_leitura")
        .groupby("sensor_id")["valor_sensor"]
        .first()
    )
    df_tipo_trigger["valor_grafico"] = df_tipo_trigger["valor_sensor"] - df_tipo_trigger["sensor_id"].map(refs_trigger)
else:
    df_tipo_trigger["valor_grafico"] = df_tipo_trigger["valor_sensor"]

ultimo_por_sensor = (
    df_tipo_trigger.sort_values("data_leitura")
    .groupby(["tipo_sensor"])
    .last()
    .reset_index()
)

maior_valor_atual = ultimo_por_sensor["valor_grafico"].abs().max()

limites_tarp = {
    "verde": 0,
    "amarelo": 5,
    "laranja": 10,
    "vermelho": 20
}

nivel_tarp = classificar_tarp(abs(maior_valor_atual), limites_tarp)

emoji_tarp = {
    "Verde": "🟢",
    "Amarelo": "🟡",
    "Laranja": "🟠",
    "Vermelho": "🔴"
}.get(nivel_tarp, "⚪")

# ===============================
# HEADER
# ===============================
info = df_final.sort_values("data_leitura").iloc[-1]

status = str(info["status"]).lower()
bateria = int(info["battery_percentage"]) if pd.notna(info["battery_percentage"]) else 0
ultima_tx = info["last_upload"]

if pd.notna(ultima_tx):
    ultima_tx = (ultima_tx - pd.Timedelta(hours=3)).strftime("%d-%m-%Y %H:%M:%S")

st.markdown(f"""
### {device_principal}
{emoji_tarp} TARP: {nivel_tarp} | 🟢 Status: {status.upper()} | 🔋 {bateria}% | ⏱ Última transmissão: {ultima_tx}
""")

# ======================================================
# 📨 ENVIO AUTOMÁTICO DE EMAIL (TRIGGER TARP)
# ======================================================
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def enviar_email_alerta(destinatario, assunto, mensagem):

    try:
        EMAIL_HOST = st.secrets["EMAIL_HOST"]
        EMAIL_PORT = int(st.secrets["EMAIL_PORT"])
        EMAIL_USER = st.secrets["EMAIL_USER"]
        EMAIL_PASSWORD = st.secrets["EMAIL_PASSWORD"]
        EMAIL_FROM = st.secrets["EMAIL_FROM"]

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

        return True

    except Exception as e:
        print("Erro envio email:", e)
        return False


# 🔴 DISPARO AUTOMÁTICO SE TARP VERMELHO
if nivel_tarp == "Vermelho":

    try:
        contatos = pd.read_sql(
            text("""
                SELECT *
                FROM alert_contacts
                WHERE device_id = :device_id
                AND receber_email = true
            """),
            engine,
            params={"device_id":device_id_atual}
        )

        for _, contato in contatos.iterrows():

            assunto = f"🚨 ALERTA TARP VERMELHO - {device_principal}"

            mensagem = f"""
Dispositivo: {device_principal}
Status TARP: {nivel_tarp}

Valor atual acima do limite configurado.

Acesse o Orion para mais detalhes.
"""

            enviar_email_alerta(contato["email"], assunto, mensagem)

    except Exception as e:
        print("Erro trigger email:", e)


# ===============================
# GRÁFICO
# ===============================
df_final["serie"]=df_final["device_name"].astype(str)+" | "+df_final["tipo_sensor"].astype(str)

fig=px.line(
    df_final,
    x="data_leitura",
    y="valor_grafico",
    color="serie",
    template="plotly_white"
)

if not limites_existentes.empty:
    for _,alerta in limites_existentes.iterrows():
        if alerta["mostrar_linha"]:
            fig.add_hline(
                y=alerta["limite_valor"],
                line_dash="dash",
                annotation_text=alerta["mensagem"],
                annotation_position="top left"
            )

fig.update_layout(
    height=780,
    legend=dict(
        orientation="h",
        y=-0.15,
        x=0.5,
        xanchor="center"
    )
)

st.plotly_chart(fig,use_container_width=True)
