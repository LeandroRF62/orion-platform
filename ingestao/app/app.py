import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from pathlib import Path

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
# LOAD ENV
# ===============================
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")

if not DATABASE_URL:
    st.error("DATABASE_URL não configurada")
    st.stop()

engine = create_engine(DATABASE_URL)

st.set_page_config(
    page_title="Gestão Geotécnica Orion",
    layout="wide",
    initial_sidebar_state="expanded"
)

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

df["data_leitura"] = pd.to_datetime(df["data_leitura"]).dt.tz_localize(None)
df["last_upload"] = pd.to_datetime(df["last_upload"], errors="coerce")

if df.empty:
    st.stop()

# ===============================
# SIDEBAR FILTROS
# ===============================
st.sidebar.header("🛠️ Configurações")

df_devices = df[["device_name","status"]].drop_duplicates()
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

df_final = df[df["device_name"]==device_principal].copy()

# ===============================
# 🚨 LIMITES DE ALERTA (TARPs)
# ===============================
st.sidebar.markdown("### 🚨 Limites de Alerta")

tipos_ordenados = sorted(
    df_final["tipo_sensor"].unique(),
    key=lambda x: ("A" not in x, x)
)

device_id_atual = df_final.iloc[-1]["device_id"]

with engine.begin() as conn:
    limites_existentes = pd.read_sql(
        text("""
            SELECT *
            FROM alert_limits
            WHERE device_id = :device_id
        """),
        conn,
        params={"device_id":device_id_atual}
    )

novo_alerta_tipo = st.sidebar.selectbox(
    "Tipo de Sensor",
    tipos_ordenados
)

novo_valor = st.sidebar.number_input(
    "Valor do Limite",
    value=0.0,
    step=0.1
)

mostrar_linha = st.sidebar.checkbox(
    "Mostrar linha tracejada no gráfico",
    value=True
)

mensagem_alerta = st.sidebar.text_input(
    "Mensagem do alerta",
    value="Ex: Fazer inspeção imediata"
)

if st.sidebar.button("➕ Adicionar Alerta"):

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO alert_limits (
                device_id,
                tipo_sensor,
                limite_valor,
                mostrar_linha,
                mensagem
            )
            VALUES (:device_id,:tipo,:valor,:mostrar,:mensagem)
        """),{
            "device_id":device_id_atual,
            "tipo":novo_alerta_tipo,
            "valor":novo_valor,
            "mostrar":mostrar_linha,
            "mensagem":mensagem_alerta
        })

    st.sidebar.success("Alerta criado!")

# ===============================
# GRÁFICO
# ===============================
df_final["serie"]=df_final["device_name"]+" | "+df_final["tipo_sensor"]

fig=px.line(
    df_final,
    x="data_leitura",
    y="valor_sensor",
    color="serie",
    template="plotly_white"
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

# ===============================
# DESENHAR LINHAS DE ALERTA
# ===============================
if not limites_existentes.empty:

    for _,alerta in limites_existentes.iterrows():

        if alerta["mostrar_linha"]:
            fig.add_hline(
                y=alerta["limite_valor"],
                line_dash="dash",
                line_width=2,
                annotation_text=alerta["mensagem"],
                annotation_position="top left"
            )

st.plotly_chart(fig,use_container_width=True)
