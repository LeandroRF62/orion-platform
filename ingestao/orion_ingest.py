import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
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
# ENV
# ===============================
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

# ===============================
# CONFIGURAÇÕES
# ===============================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    st.error("DATABASE_URL não configurada")
    st.stop()

engine = create_engine(DATABASE_URL)

MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")
if not MAPBOX_TOKEN:
    st.warning("MAPBOX_TOKEN não configurado")

ARQUIVO_CACHE = "cache_orion_dev.csv"

st.set_page_config(
    page_title="Gestão Geotécnica Orion",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===============================
# CABEÇALHO
# ===============================
if os.path.exists("header_orion.png"):
    st.image("header_orion.png", use_container_width=True)

# ===============================
# SIDEBAR
# ===============================
st.sidebar.header("🛠️ Configurações")

modo_dev = st.sidebar.checkbox(
    "Modo desenvolvimento (não consultar banco)",
    value=False
)

if st.sidebar.button("🔄 Atualizar dados"):
    st.cache_data.clear()
    st.rerun()

# ===============================
# CARGA DO BANCO (CORRIGIDA)
# ===============================
@st.cache_data(ttl=300)
def carregar_dados_db():
    query = """
    SELECT 
        l.data_leitura,
        l.valor_sensor,
        s.sensor_id,
        s.tipo_sensor,
        d.device_name,
        d.latitude,
        d.longitude,
        d.status,
        d.battery_percentage,
        d.last_upload
    FROM leituras l
    INNER JOIN sensores s ON l.sensor_id = s.sensor_id
    INNER JOIN devices d ON s.device_id = d.device_id
    WHERE s.tipo_sensor IN (
        'A-Axis Delta Angle',
        'B-Axis Delta Angle'
    )
    ORDER BY l.data_leitura
    """
    return pd.read_sql(query, engine)

# ===============================
# CARGA DOS DADOS
# ===============================
if modo_dev and os.path.exists(ARQUIVO_CACHE):
    df = pd.read_csv(ARQUIVO_CACHE)
else:
    df = carregar_dados_db()
    df.to_csv(ARQUIVO_CACHE, index=False)

if df.empty:
    st.warning("Nenhum dado encontrado")
    st.stop()

# ===============================
# NORMALIZAÇÃO
# ===============================
df["data_leitura"] = pd.to_datetime(df["data_leitura"], errors="coerce").dt.tz_localize(None)
df["last_upload"] = pd.to_datetime(df["last_upload"], errors="coerce")
df["battery_percentage"] = pd.to_numeric(df["battery_percentage"], errors="coerce")

# ===============================
# FILTROS
# ===============================
tipos_selecionados = st.sidebar.multiselect(
    "Variável do Dispositivo",
    sorted(df["tipo_sensor"].unique()),
    default=sorted(df["tipo_sensor"].unique())
)

df_tipo = df[df["tipo_sensor"].isin(tipos_selecionados)]

st.sidebar.subheader("📡 Status do Dispositivo")

col1, col2 = st.sidebar.columns(2)
with col1:
    filtro_online = st.checkbox("Online", value=True)
with col2:
    filtro_offline = st.checkbox("Offline", value=True)

status_permitidos = []
if filtro_online:
    status_permitidos.append("online")
if filtro_offline:
    status_permitidos.append("offline")

df_devices = df[["device_name", "status"]].drop_duplicates()
df_devices["status_lower"] = df_devices["status"].astype(str).str.lower()

if status_permitidos:
    df_devices = df_devices[df_devices["status_lower"].isin(status_permitidos)]

df_devices["status_str"] = df_devices["status_lower"].map({
    "online": "🟢 Online",
    "offline": "🔴 Offline"
}).fillna("⚪ Desconhecido")

df_devices["label"] = df_devices["device_name"] + " – " + df_devices["status_str"]

device_label_map = dict(zip(df_devices["label"], df_devices["device_name"]))

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

devices_selecionados = list(
    dict.fromkeys([device_principal] + [device_label_map[l] for l in outros_labels])
)

# ===============================
# PERÍODO
# ===============================
st.sidebar.subheader("📅 Período de Análise")

data_min = df_tipo["data_leitura"].min().date()
data_max = df_tipo["data_leitura"].max().date()

c1, c2 = st.sidebar.columns(2)
data_ini = c1.date_input("Data inicial", data_min)
data_fim = c2.date_input("Data final", data_max)

df_final = df_tipo[
    (df_tipo["device_name"].isin(devices_selecionados)) &
    (df_tipo["data_leitura"] >= pd.to_datetime(data_ini)) &
    (df_tipo["data_leitura"] < pd.to_datetime(data_fim) + pd.Timedelta(days=1))
].copy()

if df_final.empty:
    st.warning("Nenhum dado no período selecionado")
    st.stop()

# ===============================
# ESCALA
# ===============================
modo_escala = st.sidebar.radio("Escala", ["Absoluta", "Relativa"])

if modo_escala == "Relativa":
    refs = {}
    for sid in df_final["sensor_id"].unique():
        refs[sid] = df_final[df_final["sensor_id"] == sid].sort_values(
            "data_leitura"
        ).iloc[0]["valor_sensor"]

    df_final["valor_grafico"] = df_final.apply(
        lambda r: r["valor_sensor"] - refs.get(r["sensor_id"], 0),
        axis=1
    )
    label_y = "Variação Relativa"
else:
    df_final["valor_grafico"] = df_final["valor_sensor"]
    label_y = "Valor Absoluto"

# ===============================
# HEADER
# ===============================
info = df_final.sort_values("data_leitura").iloc[-1]

status = str(info["status"]).lower()
bateria = int(info["battery_percentage"]) if pd.notna(info["battery_percentage"]) else 0
ultima_tx = info["last_upload"]

if pd.notna(ultima_tx):
    ultima_tx = (ultima_tx - pd.Timedelta(hours=3)).strftime("%d-%m-%Y %H:%M:%S")

cor_status = "#22c55e" if status == "online" else "#ef4444"

# ===============================
# GRÁFICO
# ===============================
df_final["serie"] = df_final["device_name"] + " | " + df_final["tipo_sensor"]

fig = px.line(
    df_final,
    x="data_leitura",
    y="valor_grafico",
    color="serie",
    template="plotly_white"
)

st.plotly_chart(fig, use_container_width=True)

# ===============================
# MAPA
# ===============================
st.subheader("🛰️ Localização dos Dispositivos")

df_mapa = df_final[
    ["device_name", "latitude", "longitude", "status"]
].drop_duplicates().dropna(subset=["latitude", "longitude"])

df_mapa["cor"] = df_mapa["status"].astype(str).str.lower().apply(
    lambda x: "#22c55e" if x == "online" else "#ef4444"
)

mapa = go.Figure(go.Scattermapbox(
    lat=df_mapa["latitude"],
    lon=df_mapa["longitude"],
    mode="markers+text",
    marker=dict(size=18, color=df_mapa["cor"]),
    text=df_mapa["device_name"],
    textposition="top center"
))

mapa.update_layout(
    mapbox=dict(
        accesstoken=MAPBOX_TOKEN,
        style="satellite-streets",
        zoom=12,
        center=dict(
            lat=df_mapa["latitude"].mean(),
            lon=df_mapa["longitude"].mean()
        )
    ),
    height=600,
    margin=dict(l=0, r=0, t=0, b=0)
)

st.plotly_chart(mapa, use_container_width=True)

# ===============================
# TABELA
# ===============================
st.dataframe(
    df_final[[
        "data_leitura",
        "device_name",
        "tipo_sensor",
        "valor_sensor",
        "valor_grafico"
    ]],
    use_container_width=True
)

# ===============================
# EXPORTAÇÃO
# ===============================
csv = df_final.to_csv(index=False).encode("utf-8")
st.download_button(
    "📥 Baixar CSV",
    csv,
    "dados_geotecnicos.csv",
    "text/csv"
)

