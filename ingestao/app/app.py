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
# LOAD ENV
# ===============================
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

# ===============================
# CONEXÃO DB
# ===============================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    st.error("DATABASE_URL não configurada")
    st.stop()

engine = create_engine(DATABASE_URL)

# ===============================
# MAPBOX TOKEN
# ===============================
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
    if not modo_dev:
        st.cache_data.clear()
    st.rerun()

# ===============================
# QUERY BANCO (CORRIGIDA)
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
    JOIN sensores s ON l.sensor_id = s.sensor_id
    JOIN devices d ON s.device_id = d.device_id
    WHERE s.tipo_sensor IN ('A-Axis Delta Angle', 'B-Axis Delta Angle')
    ORDER BY l.data_leitura
    """
    return pd.read_sql(query, engine)

# ===============================
# CARGA
# ===============================
if modo_dev and os.path.exists(ARQUIVO_CACHE):
    df = pd.read_csv(ARQUIVO_CACHE)
else:
    df = carregar_dados_db()
    df.to_csv(ARQUIVO_CACHE, index=False)

df['data_leitura'] = pd.to_datetime(df['data_leitura'], errors='coerce').dt.tz_localize(None)

if 'last_upload' in df.columns:
    df['last_upload'] = pd.to_datetime(df['last_upload'], errors='coerce')

if df.empty:
    st.stop()

# ===============================
# FILTROS
# ===============================
tipos_selecionados = st.sidebar.multiselect(
    "Variável do Dispositivo",
    sorted(df['tipo_sensor'].unique()),
    default=sorted(df['tipo_sensor'].unique())
)

df_tipo = df[df['tipo_sensor'].isin(tipos_selecionados)]

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

df_devices = df[['device_name', 'status']].drop_duplicates()
df_devices['status_lower'] = df_devices['status'].astype(str).str.lower()

if status_permitidos:
    df_devices = df_devices[df_devices['status_lower'].isin(status_permitidos)]

df_devices['status_str'] = df_devices['status_lower'].map({
    'online': '🟢 Online',
    'offline': '🔴 Offline'
}).fillna('⚪ Desconhecido')

df_devices['label'] = df_devices['device_name'] + " – " + df_devices['status_str']

device_label_map = dict(zip(df_devices['label'], df_devices['device_name']))

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

outros_devices = [device_label_map[lbl] for lbl in outros_labels]
devices_selecionados = list(dict.fromkeys([device_principal] + outros_devices))

# ===============================
# PERÍODO
# ===============================
st.sidebar.subheader("📅 Período de Análise")
data_min = df_tipo['data_leitura'].min().date()
data_max = df_tipo['data_leitura'].max().date()

c1, c2 = st.sidebar.columns(2)
data_ini = c1.date_input("Data inicial", data_min)
data_fim = c2.date_input("Data final", data_max)

data_ini_dt = pd.to_datetime(data_ini)
data_fim_dt = pd.to_datetime(data_fim) + pd.Timedelta(days=1)

df_final = df_tipo[
    (df_tipo['device_name'].isin(devices_selecionados)) &
    (df_tipo['data_leitura'] >= data_ini_dt) &
    (df_tipo['data_leitura'] < data_fim_dt)
].copy()

if df_final.empty:
    st.stop()

# ===============================
# HEADER (CORRIGIDO)
# ===============================
info = df_final.sort_values("data_leitura").iloc[-1]

status = str(info["status"]).lower()
bateria = int(info["battery_percentage"]) if pd.notna(info["battery_percentage"]) else 0
ultima_tx = info["last_upload"]

if pd.notna(ultima_tx):
    ultima_tx = (ultima_tx - pd.Timedelta(hours=3)).strftime('%d-%m-%Y %H:%M:%S')

cor_status = "#22c55e" if status == "online" else "#ef4444"

if bateria >= 75:
    cor_bateria = "#22c55e"
elif bateria >= 40:
    cor_bateria = "#facc15"
else:
    cor_bateria = "#ef4444"
