import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from pathlib import Path

# ======================================================
# AUTENTICAÇÃO
# ======================================================
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

# ======================================================
# LOAD ENV
# ======================================================
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

# ======================================================
# CONEXÃO COM BANCO (🔥 melhoria estabilidade cloud)
# ======================================================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    st.error("DATABASE_URL não configurada")
    st.stop()

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300
)

# ======================================================
# MAPBOX TOKEN
# ======================================================
MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")

# ======================================================
# CONFIG STREAMLIT
# ======================================================
st.set_page_config(
    page_title="Gestão Geotécnica Orion",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ======================================================
# HEADER
# ======================================================
if os.path.exists("header_orion.png"):
    st.image("header_orion.png", use_container_width=True)

# ======================================================
# SIDEBAR
# ======================================================
st.sidebar.header("🛠️ Configurações")

modo_dev = st.sidebar.checkbox(
    "Modo desenvolvimento (não consultar banco)",
    value=False
)

if st.sidebar.button("🔄 Atualizar dados"):
    if not modo_dev:
        st.cache_data.clear()
    st.rerun()

# ======================================================
# QUERY OTIMIZADA PROFISSIONAL (🔥)
# ======================================================
@st.cache_data(ttl=300, show_spinner=False)
def carregar_dados_db():
    query = """
    SELECT 
        l.data_leitura AT TIME ZONE 'UTC' AS data_leitura,
        l.valor_sensor,
        s.sensor_id,
        s.tipo_sensor,
        d.device_name,
        d.latitude,
        d.longitude,
        LOWER(d.status) AS status,
        d.battery_percent,
        d.last_upload AT TIME ZONE 'UTC' AS last_upload
    FROM leituras l
    JOIN sensores s ON l.sensor_id = s.sensor_id
    JOIN devices d ON s.device_id = d.device_id
    WHERE
        s.tipo_sensor IN ('A-Axis Delta Angle','B-Axis Delta Angle')
        AND l.data_leitura >= NOW() - INTERVAL '30 days'
    ORDER BY l.data_leitura ASC
    """
    return pd.read_sql_query(query, engine)

df = carregar_dados_db()

# ======================================================
# NORMALIZAÇÃO (🔥 menos processamento)
# ======================================================
df['data_leitura'] = pd.to_datetime(df['data_leitura'], errors='coerce')

if 'last_upload' in df.columns:
    df['last_upload'] = pd.to_datetime(df['last_upload'], errors='coerce')

if df.empty:
    st.stop()

# ======================================================
# FILTROS
# ======================================================
tipos_selecionados = st.sidebar.multiselect(
    "Variável do Dispositivo",
    sorted(df['tipo_sensor'].unique()),
    default=sorted(df['tipo_sensor'].unique())
)

df_tipo = df[df['tipo_sensor'].isin(tipos_selecionados)]

# Status
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

df_devices = df[['device_name','status']].drop_duplicates()

if status_permitidos:
    df_devices = df_devices[df_devices['status'].isin(status_permitidos)]

df_devices['status_str'] = df_devices['status'].map({
    'online':'🟢 Online',
    'offline':'🔴 Offline'
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

# ======================================================
# PERÍODO
# ======================================================
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

# ======================================================
# ESCALA
# ======================================================
modo_escala = st.sidebar.radio(
    "Escala de Visualização",
    ["Absoluta","Relativa"]
)

if modo_escala == "Relativa":
    df_final['valor_grafico'] = df_final.groupby('sensor_id')['valor_sensor'].transform(lambda x: x - x.iloc[0])
    label_y = "Variação Relativa"
else:
    df_final['valor_grafico'] = df_final['valor_sensor']
    label_y = "Valor Absoluto"

# ======================================================
# GRÁFICO
# ======================================================
df_final['serie'] = df_final['device_name'] + ' | ' + df_final['tipo_sensor']

fig = px.line(
    df_final,
    x="data_leitura",
    y="valor_grafico",
    color="serie",
    template="plotly_white"
)

fig.update_xaxes(
    title_text="",
    tickformat="%d/%m\n%H:%M",
    tickfont=dict(size=17),
    showspikes=True,
    spikemode="across",
    spikesnap="cursor"
)

fig.update_yaxes(
    title_text=f"<b>{label_y}</b>",
    tickfont=dict(size=17)
)

fig.update_layout(
    height=780,
    legend=dict(
        orientation="h",
        y=-0.15,
        x=0.5,
        xanchor="center",
        title_text=""
    )
)

st.plotly_chart(fig, use_container_width=True)

# ======================================================
# MAPA
# ======================================================
st.subheader("🛰️ Localização dos Dispositivos")

df_mapa = (
    df_final[['device_name','latitude','longitude','status']]
    .drop_duplicates()
    .dropna(subset=['latitude','longitude'])
)

df_mapa['cor'] = df_mapa['status'].apply(
    lambda x:"#6ee7b7" if x=="online" else "#ef4444"
)

mapa = go.Figure(go.Scattermapbox(
    lat=df_mapa['latitude'],
    lon=df_mapa['longitude'],
    mode="markers+text",
    marker=dict(size=20,color=df_mapa['cor']),
    text=df_mapa['device_name'],
    textposition="top center",
    textfont=dict(size=18,color="white")
))

mapa.update_layout(
    height=700,
    mapbox=dict(
        accesstoken=MAPBOX_TOKEN,
        style="satellite-streets",
        zoom=12,
        center=dict(
            lat=df_mapa['latitude'].mean(),
            lon=df_mapa['longitude'].mean()
        )
    ),
    margin=dict(l=0,r=0,t=0,b=0)
)

st.plotly_chart(mapa, use_container_width=True)

# ======================================================
# TABELA
# ======================================================
st.dataframe(
    df_final[['data_leitura','device_name','tipo_sensor','valor_sensor','valor_grafico']],
    use_container_width=True
)

# ======================================================
# DOWNLOAD CSV
# ======================================================
csv = df_final.to_csv(index=False).encode("utf-8")

st.download_button(
    "📥 Baixar CSV",
    csv,
    "dados_geotecnicos.csv",
    "text/csv"
)
