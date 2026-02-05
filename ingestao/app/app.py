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
# CONEXÃO BANCO
# ======================================================
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)

MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")

# ======================================================
# CONFIG STREAMLIT
# ======================================================
st.set_page_config(page_title="Gestão Geotécnica Orion", layout="wide")

# ======================================================
# QUERY
# ======================================================
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
        LOWER(d.status) AS status,
        d.battery_percentage,
        d.last_upload
    FROM leituras l
    JOIN sensores s ON l.sensor_id = s.sensor_id
    JOIN devices d ON s.device_id = d.device_id
    WHERE
        s.tipo_sensor IN ('A-Axis Delta Angle','B-Axis Delta Angle')
        AND l.data_leitura >= NOW() - INTERVAL '30 days'
    ORDER BY l.data_leitura ASC
    """

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    return df

df = carregar_dados_db()
df['data_leitura'] = pd.to_datetime(df['data_leitura'])

if df.empty:
    st.stop()

# ======================================================
# SIDEBAR
# ======================================================
st.sidebar.header("🛠️ Configurações")

if st.sidebar.button("🔄 Atualizar dados"):
    st.cache_data.clear()
    st.rerun()

# ======================================================
# FILTROS DEVICE
# ======================================================
df_devices = df[['device_name','status']].drop_duplicates()

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

# ======================================================
# HEADER DISPOSITIVO
# ======================================================
device_info = df[df['device_name']==device_principal].iloc[-1]

colA,colB,colC,colD = st.columns(4)

colA.metric("Dispositivo", device_info['device_name'])
colB.metric("Status", device_info['status'])
colC.metric("Bateria (%)", device_info['battery_percentage'])
colD.metric("Último envio", str(device_info['last_upload']))

# ======================================================
# PERÍODO
# ======================================================
data_min = df['data_leitura'].min().date()
data_max = df['data_leitura'].max().date()

c1, c2 = st.sidebar.columns(2)

data_ini = c1.date_input("Data inicial", value=data_min)
data_fim = c2.date_input("Data final", value=data_max)

data_ini_dt = pd.to_datetime(data_ini)
data_fim_dt = pd.to_datetime(data_fim) + pd.Timedelta(days=1)

df_final = df[
    (df['device_name']==device_principal) &
    (df['data_leitura']>=data_ini_dt) &
    (df['data_leitura']<data_fim_dt)
].copy()

# ======================================================
# ESCALA PROFISSIONAL
# ======================================================
st.sidebar.subheader("📐 Escala")

modo_escala = st.sidebar.radio(
    "Tipo de escala",
    ["Absoluta", "Relativa (primeiro valor = zero)", "Relativa manual"]
)

df_final['valor_grafico'] = df_final['valor_sensor']

if modo_escala == "Relativa (primeiro valor = zero)":
    df_final['valor_grafico'] = (
        df_final.sort_values('data_leitura')
        .groupby('sensor_id')['valor_sensor']
        .transform(lambda x: x - x.iloc[0])
    )

elif modo_escala == "Relativa manual":

    sensores_lista = sorted(df_final['sensor_id'].unique())
    referencia_manual = {}

    st.sidebar.markdown("Valor de referência por eixo:")

    for sid in sensores_lista:
        valor = st.sidebar.number_input(
            f"Sensor {sid}",
            value=0.0,
            step=0.01,
            key=f"ref_{sid}"
        )
        referencia_manual[sid] = valor

    df_final['valor_grafico'] = df_final.apply(
        lambda row: row['valor_sensor'] - referencia_manual.get(row['sensor_id'],0),
        axis=1
    )

# ======================================================
# GRÁFICO
# ======================================================
fig = px.line(
    df_final,
    x="data_leitura",
    y="valor_grafico",
    color="tipo_sensor",
    template="plotly_white"
)

st.plotly_chart(
    fig,
    use_container_width=True,
    config={"scrollZoom":True}
)

# ======================================================
# MAPA
# ======================================================
st.subheader("🛰️ Localização dos Dispositivos")

df_mapa = df_final[['device_name','latitude','longitude','status']].drop_duplicates()

df_mapa['cor'] = df_mapa['status'].apply(
    lambda x:"#6ee7b7" if x=="online" else "#ef4444"
)

mapa = go.Figure(go.Scattermapbox(
    lat=df_mapa['latitude'],
    lon=df_mapa['longitude'],
    mode="markers+text",
    marker=dict(size=20,color=df_mapa['cor']),
    text=df_mapa['device_name'],
    textposition="top center"
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

st.plotly_chart(
    mapa,
    use_container_width=True,
    config={"scrollZoom":True}
)

# ======================================================
# 🔥 TABELA + EXPORTAÇÃO (RESTAUROU)
# ======================================================
st.subheader("📋 Dados")

st.dataframe(
    df_final[['data_leitura','tipo_sensor','valor_sensor','valor_grafico']],
    use_container_width=True
)

csv = df_final.to_csv(index=False).encode("utf-8")

st.download_button(
    "📥 Baixar CSV",
    csv,
    "dados_geotecnicos.csv",
    "text/csv"
)
