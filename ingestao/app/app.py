import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine
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
# 🎨 CORES FIXAS PROFISSIONAIS
# ===============================
CORES_SENSOR = {
    "A-Axis Delta Angle": "#2563eb",   # Azul
    "B-Axis Delta Angle": "#f97316",   # Laranja
    "Device Temperature": "#a855f7",   # Vermelho
    "Air Temperature": "#ef4444"       # Roxo
}

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

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)

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
    WHERE s.tipo_sensor IN (
        'A-Axis Delta Angle',
        'B-Axis Delta Angle',
        'Device Temperature',
        'Air Temperature'
    )
    ORDER BY l.data_leitura
    """
    return pd.read_sql(query, engine)

df = carregar_dados_db()

df["data_leitura"] = pd.to_datetime(df["data_leitura"]).dt.tz_localize(None)
df["last_upload"] = pd.to_datetime(df["last_upload"], errors="coerce")

# ======================================================
# 🎛️ DISPOSITIVO
# ======================================================
with st.sidebar.expander("🎛️ Dispositivo", expanded=True):

    tipos_selecionados = st.multiselect(
        "Variável do Dispositivo",
        sorted(df["tipo_sensor"].astype(str).unique()),
        default=sorted(df["tipo_sensor"].astype(str).unique())
    )

    df_tipo = df[df["tipo_sensor"].isin(tipos_selecionados)]

    df_devices = df_tipo[["device_name","status"]].drop_duplicates()

    device_principal = st.selectbox(
        "Selecionar Dispositivo Principal",
        sorted(df_devices["device_name"].unique())
    )

df_final = df_tipo[df_tipo["device_name"]==device_principal].copy()

# ======================================================
# 📅 PERÍODO
# ======================================================
data_min = df_final["data_leitura"].min().date()
data_max = df_final["data_leitura"].max().date()

data_ini = st.sidebar.date_input("Data inicial", data_min)
data_fim = st.sidebar.date_input("Data final", data_max)

df_final = df_final[
    (df_final["data_leitura"]>=pd.to_datetime(data_ini)) &
    (df_final["data_leitura"]<pd.to_datetime(data_fim)+pd.Timedelta(days=1))
]

# ======================================================
# ⚙️ ESCALA PROFISSIONAL
# ======================================================
modo_escala = st.sidebar.radio(
    "Escala de Visualização",
    ["Absoluta","Relativa (primeiro valor = zero)","Relativa manual"]
)

df_final["valor_grafico"]=df_final["valor_sensor"]

mask_inclin = df_final["tipo_sensor"].isin(["A-Axis Delta Angle","B-Axis Delta Angle"])

if modo_escala=="Relativa (primeiro valor = zero)":
    refs = (
        df_final[mask_inclin]
        .sort_values("data_leitura")
        .groupby("sensor_id")["valor_sensor"]
        .first()
    )
    df_final.loc[mask_inclin,"valor_grafico"] = \
        df_final.loc[mask_inclin,"valor_sensor"] - df_final.loc[mask_inclin,"sensor_id"].map(refs)

# ======================================================
# HEADER
# ======================================================
info = df_final.sort_values("data_leitura").iloc[-1]

st.markdown(f"""
### {device_principal}
🟢 Status: {info['status']} | 🔋 {info['battery_percentage']}%
""")

# ======================================================
# 🔥 GRÁFICO PROFISSIONAL (CORES + TRACEJADO)
# ======================================================
df_final["serie"]=df_final["device_name"]+" | "+df_final["tipo_sensor"]

df_temp = df_final[df_final["tipo_sensor"].isin(["Device Temperature","Air Temperature"])]
df_inclin = df_final[~df_final["tipo_sensor"].isin(["Device Temperature","Air Temperature"])]

fig = go.Figure()

# 🔵 Inclinação (linhas sólidas)
for _,row in df_inclin.iterrows():
    pass

for serie in df_inclin["serie"].unique():
    d=df_inclin[df_inclin["serie"]==serie]
    tipo=d["tipo_sensor"].iloc[0]

    fig.add_trace(go.Scatter(
        x=d["data_leitura"],
        y=d["valor_grafico"],
        mode="lines",
        name=serie,
        line=dict(color=CORES_SENSOR.get(tipo,"#000000"))
    ))

# 🌡️ Temperatura (linhas tracejadas)
for serie in df_temp["serie"].unique():
    d=df_temp[df_temp["serie"]==serie]
    tipo=d["tipo_sensor"].iloc[0]

    fig.add_trace(go.Scatter(
        x=d["data_leitura"],
        y=d["valor_grafico"],
        mode="lines",
        name=serie,
        yaxis="y2",
        line=dict(
            color=CORES_SENSOR.get(tipo,"#000000"),
            dash="dash"  # 👈 TRACEJADO
        )
    ))

label_y = "Valor Absoluto" if modo_escala=="Absoluta" else "Δ Valor Relativo"

fig.update_layout(
    height=780,
    hovermode="x unified",

    # 👇 permite esticar eixo vertical arrastando
    dragmode="zoom",

    legend=dict(orientation="h",y=-0.15,x=0.5,xanchor="center"),
    yaxis=dict(
        title=f"<b>{label_y}</b>",
        fixedrange=False   # 👈 libera zoom vertical
    ),
    yaxis2=dict(
        title="<b>Temperatura (°C)</b>",
        overlaying="y",
        side="right",
        fixedrange=False   # 👈 libera zoom vertical temperatura
    )
)


# ======================================================
# 🛰️ MAPA
# ======================================================
st.subheader("🛰️ Localização dos Dispositivos")

df_mapa = df_final[["device_name","latitude","longitude"]].drop_duplicates()

mapa = go.Figure(go.Scattermapbox(
    lat=df_mapa["latitude"],
    lon=df_mapa["longitude"],
    mode="markers+text",
    text=df_mapa["device_name"]
))

mapa.update_layout(
    mapbox=dict(accesstoken=MAPBOX_TOKEN,style="satellite-streets",zoom=12)
)

st.plotly_chart(mapa,use_container_width=True)

# ======================================================
# 📋 TABELA
# ======================================================
st.dataframe(df_final)

csv=df_final.to_csv(index=False).encode("utf-8")
st.download_button("📥 Baixar CSV",csv,"dados_geotecnicos.csv","text/csv")
