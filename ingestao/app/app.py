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
# FILTROS
# ===============================
tipos_selecionados = st.sidebar.multiselect(
    "Variável do Dispositivo",
    sorted(df["tipo_sensor"].astype(str).unique()),
    default=sorted(df["tipo_sensor"].astype(str).unique())
)

df_tipo = df[df["tipo_sensor"].astype(str).isin(tipos_selecionados)]

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
# 🚨 DETECÇÃO AUTOMÁTICA DO TARP (SÓ ADICIONADO AQUI)
# ======================================================
ultimo_por_sensor = (
    df_final.sort_values("data_leitura")
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

bateria = int(info["battery_percentage"]) if pd.notna(info["battery_percentage"]) else 0
ultima_tx = info["last_upload"]

if pd.notna(ultima_tx):
    ultima_tx = (ultima_tx - pd.Timedelta(hours=3)).strftime("%d-%m-%Y %H:%M:%S")

st.markdown(f"""
### {device_principal}
{emoji_tarp} TARP: {nivel_tarp} | 🔋 {bateria}% | ⏱ Última transmissão: {ultima_tx}
""")

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

fig.update_layout(height=780)

st.plotly_chart(fig,use_container_width=True)
