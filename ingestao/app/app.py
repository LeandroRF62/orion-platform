# ===============================
# IMPORTS
# ===============================
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from pathlib import Path

# ===============================
# AUTH
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

# ===============================
# LOAD DATA
# ===============================
@st.cache_data(ttl=300)
def carregar_dados():
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

df = carregar_dados()

df["data_leitura"] = pd.to_datetime(df["data_leitura"]).dt.tz_localize(None)
df["last_upload"] = pd.to_datetime(df["last_upload"], errors="coerce")

# ===============================
# SIDEBAR DISPOSITIVOS
# ===============================
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

outros_labels = st.sidebar.multiselect(
    "Adicionar Outros Dispositivos",
    sorted(device_label_map.keys())
)

devices_selecionados = list(dict.fromkeys(
    [device_principal]+[device_label_map[l] for l in outros_labels]
))

df_final = df[df["device_name"].isin(devices_selecionados)].copy()

# ===============================
# HEADER INFO
# ===============================
info = df_final.sort_values("data_leitura").iloc[-1]

status = str(info["status"]).lower()
bateria = int(info["battery_percentage"]) if pd.notna(info["battery_percentage"]) else 0
ultima_tx = info["last_upload"]

if pd.notna(ultima_tx):
    ultima_tx = (ultima_tx - pd.Timedelta(hours=3)).strftime("%d-%m-%Y %H:%M:%S")

st.markdown(f"""
### {device_principal}
🟢 Status: {status.upper()} | 🔋 {bateria}% | ⏱ Última transmissão: {ultima_tx}
""")

# ===============================
# ZERO REFERENCIA
# ===============================
modo_escala = st.sidebar.radio(
    "Escala",
    ["Absoluta","Relativa"]
)

if modo_escala=="Relativa":
    refs = (
        df_final.sort_values("data_leitura")
        .groupby("sensor_id")["valor_sensor"]
        .first()
    )
    df_final["valor_grafico"] = df_final["valor_sensor"] - df_final["sensor_id"].map(refs)
else:
    df_final["valor_grafico"] = df_final["valor_sensor"]

# ===============================
# TARPs PANEL
# ===============================
st.sidebar.markdown("### 🚨 Limites de Alerta")

device_id_atual = info["device_id"]

query_limites = f"""
SELECT *
FROM alert_limits
WHERE device_id = {device_id_atual}
"""
limites_existentes = pd.read_sql(query_limites, engine)

novo_valor = st.sidebar.number_input("Valor Limite",0.0)
mostrar_linha = st.sidebar.checkbox("Mostrar linha",True)
mensagem_alerta = st.sidebar.text_input("Mensagem","Fazer inspeção")

if st.sidebar.button("➕ Adicionar Alerta"):

    with engine.begin() as conn:
        conn.execute(f"""
            INSERT INTO alert_limits
            (device_id,limite_valor,mostrar_linha,mensagem)
            VALUES ({device_id_atual},{novo_valor},{mostrar_linha},'{mensagem_alerta}')
        """)

# ===============================
# GRÁFICO
# ===============================
df_final["serie"]=df_final["device_name"]+" | "+df_final["tipo_sensor"]

fig=px.line(
    df_final,
    x="data_leitura",
    y="valor_grafico",
    color="serie",
    template="plotly_white"
)

# TARPs lines
if not limites_existentes.empty:
    for _,alerta in limites_existentes.iterrows():
        if alerta["mostrar_linha"]:
            fig.add_hline(
                y=alerta["limite_valor"],
                line_dash="dash",
                annotation_text=alerta["mensagem"]
            )

st.plotly_chart(fig,use_container_width=True)
