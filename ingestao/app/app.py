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

DATABASE_URL = os.getenv("DATABASE_URL")
MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")

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

# ===============================
# FILTRO EIXO A/B/AMBOS (VOLTOU)
# ===============================
tipos_selecionados = st.sidebar.multiselect(
    "Variável do Dispositivo",
    sorted(df["tipo_sensor"].unique()),
    default=sorted(df["tipo_sensor"].unique())
)

df_tipo = df[df["tipo_sensor"].isin(tipos_selecionados)]

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
# ORDEM CORRETA DOS EIXOS (A EM CIMA)
# ===============================
ordem_series = sorted(
    df_final["tipo_sensor"].unique(),
    key=lambda x: ("B" in x, x)
)

df_final["tipo_sensor"] = pd.Categorical(
    df_final["tipo_sensor"],
    categories=ordem_series,
    ordered=True
)

df_final = df_final.sort_values(["tipo_sensor","data_leitura"])

# ===============================
# 🚨 TARPs
# ===============================
st.sidebar.markdown("### 🚨 Limites de Alerta")

device_id_atual = df_final.iloc[-1]["device_id"]

query_limites = f"""
SELECT *
FROM alert_limits
WHERE device_id = {device_id_atual}
ORDER BY tipo_sensor ASC, limite_valor ASC
"""

try:
    limites_existentes = pd.read_sql(query_limites, engine)
except:
    limites_existentes = pd.DataFrame()

tipos_ordenados = sorted(
    df_final["tipo_sensor"].unique(),
    key=lambda x: ("A" not in x, x)
)

novo_alerta_tipo = st.sidebar.selectbox("Tipo de Sensor",tipos_ordenados)
novo_valor = st.sidebar.number_input("Valor do Limite",value=0.0,step=0.1)
mostrar_linha = st.sidebar.checkbox("Mostrar linha tracejada no gráfico",value=True)
mensagem_alerta = st.sidebar.text_input("Mensagem do alerta",value="Ex: Fazer inspeção")

if st.sidebar.button("➕ Adicionar Alerta"):

    with engine.begin() as conn:
        conn.execute(f"""
            INSERT INTO alert_limits
            (device_id,tipo_sensor,limite_valor,mostrar_linha,mensagem)
            VALUES (
                {device_id_atual},
                '{novo_alerta_tipo}',
                {novo_valor},
                {mostrar_linha},
                '{mensagem_alerta}'
            )
        """)

    st.sidebar.success("Alerta criado!")

# ===============================
# ZERO REFERÊNCIA
# ===============================
modo_escala = st.sidebar.radio(
    "Escala de Visualização",
    ["Absoluta","Relativa"]
)

if modo_escala=="Relativa":

    usar_primeiro_valor = st.sidebar.checkbox(
        "Usar primeiro valor como zero",
        value=True
    )

    if usar_primeiro_valor:
        refs = (
            df_final.sort_values("data_leitura")
            .groupby("sensor_id")["valor_sensor"]
            .first()
        )
    else:
        refs = {sid:0 for sid in df_final["sensor_id"].unique()}

    df_final["valor_grafico"]=df_final["valor_sensor"]-df_final["sensor_id"].map(refs)
    label_y="Variação Relativa"
else:
    df_final["valor_grafico"]=df_final["valor_sensor"]
    label_y="Valor Absoluto"

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
🟢 Status: {status.upper()} | 🔋 {bateria}% | ⏱ Última transmissão: {ultima_tx}
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

# 🔴 TARPs NO GRÁFICO
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
