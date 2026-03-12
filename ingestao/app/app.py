import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from pathlib import Path

# ======================================================
# FUNÇÃO TARP
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

# ======================================================
# CORES E PALETAS
# ======================================================

# Cores fixas para quando apenas UM dispositivo é selecionado
CORES_SENSOR = {
    "A-Axis Delta Angle": "#2563eb",  # Azul
    "B-Axis Delta Angle": "#059669",  # Verde (ajustado para diferenciar do ar)
    "Device Temperature": "#f59e0b",  # Amarelo/Laranja
    "Air Temperature": "#ef4444"      # Vermelho
}

# Paleta para múltiplos dispositivos
PALETA_DEVICES = [
    "#636EFA", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3", 
    "#FF6692", "#B6E880", "#FF97FF", "#FECB52"
]

# ======================================================
# ENV & DATABASE
# ======================================================

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")
APP_PASSWORD = os.getenv("APP_PASSWORD", "orion123")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)

# ======================================================
# AUTENTICAÇÃO
# ======================================================

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

st.set_page_config(
    page_title="Gestão Geotécnica Orion",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ======================================================
# QUERY BANCO
# ======================================================

@st.cache_data(ttl=300)
def carregar_dados_db():
    # Garantir que a coluna reference existe antes de ler (migração leve)
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE devices ADD COLUMN IF NOT EXISTS reference TEXT;"))
        conn.commit()

    query = """
        SELECT 
            l.data_leitura,
            l.valor_sensor,
            s.sensor_id,
            s.tipo_sensor,
            s.device_id,
            d.device_name,
            d.reference,
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

# ======================================================
# INÍCIO DO APP
# ======================================================

if st.sidebar.button("🔄 Atualizar Dados"):
    st.cache_data.clear()
    st.rerun()

df = carregar_dados_db()

if df.empty:
    st.warning("Sem dados ainda.")
    st.stop()

df["data_leitura"] = pd.to_datetime(df["data_leitura"]).dt.tz_localize(None)
df["last_upload"] = pd.to_datetime(df["last_upload"], errors="coerce")

# ======================================================
# FILTRO RAMAL
# ======================================================

with st.sidebar.expander("📍 Ramal", expanded=True):
    ramais_disponiveis = sorted(df["reference"].dropna().unique())
    if not ramais_disponiveis:
        ramais_disponiveis = ["Sem Ramal"]
    
    ramal_selecionado = st.selectbox("Selecionar Ramal", ramais_disponiveis)

df = df[df["reference"] == ramal_selecionado]

if df.empty:
    st.warning("Nenhum dado encontrado para este ramal.")
    st.stop()

# ======================================================
# DISPOSITIVOS E VARIÁVEIS
# ======================================================

with st.sidebar.expander("🎛️ Dispositivo", expanded=True):
    tipos_selecionados = st.multiselect(
        "Variável do Dispositivo",
        sorted(df["tipo_sensor"].astype(str).unique()),
        default=sorted(df["tipo_sensor"].astype(str).unique())
    )

    df_tipo = df[df["tipo_sensor"].astype(str).isin(tipos_selecionados)]
    df_devices = df_tipo[["device_name", "status"]].drop_duplicates()
    
    df_devices["status_str"] = df_devices["status"].astype(str).str.lower().map({
        "online": "🟢 Online",
        "offline": "🔴 Offline"
    }).fillna("⚪ Desconhecido")

    df_devices["label"] = df_devices["device_name"] + " – " + df_devices["status_str"]
    device_label_map = dict(zip(df_devices["label"], df_devices["device_name"]))
    labels = sorted(device_label_map.keys())

    selecionar_todos = st.checkbox("Selecionar todos os dispositivos deste ramal")

    if selecionar_todos:
        devices_selecionados = list(device_label_map.values())
    else:
        device_principal_label = st.selectbox("Selecionar Dispositivo Principal", labels)
        device_principal = device_label_map.get(device_principal_label)

        outros_labels = st.multiselect("Adicionar Outros Dispositivos", [l for l in labels if l != device_principal_label])
        
        devices_selecionados = [device_principal] + [device_label_map[l] for l in outros_labels]

df_final = df_tipo[df_tipo["device_name"].isin(devices_selecionados)].copy()

# ======================================================
# FILTROS DE TEMPO E ESCALA
# ======================================================

with st.sidebar.expander("📅 Período de Análise", expanded=False):
    data_min, data_max = df_final["data_leitura"].min().date(), df_final["data_leitura"].max().date()
    c1, c2 = st.columns(2)
    data_ini = c1.date_input("Data inicial", data_min)
    data_fim = c2.date_input("Data final", data_max)

df_final = df_final[
    (df_final["data_leitura"] >= pd.to_datetime(data_ini)) &
    (df_final["data_leitura"] < pd.to_datetime(data_fim) + pd.Timedelta(days=1))
]

modo_escala = st.sidebar.radio("Escala", ["Absoluta", "Relativa (T0)"])
df_final["valor_grafico"] = df_final["valor_sensor"]

if modo_escala == "Relativa (T0)":
    refs = df_final.sort_values("data_leitura").groupby("sensor_id")["valor_sensor"].transform("first")
    df_final["valor_grafico"] = df_final["valor_sensor"] - refs

# ======================================================
# GRÁFICO (LÓGICA DE CORES DINÂMICA)
# ======================================================

df_final["serie"] = df_final["device_name"] + " | " + df_final["tipo_sensor"]
fig = go.Figure()

num_devices = len(devices_selecionados)
device_color_map = {dev: PALETA_DEVICES[i % len(PALETA_DEVICES)] for i, dev in enumerate(devices_selecionados)}

for serie in df_final["serie"].unique():
    d = df_final[df_final["serie"] == serie]
    tipo = d["tipo_sensor"].iloc[0]
    nome_device = d["device_name"].iloc[0]
    
    eixo_secundario = tipo in ["Device Temperature", "Air Temperature"]
    line_style = dict(width=2)

    if num_devices == 1:
        # Caso 1: Somente 1 Device - Cores por Variável
        line_style["color"] = CORES_SENSOR.get(tipo, "#6b7280")
        if tipo == "Air Temperature":
            line_style["dash"] = "dash"
    else:
        # Caso 2: Adicionar Outros - Cada Device tem sua Cor
        line_style["color"] = device_color_map.get(nome_device)
        if tipo == "Air Temperature":
            line_style["dash"] = "dot" # Mantém tracejado para identificar temperatura
        elif tipo == "Device Temperature":
            line_style["width"] = 1 # Mais fina para diferenciar da do Ar

    fig.add_trace(go.Scatter(
        x=d["data_leitura"], y=d["valor_grafico"],
        mode="lines", name=serie, line=line_style,
        yaxis="y2" if eixo_secundario else "y"
    ))

fig.update_layout(
    height=700, hovermode="x unified",
    title="Análise de Variáveis" if num_devices == 1 else "Comparativo entre Dispositivos",
    yaxis=dict(title="Delta Angle"),
    yaxis2=dict(title="Temperatura (°C)", overlaying="y", side="right"),
    legend=dict(orientation="h", y=-0.2, x=0.5, xanchor="center")
)

st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": True})

# ======================================================
# MAPA E TABELA
# ======================================================

st.subheader("🛰️ Localização")
df_mapa = df_final[["device_name", "latitude", "longitude", "status"]].drop_duplicates().dropna()
df_mapa["cor"] = df_mapa["status"].str.lower().apply(lambda x: "#6ee7b7" if x == "online" else "#ef4444")

mapa = go.Figure(go.Scattermapbox(
    lat=df_mapa["latitude"], lon=df_mapa["longitude"],
    mode="markers+text", marker=dict(size=15, color=df_mapa["cor"]),
    text=df_mapa["device_name"], textposition="top center"
))

mapa.update_layout(
    height=500, margin=dict(l=0,r=0,t=0,b=0),
    mapbox=dict(accesstoken=MAPBOX_TOKEN, style="satellite-streets", zoom=14,
                center=dict(lat=df_mapa["latitude"].mean(), lon=df_mapa["longitude"].mean()))
)
st.plotly_chart(mapa, use_container_width=True)

st.subheader("📋 Dados Brutos")
st.dataframe(df_final[["data_leitura", "device_name", "tipo_sensor", "valor_sensor"]], use_container_width=True)

st.download_button("📥 Baixar CSV", df_final.to_csv(index=False).encode("utf-8"), "dados.csv", "text/csv")
