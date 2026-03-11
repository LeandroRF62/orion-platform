import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine
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
# CORES
# ======================================================

CORES_SENSOR = {
    "A-Axis Delta Angle": "#2563eb",
    "B-Axis Delta Angle": "#f97316",
    "Device Temperature": "#ef4444",
    "Air Temperature": "#ef4444"
}

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
# ENV
# ======================================================

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

# ======================================================
# BOTÃO ATUALIZAR
# ======================================================

if st.sidebar.button("🔄 Atualizar Dados"):
    st.cache_data.clear()
    st.rerun()

# ======================================================
# QUERY BANCO
# ======================================================

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

    ramais = [
        "LPR - Brito",
        "LPR - Renan",
        "LPR - Witheney",
        "RBH - José",
        "RBR - José",
        "RFA - Léo Silva",
        "RFA - Thiago"
    ]

    ramal_selecionado = st.selectbox(
        "Selecionar Ramal",
        ramais
    )

df = df[df["reference"] == ramal_selecionado]

# ======================================================
# FILTRO TILT
# ======================================================

tilt_devices = (
    df[df["tipo_sensor"].isin(["A-Axis Delta Angle", "B-Axis Delta Angle"])]
    ["device_name"].unique()
)

df = df[df["device_name"].isin(tilt_devices)]

# ======================================================
# DISPOSITIVOS
# ======================================================

with st.sidebar.expander("🎛️ Dispositivo", expanded=True):

    tipos_selecionados = st.multiselect(
        "Variável do Dispositivo",
        sorted(df["tipo_sensor"].astype(str).unique()),
        default=sorted(df["tipo_sensor"].astype(str).unique())
    )

    df_tipo = df[df["tipo_sensor"].astype(str).isin(tipos_selecionados)]

    df_devices = df_tipo[["device_name", "status"]].drop_duplicates()

    df_devices["status_lower"] = df_devices["status"].astype(str).str.lower()

    df_devices["status_str"] = df_devices["status_lower"].map({
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

        device_principal_label = st.selectbox(
            "Selecionar Dispositivo Principal",
            labels
        )

        device_principal = device_label_map.get(device_principal_label)

        outros_labels = st.multiselect(
            "Adicionar Outros Dispositivos",
            labels
        )

        devices_selecionados = list(dict.fromkeys(
            [device_principal] + [device_label_map[l] for l in outros_labels if l in device_label_map]
        ))

df_final = df_tipo[df_tipo["device_name"].isin(devices_selecionados)].copy()

# ======================================================
# PERÍODO
# ======================================================

with st.sidebar.expander("📅 Período de Análise", expanded=False):

    data_min = df_final["data_leitura"].min().date()
    data_max = df_final["data_leitura"].max().date()

    c1, c2 = st.columns(2)

    data_ini = c1.date_input("Data inicial", data_min)
    data_fim = c2.date_input("Data final", data_max)

df_final = df_final[
    (df_final["data_leitura"] >= pd.to_datetime(data_ini)) &
    (df_final["data_leitura"] < pd.to_datetime(data_fim) + pd.Timedelta(days=1))
]

# ======================================================
# ESCALA
# ======================================================

with st.sidebar.expander("⚙️ Visualização", expanded=False):

    modo_escala = st.radio(
        "Escala de Visualização",
        ["Absoluta", "Relativa (primeiro valor = zero)"]
    )

df_final["valor_grafico"] = df_final["valor_sensor"]

if modo_escala == "Relativa (primeiro valor = zero)":

    refs = (
        df_final.sort_values("data_leitura")
        .groupby("sensor_id")["valor_sensor"]
        .first()
    )

    df_final["valor_grafico"] = (
        df_final["valor_sensor"] - df_final["sensor_id"].map(refs)
    )

# ======================================================
# GRÁFICO
# ======================================================

df_final["serie"] = df_final["device_name"] + " | " + df_final["tipo_sensor"]

fig = go.Figure()

for serie in df_final["serie"].unique():

    d = df_final[df_final["serie"] == serie]

    tipo = d["tipo_sensor"].iloc[0]

    eixo_secundario = tipo in ["Device Temperature", "Air Temperature"]

    fig.add_trace(go.Scatter(
        x=d["data_leitura"],
        y=d["valor_grafico"],
        mode="lines",
        name=serie,
        yaxis="y2" if eixo_secundario else "y"
    ))

fig.update_layout(
    height=780,
    hovermode="x unified",
    dragmode="pan",
    yaxis=dict(title="Valor"),
    yaxis2=dict(title="Temperatura (°C)", overlaying="y", side="right"),
    legend=dict(
        orientation="h",
        yanchor="top",
        y=-0.25,
        xanchor="center",
        x=0.5
    ),
    margin=dict(b=120)
)

fig.update_xaxes(showspikes=True)
fig.update_yaxes(showspikes=True)

st.plotly_chart(
    fig,
    use_container_width=True,
    config={
        "scrollZoom": True,
        "doubleClick": "reset",
        "displaylogo": False
    }
)

# ======================================================
# MAPA
# ======================================================

st.subheader("🛰️ Localização dos Dispositivos")

df_mapa = (
    df_final[["device_name", "latitude", "longitude", "status"]]
    .drop_duplicates()
    .dropna(subset=["latitude", "longitude"])
)

df_mapa["cor"] = df_mapa["status"].astype(str).str.lower().apply(
    lambda x: "#6ee7b7" if x == "online" else "#ef4444"
)

mapa = go.Figure(go.Scattermapbox(
    lat=df_mapa["latitude"],
    lon=df_mapa["longitude"],
    mode="markers+text",
    marker=dict(size=20, color=df_mapa["cor"]),
    text=df_mapa["device_name"],
    textposition="top center"
))

mapa.update_layout(
    height=700,
    mapbox=dict(
        accesstoken=MAPBOX_TOKEN,
        style="satellite-streets",
        zoom=12,
        center=dict(
            lat=df_mapa["latitude"].mean(),
            lon=df_mapa["longitude"].mean()
        )
    ),
    margin=dict(l=0, r=0, t=0, b=0)
)

st.plotly_chart(
    mapa,
    use_container_width=True,
    config={"scrollZoom": True}
)

# ======================================================
# TABELA
# ======================================================

st.subheader("📋 Dados")

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

csv = df_final.to_csv(index=False).encode("utf-8")

st.download_button(
    "📥 Baixar CSV",
    csv,
    "dados_geotecnicos.csv",
    "text/csv"
)
