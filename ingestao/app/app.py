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

# ======================================================
# 🎨 CORES FIXAS DEFINIDAS
# ======================================================
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
# 🔄 BOTÃO ATUALIZAR DADOS
# ===============================
if st.sidebar.button("🔄 Atualizar Dados"):
    st.cache_data.clear()
    st.rerun()

# ===============================
# QUERY BANCO (🔥 AGORA COM TEMPERATURA)
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

if df.empty:
    st.warning("Sem dados ainda.")
    st.stop()

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

    df_tipo = df[df["tipo_sensor"].astype(str).isin(tipos_selecionados)]

    df_devices = df_tipo[["device_name", "status"]].drop_duplicates()
    df_devices["status_lower"] = df_devices["status"].astype(str).str.lower()

    df_devices["status_str"] = df_devices["status_lower"].map({
        "online": "🟢 Online",
        "offline": "🔴 Offline"
    }).fillna("⚪ Desconhecido")

    df_devices["label"] = df_devices["device_name"] + " – " + df_devices["status_str"]

    device_label_map = dict(zip(df_devices["label"], df_devices["device_name"]))

    device_principal_label = st.selectbox(
        "Selecionar Dispositivo Principal",
        sorted(device_label_map.keys())
    )

    device_principal = device_label_map[device_principal_label]

    if "outros_labels" not in st.session_state:
       st.session_state.outros_labels = []

    outros_labels = st.multiselect(
       "Adicionar Outros Dispositivos",
        sorted(device_label_map.keys()),
        default=st.session_state.outros_labels,
        key="outros_labels"
    )
  
devices_selecionados = list(dict.fromkeys(
    [device_principal] + [device_label_map[l] for l in outros_labels]
))

df_final = df_tipo[df_tipo["device_name"].isin(devices_selecionados)].copy()

# ======================================================
# 📅 PERÍODO
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
# ⚙️ ESCALA PROFISSIONAL
# ======================================================
with st.sidebar.expander("⚙️ Visualização", expanded=False):

    modo_escala = st.radio(
        "Escala de Visualização",
        ["Absoluta", "Relativa (primeiro valor = zero)", "Relativa manual"]
    )

df_final["valor_grafico"] = df_final["valor_sensor"]

if modo_escala == "Relativa (primeiro valor = zero)":
    refs = (
        df_final.sort_values("data_leitura")
        .groupby("sensor_id")["valor_sensor"]
        .first()
    )
    df_final["valor_grafico"] = df_final["valor_sensor"] - df_final["sensor_id"].map(refs)

# ======================================================
# HEADER – STATUS / BATERIA / ÚLTIMA TX
# ======================================================
info = df_final.sort_values("data_leitura").iloc[-1]

status = str(info["status"]).lower()
bateria = int(info["battery_percentage"]) if pd.notna(info["battery_percentage"]) else 0
ultima_tx = info["last_upload"]

if pd.notna(ultima_tx):
    ultima_tx = (ultima_tx - pd.Timedelta(hours=3)).strftime("%d-%m-%Y %H:%M:%S")

cor_status = "#22c55e" if status == "online" else "#ef4444"

if bateria >= 75:
    cor_bateria = "#22c55e"
elif bateria >= 40:
    cor_bateria = "#facc15"
else:
    cor_bateria = "#ef4444"

if len(devices_selecionados) == 1:
    st.markdown(
f"""
<div style="display:flex;align-items:center;gap:14px;padding:8px 0;">
<h3 style="margin:0;">{device_principal}</h3>

<span style="background:{cor_status};color:white;padding:4px 10px;border-radius:6px;font-size:14px;">
{status.capitalize()}
</span>

<div style="display:flex;align-items:center;gap:6px;background:#f3f4f6;padding:4px 10px;border-radius:6px;">
<div style="width:28px;height:12px;border:2px solid #111;border-radius:3px;">
<div style="width:{bateria}%;height:100%;background:{cor_bateria};"></div>
</div>
<strong>{bateria}%</strong>
</div>

<span style="color:#f97316;font-size:16px;">
⏱ Última transmissão: {ultima_tx}
</span>
</div>
""",
        unsafe_allow_html=True
    )


# ===============================
# GRÁFICO
# ===============================
df_final["serie"] = df_final["device_name"].astype(str) + " | " + df_final["tipo_sensor"].astype(str)

fig = go.Figure()

devices_unicos = list(dict.fromkeys(df_final["device_name"].tolist()))
device_index = {d: i for i, d in enumerate(devices_unicos)}

# 🎨 Paleta profissional por DEVICE
PALETA_DEVICES = [
    {
        "A-Axis Delta Angle": "#2563eb",   # azul
        "B-Axis Delta Angle": "#f97316",   # laranja
        "Device Temperature": "#a855f7",
        "Air Temperature": "#ef4444"
    },
    {
        "A-Axis Delta Angle": "#10b981",   # verde
        "B-Axis Delta Angle": "#ec4899",   # rosa
        "Device Temperature": "#6366f1",
        "Air Temperature": "#ef4444"
    },
    {
        "A-Axis Delta Angle": "#06b6d4",   # ciano
        "B-Axis Delta Angle": "#eab308",   # amarelo
        "Device Temperature": "#a855f7",
        "Air Temperature": "#ef4444"
    },
    {
        "A-Axis Delta Angle": "#8b5cf6",   # roxo
        "B-Axis Delta Angle": "#ef4444",   # vermelho
        "Device Temperature": "#a855f7",
        "Air Temperature": "#ef4444"
    }
]

for serie in df_final["serie"].unique():

    d = df_final[df_final["serie"] == serie]
    tipo = d["tipo_sensor"].iloc[0]
    device = d["device_name"].iloc[0]

    eixo_secundario = tipo in ["Device Temperature", "Air Temperature"]

    idx = device_index.get(device, 0)

    # 🎨 cores únicas por device (incluindo temperatura)
    if tipo == "A-Axis Delta Angle":
        cor_final = ["#2563eb","#10b981","#06b6d4","#8b5cf6","#f43f5e"][idx % 5]

    elif tipo == "B-Axis Delta Angle":
        cor_final = ["#f97316","#ec4899","#eab308","#22c55e","#0ea5e9"][idx % 5]

    elif tipo == "Device Temperature":
        cor_final = ["#a855f7","#6366f1","#9333ea","#c026d3","#7c3aed"][idx % 5]

    elif tipo == "Air Temperature":
        cor_final = ["#ef4444","#f59e0b","#14b8a6","#fb7185","#e11d48"][idx % 5]

    else:
        cor_final = "#000000"

    fig.add_trace(go.Scatter(
        x=d["data_leitura"],
        y=d["valor_grafico"],
        mode="lines",
        name=serie,
        yaxis="y2" if eixo_secundario else "y",
        line=dict(
            color=cor_final,
            dash="dash" if eixo_secundario else "solid"
        ),
        hovertemplate=
        "<b>%{x|%d/%m/%Y %H:%M:%S}</b><br>" +
        "%{fullData.name}<br>" +
        "Valor: %{y:.4f}<extra></extra>"
    ))



label_y = "Valor Absoluto" if modo_escala == "Absoluta" else "Δ Valor Relativo"

fig.update_layout(
    height=780,
    hovermode="x unified",
    dragmode="pan",
    legend=dict(
        orientation="h",
        y=-0.15,
        x=0.5,
        xanchor="center",
        title_text=""
    ),
    yaxis=dict(title=f"<b>{label_y}</b>"),
    yaxis2=dict(
        title="<b>Temperatura (°C)</b>",
        overlaying="y",
        side="right"
    )
)

fig.update_xaxes(showspikes=True, spikemode="across", spikesnap="cursor")
fig.update_yaxes(showspikes=True, spikemode="across", spikesnap="cursor")

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
# 🛰️ MAPA
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
    textposition="top center",
    textfont=dict(size=18, color="white")
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

st.plotly_chart(mapa, use_container_width=True, config={"scrollZoom": True})

# ======================================================
# 📋 TABELA + EXPORTAÇÃO CSV
# ======================================================
st.subheader("📋 Dados")

st.dataframe(
    df_final[["data_leitura", "device_name", "tipo_sensor", "valor_sensor", "valor_grafico"]],
    use_container_width=True
)

csv = df_final.to_csv(index=False).encode("utf-8")

st.download_button(
    "📥 Baixar CSV",
    csv,
    "dados_geotecnicos.csv",
    "text/csv"
)
