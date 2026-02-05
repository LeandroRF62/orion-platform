import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
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

DATABASE_URL = os.getenv("DATABASE_URL")
MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")

if not DATABASE_URL:
    st.error("DATABASE_URL não configurada")
    st.stop()

engine = create_engine(DATABASE_URL)

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

df["data_leitura"] = pd.to_datetime(df["data_leitura"], errors="coerce").dt.tz_localize(None)
df["last_upload"] = pd.to_datetime(df["last_upload"], errors="coerce")

if df.empty:
    st.warning("Nenhum dado encontrado ainda.")
    st.stop()

# ===============================
# FILTROS
# ===============================
tipos_selecionados = st.sidebar.multiselect(
    "Variável do Dispositivo",
    sorted(df["tipo_sensor"].unique()),
    default=sorted(df["tipo_sensor"].unique())
)

df_tipo = df[df["tipo_sensor"].isin(tipos_selecionados)]

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

df_devices = df[["device_name", "status"]].drop_duplicates()
df_devices["status_lower"] = df_devices["status"].astype(str).str.lower()

if status_permitidos:
    df_devices = df_devices[df_devices["status_lower"].isin(status_permitidos)]

df_devices["status_str"] = df_devices["status_lower"].map({
    "online": "🟢 Online",
    "offline": "🔴 Offline"
}).fillna("⚪ Desconhecido")

df_devices["label"] = df_devices["device_name"] + " – " + df_devices["status_str"]

device_label_map = dict(zip(df_devices["label"], df_devices["device_name"]))

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
    [device_principal] + [device_label_map[l] for l in outros_labels]
))

# ===============================
# PERÍODO
# ===============================
st.sidebar.subheader("📅 Período de Análise")

data_min = df_tipo["data_leitura"].min().date()
data_max = df_tipo["data_leitura"].max().date()

c1, c2 = st.sidebar.columns(2)
data_ini = c1.date_input("Data inicial", data_min)
data_fim = c2.date_input("Data final", data_max)

df_final = df_tipo[
    (df_tipo["device_name"].isin(devices_selecionados)) &
    (df_tipo["data_leitura"] >= pd.to_datetime(data_ini)) &
    (df_tipo["data_leitura"] < pd.to_datetime(data_fim) + pd.Timedelta(days=1))
].copy()

if df_final.empty:
    st.warning("Nenhum dado no período.")
    st.stop()

# ======================================================
# 🚨 CONFIGURAÇÃO DE LIMITES DE ALERTA (NOVO)
# ======================================================
st.sidebar.markdown("### 🚨 Limites de Alerta")

limites = {}

for tipo in df_final["tipo_sensor"].unique():

    valor = st.sidebar.number_input(
        f"Limite – {tipo}",
        value=0.0,
        step=0.1,
        key=f"limite_{tipo}"
    )

    limites[tipo] = valor

if st.sidebar.button("💾 Salvar limites"):

    device_id_atual = df_final.iloc[-1]["device_id"]

    with engine.begin() as conn:

        for tipo, valor in limites.items():

            conn.execute(text("""
                INSERT INTO alert_limits (
                    device_id,
                    tipo_sensor,
                    limite_valor
                )
                VALUES (:device_id,:tipo,:valor)
            """), {
                "device_id": device_id_atual,
                "tipo": tipo,
                "valor": valor
            })

    st.sidebar.success("Limites salvos!")

# ===============================
# ZERO REFERÊNCIA OTIMIZADO
# ===============================
modo_escala = st.sidebar.radio(
    "Escala de Visualização",
    ["Absoluta", "Relativa"]
)

if modo_escala == "Relativa":

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
        refs = {sid: 0 for sid in df_final["sensor_id"].unique()}

    df_final["valor_grafico"] = df_final["valor_sensor"] - df_final["sensor_id"].map(refs)
    label_y = "Variação Relativa"
else:
    df_final["valor_grafico"] = df_final["valor_sensor"]
    label_y = "Valor Absoluto"

# ===============================
# HEADER DO DISPOSITIVO
# ===============================
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
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:14px;padding:8px 0;">
      <h3 style="margin:0;">{device_principal}</h3>
      <span style="background:{cor_status};color:white;padding:4px 10px;border-radius:6px;">
        {status.capitalize()}
      </span>
      <div style="display:flex;align-items:center;gap:6px;background:#f3f4f6;padding:4px 10px;border-radius:6px;">
        <div style="width:28px;height:12px;border:2px solid #111;border-radius:3px;">
          <div style="width:{bateria}%;height:100%;background:{cor_bateria};"></div>
        </div>
        <strong>{bateria}%</strong>
      </div>
      <span style="color:#f97316;">
        ⏱ Última transmissão: {ultima_tx}
      </span>
    </div>
    """, unsafe_allow_html=True)

# ===============================
# GRÁFICO
# ===============================
df_final["serie"] = df_final["device_name"] + " | " + df_final["tipo_sensor"]

fig = px.line(
    df_final,
    x="data_leitura",
    y="valor_grafico",
    color="serie",
    template="plotly_white"
)

fig.update_layout(
    height=780,
    hovermode="closest",
    legend=dict(
        orientation="h",
        y=-0.15,
        x=0.5,
        xanchor="center",
        font=dict(size=17),
        title_text=""
    ),
    dragmode="pan"
)

fig.update_xaxes(
    tickfont=dict(size=17),
    showspikes=True,
    spikemode="across",
    spikesnap="cursor"
)

fig.update_yaxes(
    title_text=f"<b>{label_y}</b>",
    title_font=dict(size=20),
    tickfont=dict(size=17),
    showspikes=True,
    spikemode="across",
    spikesnap="cursor"
)

st.plotly_chart(
    fig,
    use_container_width=True,
    config={"editable": True, "scrollZoom": True}
)

# ===============================
# MAPA
# ===============================
st.subheader("🛰️ Localização dos Dispositivos")

df_mapa = df_final[
    ["device_name", "latitude", "longitude", "status"]
].drop_duplicates().dropna(subset=["latitude", "longitude"])

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

st.plotly_chart(
    mapa,
    use_container_width=True,
    config={"scrollZoom": True}
)

# ===============================
# TABELA
# ===============================
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

# ===============================
# EXPORTAÇÃO
# ===============================
csv = df_final.to_csv(index=False).encode("utf-8")
st.download_button(
    "📥 Baixar CSV",
    csv,
    "dados_geotecnicos.csv",
    "text/csv"
)
