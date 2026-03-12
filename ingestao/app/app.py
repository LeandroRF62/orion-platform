import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from pathlib import Path

# ======================================================
# 1. CONFIGURAÇÕES, ENV E BANCO
# ======================================================
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")
APP_PASSWORD = os.getenv("APP_PASSWORD", "orion123")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)

# ======================================================
# 2. AUTENTICAÇÃO
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

st.set_page_config(page_title="Gestão Geotécnica Orion", layout="wide", initial_sidebar_state="expanded")

# ======================================================
# 3. CARREGAMENTO DE DADOS
# ======================================================
@st.cache_data(ttl=300)
def carregar_dados_db():
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE devices ADD COLUMN IF NOT EXISTS reference TEXT;"))
        conn.commit()
    query = """
        SELECT l.data_leitura, l.valor_sensor, s.sensor_id, s.tipo_sensor, 
               d.device_name, d.reference, d.latitude, d.longitude, d.status
        FROM leituras l
        JOIN sensores s ON l.sensor_id = s.sensor_id
        JOIN devices d ON s.device_id = d.device_id
        ORDER BY l.data_leitura ASC
    """
    return pd.read_sql(query, engine)

df_raw = carregar_dados_db()
if df_raw.empty:
    st.warning("Sem dados disponíveis no banco.")
    st.stop()

df_raw["data_leitura"] = pd.to_datetime(df_raw["data_leitura"]).dt.tz_localize(None)

# ======================================================
# 4. SIDEBAR - FILTROS (TUDO MANTIDO)
# ======================================================
st.sidebar.button("🔄 Atualizar Dados", on_click=st.cache_data.clear)

# Filtro de Ramal (Estrito conforme imagens)
with st.sidebar.expander("📍 Ramal", expanded=True):
    opcoes_ramais = [
        "Humberto - S11D", "LPR - Brito", "LPR - Renan", "LPR - Witheney",
        "RBH - José", "RBR - José", "RFA - Léo Silva", "RFA - Thiago"
    ]
    ramal_selecionado = st.selectbox("Selecionar Ramal", opcoes_ramais)

df_ramal = df_raw[df_raw["reference"] == ramal_selecionado]

# Filtro de Status (Online/Offline) - Reintegrado
with st.sidebar.expander("📶 Status de Conexão", expanded=True):
    status_disponiveis = sorted(df_ramal["status"].unique().tolist())
    status_selecionados = st.multiselect("Filtrar por Status", status_disponiveis, default=status_disponiveis)

df_status = df_ramal[df_ramal["status"].isin(status_selecionados)]

# Filtro de Dispositivo e Variáveis
with st.sidebar.expander("🎛️ Dispositivo", expanded=True):
    tipos_disponiveis = sorted(df_status["tipo_sensor"].unique())
    tipos_selecionados = st.multiselect("Variáveis", tipos_disponiveis, default=tipos_disponiveis)
    
    dispositivos_filtrados = sorted(df_status["device_name"].unique())
    if not dispositivos_filtrados:
        st.warning("Nenhum dispositivo com este status.")
        st.stop()
        
    selecionar_todos = st.checkbox("Selecionar todos deste ramal/status")
    
    if selecionar_todos:
        devices_selecionados = dispositivos_filtrados
    else:
        dev_principal = st.selectbox("Dispositivo Principal", dispositivos_filtrados)
        outros = st.multiselect("Adicionar Outros", [d for d in dispositivos_filtrados if d != dev_principal])
        devices_selecionados = [dev_principal] + outros

df_filt = df_status[(df_status["device_name"].isin(devices_selecionados)) & (df_status["tipo_sensor"].isin(tipos_selecionados))].copy()

# Filtro de Período e Escala
with st.sidebar.expander("📅 Período e Escala"):
    data_min, data_max = df_filt["data_leitura"].min().date(), df_filt["data_leitura"].max().date()
    d_ini = st.date_input("Início", data_min)
    d_fim = st.date_input("Fim", data_max)
    modo_escala = st.radio("Modo de Escala", ["Absoluta", "Relativa (T0 Original)", "Relativa (T0 no Zoom)"])

df_final = df_filt[(df_filt["data_leitura"].dt.date >= d_ini) & (df_filt["data_leitura"].dt.date <= d_fim)].copy()

# ======================================================
# 5. LÓGICA DE T0 DINÂMICO AO ZOOM
# ======================================================
if "relayout_data" not in st.session_state:
    st.session_state.relayout_data = None

def aplicar_calculo_escala(df, modo):
    df_res = df.copy()
    if modo == "Absoluta":
        df_res["valor_grafico"] = df_res["valor_sensor"]
    elif modo == "Relativa (T0 Original)":
        refs = df_res.sort_values("data_leitura").groupby("sensor_id")["valor_sensor"].transform("first")
        df_res["valor_grafico"] = df_res["valor_sensor"] - refs
    elif modo == "Relativa (T0 no Zoom)":
        zoom = st.session_state.relayout_data
        if zoom and 'xaxis.range[0]' in zoom:
            x_min = pd.to_datetime(zoom['xaxis.range[0]'])
            df_vis = df_res[df_res["data_leitura"] >= x_min]
            if not df_vis.empty:
                refs = df_vis.sort_values("data_leitura").groupby("sensor_id")["valor_sensor"].first()
                df_res["valor_grafico"] = df_res["valor_sensor"] - df_res["sensor_id"].map(refs)
            else:
                df_res["valor_grafico"] = df_res["valor_sensor"]
        else:
            refs = df_res.sort_values("data_leitura").groupby("sensor_id")["valor_sensor"].transform("first")
            df_res["valor_grafico"] = df_res["valor_sensor"] - refs
    return df_res

if modo_escala == "Relativa (T0 no Zoom)":
    st.info("🔍 Dê zoom no período desejado e clique no botão abaixo para zerar a escala naquele ponto.")
    if st.button("📌 Fixar T0 no Zoom Atual"):
        st.rerun()

df_plot = aplicar_calculo_escala(df_final, modo_escala)

# ======================================================
# 6. GRÁFICO (CORES MANTIDAS)
# ======================================================
fig = go.Figure()
CORES_SENSOR = {"A-Axis Delta Angle": "#2563eb", "B-Axis Delta Angle": "#059669", "Air Temperature": "#ef4444", "Device Temperature": "#f59e0b"}
PALETA_DEVICES = ["#636EFA", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3", "#FF6692", "#B6E880"]

num_devs = len(devices_selecionados)
dev_col_map = {dev: PALETA_DEVICES[i % len(PALETA_DEVICES)] for i, dev in enumerate(devices_selecionados)}

for serie in (df_plot["device_name"] + " | " + df_plot["tipo_sensor"]).unique():
    d_p = df_plot[(df_plot["device_name"] + " | " + df_plot["tipo_sensor"]) == serie]
    tipo = d_p["tipo_sensor"].iloc[0]
    nome_dev = d_p["device_name"].iloc[0]
    
    eixo_2 = "Temperature" in tipo
    color = dev_col_map[nome_dev] if num_devs > 1 else CORES_SENSOR.get(tipo, "#6b7280")
    dash = "dash" if "Air Temperature" in tipo else "solid"
    if num_devs == 1 and "Air Temperature" in tipo: color = "#ef4444"

    fig.add_trace(go.Scatter(x=d_p["data_leitura"], y=d_p["valor_grafico"], 
                             name=serie, line=dict(color=color, dash=dash, width=2), 
                             yaxis="y2" if eixo_2 else "y"))

fig.update_layout(height=650, hovermode="x unified",
                  yaxis=dict(title="Leitura"), yaxis2=dict(title="Temp (°C)", overlaying="y", side="right"),
                  legend=dict(orientation="h", y=-0.2))

# Key "main_chart" para capturar o relayout (zoom)
chart_evt = st.plotly_chart(fig, use_container_width=True, key="main_chart")

if st.session_state.main_chart and 'relayout' in st.session_state.main_chart:
    st.session_state.relayout_data = st.session_state.main_chart['relayout']

# ======================================================
# 7. MAPA (LETRAS BRANCAS E ZOOM MANTIDOS)
# ======================================================
st.subheader("🛰️ Localização")
df_mapa = df_final[["device_name", "latitude", "longitude", "status"]].drop_duplicates().dropna()
df_mapa["cor"] = df_mapa["status"].str.lower().apply(lambda x: "#00FF00" if x == "online" else "#FF0000")

fig_mapa = go.Figure(go.Scattermapbox(
    lat=df_mapa["latitude"], lon=df_mapa["longitude"], mode="markers+text",
    marker=dict(size=12, color=df_mapa["cor"]),
    text=df_mapa["device_name"],
    textfont=dict(size=14, color="white"),
    textposition="top center"
))

fig_mapa.update_layout(
    height=500, margin=dict(l=0, r=0, t=0, b=0),
    mapbox=dict(accesstoken=MAPBOX_TOKEN, style="satellite-streets", zoom=15,
                center=dict(lat=df_mapa["latitude"].mean(), lon=df_mapa["longitude"].mean()))
)
st.plotly_chart(fig_mapa, use_container_width=True, config={'scrollZoom': True})

# ======================================================
# 8. TABELA E DOWNLOAD
# ======================================================
with st.expander("📋 Ver Tabela de Dados"):
    st.dataframe(df_plot[["data_leitura", "device_name", "tipo_sensor", "valor_sensor"]], use_container_width=True)
    st.download_button("📥 CSV", df_plot.to_csv(index=False).encode("utf-8"), "dados_geotec.csv", "text/csv")
