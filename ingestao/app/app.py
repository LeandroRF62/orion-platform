import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from pathlib import Path

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

st.set_page_config(page_title="Gestão Geotécnica Orion", layout="wide")

# ======================================================
# CORES E PALETAS
# ======================================================
CORES_SENSOR = {
    "A-Axis Delta Angle": "#2563eb",
    "B-Axis Delta Angle": "#059669",
    "Device Temperature": "#f59e0b",
    "Air Temperature": "#ef4444"
}
PALETA_DEVICES = ["#636EFA", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3", "#FF6692", "#B6E880"]

# ======================================================
# CARREGAMENTO DE DADOS
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
        ORDER BY l.data_leitura
    """
    df = pd.read_sql(query, engine)
    
    if not df.empty and "reference" in df.columns:
        df["reference"] = (
            df["reference"]
            .fillna("Sem Referência")
            .str.replace("–", "-", regex=False)
            .str.replace("—", "-", regex=False)
            .str.strip()
        )
    return df

df_raw = carregar_dados_db()

if df_raw.empty:
    st.warning("Sem dados disponíveis no banco de dados.")
    st.stop()

df_raw["data_leitura"] = pd.to_datetime(df_raw["data_leitura"]).dt.tz_localize(None)

# ======================================================
# SIDEBAR - FILTROS
# ======================================================
st.sidebar.button("🔄 Atualizar Dados", on_click=st.cache_data.clear)

RAMAIS_PERMITIDOS = [
    "Humberto - S11D", 
    "LPR - Brito", 
    "LPR - Renan", 
    "LPR - Witheney",
    "RBH - José", 
    "RBR - José", 
    "RFA - Léo Silva", 
    "RFA - Thiago"
]

with st.sidebar.expander("📍 Ramal", expanded=True):
    opcoes_no_banco = df_raw["reference"].unique().tolist()
    opcoes_finais = sorted([r for r in RAMAIS_PERMITIDOS if r in opcoes_no_banco])
    
    if not opcoes_finais:
        st.error("Nenhum dos ramais configurados foi encontrado no banco.")
        st.stop()
        
    ramal_selecionado = st.selectbox("Selecionar Ramal", opcoes_finais)

df_ramal = df_raw[df_raw["reference"] == ramal_selecionado]

if df_ramal.empty:
    st.info(f"Nenhum dado encontrado para {ramal_selecionado}.")
    st.stop()

with st.sidebar.expander("📶 Status de Conexão", expanded=True):
    status_disponiveis = df_ramal["status"].unique().tolist()
    status_selecionados = st.multiselect("Filtrar por Status", status_disponiveis, default=status_disponiveis)

df_status = df_ramal[df_ramal["status"].isin(status_selecionados)]

with st.sidebar.expander("🎛️ Dispositivo", expanded=True):
    tipos_disponiveis = sorted(df_status["tipo_sensor"].unique())
    tipos_selecionados = st.multiselect("Variáveis", tipos_disponiveis, default=[t for t in tipos_disponiveis if "Battery" not in t])
    
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

df_final = df_status[(df_status["device_name"].isin(devices_selecionados)) & (df_status["tipo_sensor"].isin(tipos_selecionados))].copy()

if df_final.empty:
    st.warning("Selecione ao menos uma variável e um dispositivo.")
    st.stop()

data_min, data_max = df_final["data_leitura"].min().date(), df_final["data_leitura"].max().date()
with st.sidebar.expander("📅 Período"):
    d_ini = st.date_input("Início", data_min)
    d_fim = st.date_input("Fim", data_max)

modo_escala = st.sidebar.radio("Escala", ["Absoluta", "Relativa (T0)"])
df_final = df_final[(df_final["data_leitura"].dt.date >= d_ini) & (df_final["data_leitura"].dt.date <= d_fim)]

if df_final.empty:
    st.error("Não há dados para o intervalo de datas selecionado.")
    st.stop()

if modo_escala == "Relativa (T0)":
    refs = df_final.sort_values("data_leitura").groupby("sensor_id")["valor_sensor"].transform("first")
    df_final["valor_grafico"] = df_final["valor_sensor"] - refs
else:
    df_final["valor_grafico"] = df_final["valor_sensor"]

# ======================================================
# GRÁFICO PRINCIPAL
# ======================================================
fig = go.Figure()
num_devs = len(devices_selecionados)
dev_col_map = {dev: PALETA_DEVICES[i % len(PALETA_DEVICES)] for i, dev in enumerate(devices_selecionados)}

for serie in (df_final["device_name"] + " | " + df_final["tipo_sensor"]).unique():
    d_plot = df_final[(df_final["device_name"] + " | " + df_final["tipo_sensor"]) == serie]
    tipo = d_plot["tipo_sensor"].iloc[0]
    nome_dev = d_plot["device_name"].iloc[0]
    
    eixo_2 = "Temperature" in tipo
    style = dict(width=2, color=dev_col_map[nome_dev] if num_devs > 1 else CORES_SENSOR.get(tipo, "#6b7280"))
    
    if "Air Temperature" in tipo:
        style["dash"] = "dash" if num_devs == 1 else "dot"
        if num_devs == 1: style["color"] = "#ef4444"

    fig.add_trace(go.Scatter(x=d_plot["data_leitura"], y=d_plot["valor_grafico"], 
                             name=serie, line=style, yaxis="y2" if eixo_2 else "y"))

fig.update_layout(
    height=650, 
    hovermode="x unified",
    yaxis=dict(title="Leitura", fixedrange=False), 
    yaxis2=dict(title="Temp (°C)", overlaying="y", side="right", fixedrange=False),
    xaxis=dict(title="Data/Hora", fixedrange=False),
    legend=dict(orientation="h", y=-0.2)
)

st.plotly_chart(fig, use_container_width=True, config={'scrollZoom': True, 'displayModeBar': True, 'displaylogo': False})

# ======================================================
# MAPA (ATUALIZADO COM BATERIA)
# ======================================================
st.subheader("🛰️ Localização dos Dispositivos")

# Criamos uma cópia dos dados filtrados pelo ramal/status para buscar a bateria
# Mesmo que o usuário não selecione o gráfico de bateria, o mapa tentará mostrar
df_bateria = df_status[df_status["tipo_sensor"].str.contains("Battery", case=False, na=False)].copy()

def get_battery_info(row):
    # Busca a última leitura de bateria para este device_name
    bat_val = df_bateria[df_bateria["device_name"] == row["device_name"]].sort_values("data_leitura", ascending=False)
    if not bat_val.empty:
        val = bat_val.iloc[0]["valor_sensor"]
        return f"{row['device_name']} ({val}V)"
    return row["device_name"]

df_mapa = df_status[["device_name", "latitude", "longitude", "status"]].drop_duplicates().dropna(subset=["latitude", "longitude"])

if not df_mapa.empty:
    # Aplicar a função para adicionar o valor da bateria ao nome
    df_mapa["label_mapa"] = df_mapa.apply(get_battery_info, axis=1)
    df_mapa["cor_ponto"] = df_mapa["status"].str.lower().apply(lambda x: "#00FF00" if x == "online" else "#FF0000")
    
    fig_mapa = go.Figure(go.Scattermapbox(
        lat=df_mapa["latitude"], lon=df_mapa["longitude"],
        mode="markers+text",
        marker=dict(size=12, color=df_mapa["cor_ponto"], opacity=0.9),
        text=df_mapa["label_mapa"], # Usando o novo label com bateria
        textfont=dict(size=14, color="white"), 
        textposition="top center",
        hoverinfo="text"
    ))

    fig_mapa.update_layout(
        height=600, margin=dict(l=0, r=0, t=0, b=0),
        mapbox=dict(
            accesstoken=MAPBOX_TOKEN, style="satellite-streets", zoom=15,
            center=dict(lat=df_mapa["latitude"].mean(), lon=df_mapa["longitude"].mean()),
        ),
        showlegend=False
    )
    st.plotly_chart(fig_mapa, use_container_width=True, config={'scrollZoom': True})
else:
    st.info("Coordenadas geográficas não disponíveis.")

# ======================================================
# TABELA E DOWNLOAD
# ======================================================
with st.expander("📋 Ver Tabela de Dados"):
    st.dataframe(df_final[["data_leitura", "device_name", "tipo_sensor", "valor_sensor"]], use_container_width=True)
    st.download_button("📥 CSV", df_final.to_csv(index=False).encode("utf-8"), "dados.csv", "text/csv")
