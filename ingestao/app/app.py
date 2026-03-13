import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

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
# CONFIGURAÇÃO E ESTILOS CUSTOMIZADOS (Estilo Orion)
# ======================================================
st.set_page_config(page_title="Gestão Geotécnica Orion", layout="wide")

st.markdown("""
    <style>
    /* Badges de Status */
    .status-badge {
        padding: 4px 12px;
        border-radius: 4px;
        color: white;
        font-weight: bold;
        font-size: 14px;
        margin-right: 10px;
    }
    .status-online { background-color: #10b981; }
    .status-offline { background-color: #ef4444; }
    
    /* Indicador de Bateria */
    .battery-box {
        background-color: #e2e8f0;
        border-radius: 4px;
        padding: 2px 8px;
        border: 1px solid #cbd5e1;
        display: inline-block;
        font-size: 13px;
        font-weight: bold;
    }
    
    /* Texto de Transmissão */
    .last-trans {
        color: #64748b;
        font-size: 13px;
        margin-left: 10px;
    }
    
    /* Container do Header */
    .device-header-card {
        background-color: #f8fafc;
        padding: 15px;
        border-radius: 8px;
        border-left: 5px solid #1e293b;
        margin-bottom: 20px;
    }
    </style>
    """, unsafe_allow_html=True)

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

# ======================================================
# CARREGAMENTO DE DADOS (Atualizado com Battery)
# ======================================================
@st.cache_data(ttl=300)
def carregar_dados_db():
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE devices ADD COLUMN IF NOT EXISTS reference TEXT;"))
        conn.execute(text("ALTER TABLE devices ADD COLUMN IF NOT EXISTS battery INTEGER;")) # Garante coluna battery
        conn.commit()
    query = """
        SELECT l.data_leitura, l.valor_sensor, s.sensor_id, s.tipo_sensor, 
               d.device_name, d.reference, d.latitude, d.longitude, d.status, d.battery
        FROM leituras l
        JOIN sensores s ON l.sensor_id = s.sensor_id
        JOIN devices d ON s.device_id = d.device_id
        ORDER BY l.data_leitura
    """
    return pd.read_sql(query, engine)

df_raw = carregar_dados_db()
if df_raw.empty:
    st.warning("Sem dados disponíveis.")
    st.stop()

df_raw["data_leitura"] = pd.to_datetime(df_raw["data_leitura"]).dt.tz_localize(None)

# ======================================================
# SIDEBAR - FILTROS
# ======================================================
st.sidebar.button("🔄 Atualizar Dados", on_click=st.cache_data.clear)

with st.sidebar.expander("📍 Ramal", expanded=True):
    opcoes_ramais = sorted(df_raw["reference"].dropna().unique().tolist())
    ramal_selecionado = st.selectbox("Selecionar Ramal", opcoes_ramais if opcoes_ramais else ["Geral"])

df_ramal = df_raw[df_raw["reference"] == ramal_selecionado]

with st.sidebar.expander("📶 Status", expanded=True):
    status_disp = df_ramal["status"].unique().tolist()
    status_sel = st.multiselect("Filtrar Status", status_disp, default=status_disp)

df_status = df_ramal[df_ramal["status"].isin(status_sel)]

with st.sidebar.expander("🎛️ Dispositivo", expanded=True):
    tipos_sel = st.multiselect("Variáveis", sorted(df_status["tipo_sensor"].unique()), default=sorted(df_status["tipo_sensor"].unique())[:1])
    dispositivos_filtrados = sorted(df_status["device_name"].unique())
    
    # Lógica de Seleção
    dev_principal = st.selectbox("Dispositivo Principal", dispositivos_filtrados)
    outros = st.multiselect("Adicionar Outros", [d for d in dispositivos_filtrados if d != dev_principal])
    devices_selecionados = [dev_principal] + outros

# Filtragem Final
df_final = df_status[(df_status["device_name"].isin(devices_selecionados)) & (df_status["tipo_sensor"].isin(tipos_sel))].copy()

# Escala e Período
modo_escala = st.sidebar.radio("Escala", ["Absoluta", "Relativa (T0)"])
if modo_escala == "Relativa (T0)":
    refs = df_final.sort_values("data_leitura").groupby("sensor_id")["valor_sensor"].transform("first")
    df_final["valor_grafico"] = df_final["valor_sensor"] - refs
else:
    df_final["valor_grafico"] = df_final["valor_sensor"]

# ======================================================
# CABEÇALHO DE STATUS (SÓ APARECE SE 'OUTROS' TIVER ITENS)
# ======================================
if outros:
    st.markdown("### 🖥️ Status dos Ativos Selecionados")
    for dev in devices_selecionados:
        # Extrai info do último registro desse dispositivo
        info = df_status[df_status["device_name"] == dev].iloc[-1]
        status_classe = "status-online" if info['status'].lower() == 'online' else "status-offline"
        
        # Cálculo de tempo de transmissão (simulado com a última leitura)
        ultima_leitura = info['data_leitura']
        diff_min = int((datetime.now() - ultima_leitura).total_seconds() / 60)
        
        st.markdown(f"""
            <div class="device-header-card">
                <span style="font-size: 18px; font-weight: bold;">{dev}</span>
                <span class="status-badge {status_classe}">{info['status'].upper()}</span>
                <div class="battery-box">🔋 {info['battery']}%</div>
                <span class="last-trans">🕒 Última transmissão: {diff_min} minutos atrás</span>
            </div>
            """, unsafe_allow_html=True)

# ======================================================
# GRÁFICO PRINCIPAL
# ======================================================
PALETA_DEVICES = ["#2563eb", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899"]
fig = go.Figure()

for i, serie in enumerate((df_final["device_name"] + " | " + df_final["tipo_sensor"]).unique()):
    d_plot = df_final[(df_final["device_name"] + " | " + df_final["tipo_sensor"]) == serie]
    fig.add_trace(go.Scatter(
        x=d_plot["data_leitura"], 
        y=d_plot["valor_grafico"], 
        name=serie,
        line=dict(width=2, color=PALETA_DEVICES[i % len(PALETA_DEVICES)])
    ))

fig.update_layout(height=500, hovermode="x unified", template="plotly_white", legend=dict(orientation="h", y=-0.2))
st.plotly_chart(fig, use_container_width=True)

# ======================================================
# MAPA E TABELA (Mantidos do anterior)
# ======================================================
st.subheader("🛰️ Localização")
df_mapa = df_final[["device_name", "latitude", "longitude", "status"]].drop_duplicates().dropna()
if not df_mapa.empty:
    fig_mapa = go.Figure(go.Scattermapbox(
        lat=df_mapa["latitude"], lon=df_mapa["longitude"],
        mode="markers+text",
        marker=dict(size=12, color="#ef4444"),
        text=df_mapa["device_name"],
        textfont=dict(color="white"),
        textposition="top center"
    ))
    fig_mapa.update_layout(
        margin=dict(l=0, r=0, t=0, b=0), height=400,
        mapbox=dict(accesstoken=MAPBOX_TOKEN, style="satellite-streets", zoom=14,
                    center=dict(lat=df_mapa["latitude"].mean(), lon=df_mapa["longitude"].mean()))
    )
    st.plotly_chart(fig_mapa, use_container_width=True)
