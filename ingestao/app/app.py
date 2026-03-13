import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from pathlib import Path

# ======================================================
# CONFIGURAÇÃO E CSS (ORION STYLE)
# ======================================================
st.set_page_config(page_title="Orion | Gestão Geotécnica", layout="wide")

st.markdown("""
    <style>
    /* Estilo barra lateral Orion */
    [data-testid="stSidebar"] { background-color: #1e293b; color: white; }
    [data-testid="stSidebar"] * { color: white !important; }
    .main { background-color: #f8fafc; }
    
    /* Indicadores de Status */
    .status-online { color: #10b981; font-weight: bold; }
    .status-offline { color: #ef4444; font-weight: bold; }
    </style>
    """, unsafe_allow_items=True)

# ======================================================
# ENV & DATABASE (Preservado)
# ======================================================
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")
DATABASE_URL = os.getenv("DATABASE_URL")
MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")
APP_PASSWORD = os.getenv("APP_PASSWORD", "orion123")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)

# ======================================================
# AUTENTICAÇÃO (Preservado)
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
# CARREGAMENTO DE DADOS
# ======================================================
@st.cache_data(ttl=300)
def carregar_dados_db():
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE devices ADD COLUMN IF NOT EXISTS reference TEXT;"))
        conn.execute(text("ALTER TABLE devices ADD COLUMN IF NOT EXISTS battery INTEGER;")) # Coluna para bateria
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
# SIDEBAR - MENU DE NAVEGAÇÃO (NOVO)
# ======================================================
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/1063/1063302.png", width=50) # Logo placeholder
    st.title("ORION")
    menu = st.radio("Navegação", ["📍 Map View", "🖥️ Devices", "📊 Charts & Data"])
    st.divider()
    
    # Filtros Globais (Aparecem em todas as abas ou apenas em Gráficos)
    opcoes_ramais = sorted(df_raw["reference"].unique().tolist())
    ramal_selecionado = st.selectbox("📍 Selecionar Ramal", opcoes_ramais if opcoes_ramais else ["Nenhum"])
    
    st.button("🔄 Sincronizar", on_click=st.cache_data.clear, use_container_width=True)

# Filtragem base do ramal
df_ramal = df_raw[df_raw["reference"] == ramal_selecionado]

# ======================================================
# ABA 1: MAP VIEW (Image reference: image_e7d214.jpg)
# ======================================================
if menu == "📍 Map View":
    st.subheader(f"Área da Mina: {ramal_selecionado}")
    
    df_mapa = df_ramal[["device_name", "latitude", "longitude", "status"]].drop_duplicates().dropna()
    df_mapa["cor_ponto"] = df_mapa["status"].str.lower().apply(lambda x: "#00FF00" if x == "online" else "#FF0000")

    fig_mapa = go.Figure(go.Scattermapbox(
        lat=df_mapa["latitude"], lon=df_mapa["longitude"],
        mode="markers+text",
        marker=dict(size=14, color=df_mapa["cor_ponto"], opacity=0.9),
        text=df_mapa["device_name"],
        textfont=dict(size=12, color="white"),
        textposition="top center",
        hoverinfo="text"
    ))

    fig_mapa.update_layout(
        height=750, margin=dict(l=0, r=0, t=0, b=0),
        mapbox=dict(
            accesstoken=MAPBOX_TOKEN, style="satellite-streets", zoom=15,
            center=dict(lat=df_mapa["latitude"].mean(), lon=df_mapa["longitude"].mean()),
        ),
        showlegend=False
    )
    st.plotly_chart(fig_mapa, use_container_width=True)

# ======================================================
# ABA 2: DEVICES (Image reference: image_e7d1f1.jpg)
# ======================================================
elif menu == "🖥️ Devices":
    st.subheader("Gerenciamento de Dispositivos (Grid)")
    
    # Prepara dataframe para o Grid estilo Orion
    df_grid = df_ramal[["device_name", "battery", "status", "latitude", "longitude"]].drop_duplicates()
    
    # Traduzindo Status para ícones
    df_grid["Status"] = df_grid["status"].apply(lambda x: "🟢 Online" if x.lower() == 'online' else "🔴 Offline")

    st.data_editor(
        df_grid,
        column_config={
            "device_name": "Device Name",
            "battery": st.column_config.ProgressColumn(
                "Battery %", format="%d%%", min_value=0, max_value=100
            ),
            "Status": "Status",
            "latitude": "Lat",
            "longitude": "Lon",
        },
        use_container_width=True,
        hide_index=True,
        disabled=True
    )

# ======================================================
# ABA 3: CHARTS & DATA (Lógica original de Gráficos)
# ======================================================
elif menu == "📊 Charts & Data":
    st.subheader("Análise Temporal e Histórico")
    
    # Filtros específicos para Gráficos
    c1, c2, c3 = st.columns(3)
    with c1:
        status_selecionados = st.multiselect("Status", df_ramal["status"].unique(), default=df_ramal["status"].unique())
    with c2:
        tipos_selecionados = st.multiselect("Variáveis", df_ramal["tipo_sensor"].unique(), default=df_ramal["tipo_sensor"].unique())
    with c3:
        devices_selecionados = st.multiselect("Dispositivos", df_ramal["device_name"].unique(), default=df_ramal["device_name"].unique()[:2])

    df_final = df_ramal[
        (df_ramal["status"].isin(status_selecionados)) & 
        (df_ramal["tipo_sensor"].isin(tipos_selecionados)) &
        (df_ramal["device_name"].isin(devices_selecionados))
    ].copy()

    # Modo de escala
    modo_escala = st.radio("Modo de Exibição", ["Absoluta", "Relativa (T0)"], horizontal=True)
    
    if modo_escala == "Relativa (T0)":
        refs = df_final.sort_values("data_leitura").groupby("sensor_id")["valor_sensor"].transform("first")
        df_final["valor_grafico"] = df_final["valor_sensor"] - refs
    else:
        df_final["valor_grafico"] = df_final["valor_sensor"]

    # Gráfico Plotly (Seu código original otimizado)
    fig = go.Figure()
    for serie in (df_final["device_name"] + " | " + df_final["tipo_sensor"]).unique():
        d_plot = df_final[(df_final["device_name"] + " | " + df_final["tipo_sensor"]) == serie]
        fig.add_trace(go.Scatter(x=d_plot["data_leitura"], y=d_plot["valor_grafico"], name=serie, line=dict(width=2)))

    fig.update_layout(height=500, hovermode="x unified", template="plotly_white")
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("📥 Exportar Dados"):
        st.dataframe(df_final, use_container_width=True)
        st.download_button("Baixar CSV", df_final.to_csv(index=False).encode("utf-8"), "dados_orion.csv")
