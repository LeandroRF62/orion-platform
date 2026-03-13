import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from pathlib import Path

# ======================================================
# 1. CONFIGURAÇÃO DA PÁGINA E ESTILO ORION
# ======================================================
st.set_page_config(page_title="Orion | Gestão Geotécnica", layout="wide")

# CSS Corrigido para evitar o erro de TypeError
st.markdown("""
    <style>
    /* Estilo barra lateral Orion (Azul Escuro) */
    [data-testid="stSidebar"] { 
        background-color: #1e293b; 
    }
    [data-testid="stSidebar"] * { 
        color: white !important; 
    }
    /* Estilo do fundo da página */
    .main { 
        background-color: #f8fafc; 
    }
    /* Customização de botões e inputs na sidebar */
    .stSelectbox label, .stMultiSelect label {
        color: white !important;
    }
    </style>
    """, unsafe_allow_html=True)

# ======================================================
# 2. AMBIENTE E BANCO DE DADOS
# ======================================================
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")
APP_PASSWORD = os.getenv("APP_PASSWORD", "orion123")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)

# ======================================================
# 3. AUTENTICAÇÃO
# ======================================================
if "auth_ok" not in st.session_state:
    st.session_state.auth_ok = False

if not st.session_state.auth_ok:
    st.title("🔐 Orion Platform - Acesso Restrito")
    col_auth, _ = st.columns([1, 2])
    with col_auth:
        senha = st.text_input("Senha de Acesso", type="password")
        if st.button("Entrar", use_container_width=True):
            if senha == APP_PASSWORD:
                st.session_state.auth_ok = True
                st.rerun()
            else:
                st.error("Senha incorreta")
    st.stop()

# ======================================================
# 4. CARREGAMENTO DE DADOS (CACHED)
# ======================================================
@st.cache_data(ttl=300)
def carregar_dados_db():
    with engine.connect() as conn:
        # Garante que colunas extras existam para o visual Orion
        conn.execute(text("ALTER TABLE devices ADD COLUMN IF NOT EXISTS reference TEXT;"))
        conn.execute(text("ALTER TABLE devices ADD COLUMN IF NOT EXISTS battery INTEGER;"))
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

try:
    df_raw = carregar_dados_db()
except Exception as e:
    st.error(f"Erro ao conectar ao banco: {e}")
    st.stop()

if df_raw.empty:
    st.warning("Sem dados disponíveis no banco.")
    st.stop()

df_raw["data_leitura"] = pd.to_datetime(df_raw["data_leitura"]).dt.tz_localize(None)

# ======================================================
# 5. SIDEBAR - NAVEGAÇÃO ESTILO ORION
# ======================================================
with st.sidebar:
    st.title("🌐 ORION")
    st.write("Geotechnical Monitoring")
    st.divider()
    
    # Menu Principal
    menu = st.radio(
        "Menu Principal", 
        ["📍 Map View", "🖥️ Devices", "📊 Charts & Analysis"],
        index=0
    )
    
    st.divider()
    
    # Filtro de Ramal (Global)
    ramais = sorted(df_raw["reference"].unique().tolist())
    ramal_selecionado = st.selectbox("Selecionar Ramal/Mina", ramais if ramais else ["Geral"])
    
    st.divider()
    if st.button("🔄 Sincronizar Dados", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# Filtragem base pelo ramal selecionado
df_ramal = df_raw[df_raw["reference"] == ramal_selecionado] if ramais else df_raw

# ======================================================
# 6. VISÕES DO APLICATIVO
# ======================================================

# --- ABA: MAP VIEW ---
if menu == "📍 Map View":
    st.subheader(f"Visualização de Ativos: {ramal_selecionado}")
    
    df_mapa = df_ramal[["device_name", "latitude", "longitude", "status"]].drop_duplicates().dropna()
    
    if not df_mapa.empty:
        df_mapa["cor_ponto"] = df_mapa["status"].str.lower().apply(
            lambda x: "#10b981" if x == "online" else "#ef4444"
        )

        fig_mapa = go.Figure(go.Scattermapbox(
            lat=df_mapa["latitude"], lon=df_mapa["longitude"],
            mode="markers+text",
            marker=dict(size=15, color=df_mapa["cor_ponto"], opacity=0.8),
            text=df_mapa["device_name"],
            textfont=dict(size=12, color="white"),
            textposition="top center",
            hoverinfo="text"
        ))

        fig_mapa.update_layout(
            height=700, margin=dict(l=0, r=0, t=0, b=0),
            mapbox=dict(
                accesstoken=MAPBOX_TOKEN,
                style="satellite-streets",
                zoom=14,
                center=dict(lat=df_mapa["latitude"].mean(), lon=df_mapa["longitude"].mean()),
            ),
            showlegend=False
        )
        st.plotly_chart(fig_mapa, use_container_width=True, config={'scrollZoom': True})
    else:
        st.info("Nenhum dado de geolocalização disponível para este ramal.")

# --- ABA: DEVICES (O GRID DO ORION) ---
elif menu == "🖥️ Devices":
    st.subheader("Inventory & Device Health")
    
    # Prepara a tabela estilo Orion
    df_grid = df_ramal[["device_name", "status", "battery", "latitude", "longitude"]].drop_duplicates()
    
    # Formatação de Status
    df_grid["status"] = df_grid["status"].apply(lambda x: "🟢 Online" if x.lower() == 'online' else "🔴 Offline")

    st.data_editor(
        df_grid,
        column_config={
            "device_name": st.column_config.TextColumn("Device Name", width="medium"),
            "status": st.column_config.TextColumn("Status"),
            "battery": st.column_config.ProgressColumn(
                "Battery %", 
                format="%d%%", 
                min_value=0, 
                max_value=100
            ),
            "latitude": "Latitude",
            "longitude": "Longitude"
        },
        use_container_width=True,
        hide_index=True,
        disabled=True
    )

# --- ABA: CHARTS & ANALYSIS ---
elif menu == "📊 Charts & Analysis":
    st.subheader("Análise de Sensores")
    
    # Filtros específicos de gráfico
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        tipos_disp = sorted(df_ramal["tipo_sensor"].unique())
        tipos_sel = st.multiselect("Variáveis", tipos_disp, default=tipos_disp[:1])
    with col_f2:
        devs_disp = sorted(df_ramal["device_name"].unique())
        devs_sel = st.multiselect("Dispositivos", devs_disp, default=devs_disp[:1])

    df_plot = df_ramal[
        (df_ramal["tipo_sensor"].isin(tipos_sel)) & 
        (df_ramal["device_name"].isin(devs_sel))
    ].copy()

    if not df_plot.empty:
        modo_escala = st.radio("Escala", ["Absoluta", "Relativa (T0)"], horizontal=True)
        
        if modo_escala == "Relativa (T0)":
            refs = df_plot.sort_values("data_leitura").groupby("sensor_id")["valor_sensor"].transform("first")
            df_plot["valor_vizia"] = df_plot["valor_sensor"] - refs
        else:
            df_plot["valor_vizia"] = df_plot["valor_sensor"]

        fig_analise = go.Figure()
        for nome_serie in (df_plot["device_name"] + " - " + df_plot["tipo_sensor"]).unique():
            d_sub = df_plot[(df_plot["device_name"] + " - " + df_plot["tipo_sensor"]) == nome_serie]
            fig_analise.add_trace(go.Scatter(
                x=d_sub["data_leitura"], 
                y=d_sub["valor_vizia"], 
                name=nome_serie,
                mode='lines'
            ))

        fig_analise.update_layout(
            height=550, 
            hovermode="x unified", 
            template="plotly_white",
            legend=dict(orientation="h", y=-0.2)
        )
        st.plotly_chart(fig_analise, use_container_width=True)
        
        with st.expander("📥 Exportar Dados Selecionados"):
            st.dataframe(df_plot, use_container_width=True)
            csv = df_plot.to_csv(index=False).encode('utf-8')
            st.download_button("Download CSV", csv, "export_orion.csv", "text/csv")
    else:
        st.info("Selecione os dispositivos e variáveis para gerar o gráfico.")
