import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

# ======================================================
# CONFIGURAÇÕES E ESTILO CSS (RIGOROSO)
# ======================================================
st.set_page_config(page_title="Orion Platform", layout="wide")

# CSS para emular exatamente os componentes das imagens enviadas
st.markdown("""
    <style>
    /* Estilo do Header de Dispositivo */
    .device-header {
        display: flex;
        align-items: center;
        gap: 15px;
        padding: 10px 0;
        border-bottom: 1px solid #e2e8f0;
        margin-bottom: 20px;
    }
    .device-title {
        font-size: 24px;
        font-weight: 500;
        color: #1e293b;
    }
    .badge-online {
        background-color: #10b981;
        color: white;
        padding: 2px 12px;
        border-radius: 6px;
        font-size: 14px;
        font-weight: bold;
    }
    .badge-offline {
        background-color: #ef4444;
        color: white;
        padding: 2px 12px;
        border-radius: 6px;
        font-size: 14px;
        font-weight: bold;
    }
    .battery-pill {
        display: flex;
        align-items: center;
        background: #f1f5f9;
        border: 1px solid #cbd5e1;
        border-radius: 4px;
        padding: 2px 8px;
        font-family: monospace;
    }
    .last-trans-text {
        color: #f97316;
        font-size: 14px;
    }
    </style>
    """, unsafe_allow_html=True)

# ======================================================
# CONEXÃO E DADOS
# ======================================================
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / ".env")
engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

@st.cache_data(ttl=60)
def fetch_data():
    query = """
        SELECT l.data_leitura, l.valor_sensor, s.tipo_sensor, 
               d.device_name, d.reference, d.latitude, d.longitude, d.status, d.battery
        FROM leituras l
        JOIN sensores s ON l.sensor_id = s.sensor_id
        JOIN devices d ON s.device_id = d.device_id
        ORDER BY l.data_leitura DESC
    """
    return pd.read_sql(query, engine)

df = fetch_data()
df["data_leitura"] = pd.to_datetime(df["data_leitura"]).dt.tz_localize(None)

# ======================================================
# SIDEBAR (FILTROS)
# ======================================================
with st.sidebar:
    st.title("ORION")
    ramais = sorted(df["reference"].dropna().unique())
    ramal_sel = st.selectbox("Ramal", ramais)
    
    df_f = df[df["reference"] == ramal_sel]
    
    devs_disp = sorted(df_f["device_name"].unique())
    dev_principal = st.selectbox("Dispositivo Principal", devs_disp)
    outros = st.multiselect("Adicionar Outros (Comparações)", [d for d in devs_disp if d != dev_principal])
    
    vars_disp = sorted(df_f["tipo_sensor"].unique())
    vars_sel = st.multiselect("Variáveis", vars_disp, default=vars_disp[:1])

# Lista final de dispositivos para exibição
selecionados = [dev_principal] + outros

# ======================================================
# ÁREA DE STATUS (O "HEADER" DAS IMAGENS)
# ======================================================
# Só renderiza os cards se houver comparação ou para o principal de forma elegante
for d_name in selecionados:
    # Pegamos os dados mais recentes deste dispositivo específico
    latest = df[df["device_name"] == d_name].iloc[0]
    
    status_class = "badge-online" if latest['status'].lower() == 'online' else "badge-offline"
    
    # Cálculo de tempo passado
    minutos_atras = int((datetime.now() - latest['data_leitura']).total_seconds() / 60)
    
    st.markdown(f"""
        <div class="device-header">
            <div class="device-title">{d_name}</div>
            <div class="{status_class}">{latest['status'].upper()}</div>
            <div class="battery-pill">
                <span style="color: #10b981; margin-right: 5px;">⚡</span> {latest['battery']}%
            </div>
            <div class="last-trans-text">
                ⚠️ Last transmission {minutos_atras} minutes ago
            </div>
        </div>
        """, unsafe_allow_html=True)

# ======================================================
# GRÁFICO TÉCNICO
# ======================================================
df_plot = df[(df["device_name"].isin(selecionados)) & (df["tipo_sensor"].isin(vars_sel))]

if not df_plot.empty:
    fig = go.Figure()
    for d in selecionados:
        for v in vars_sel:
            sub = df_plot[(df_plot["device_name"] == d) & (df_plot["tipo_sensor"] == v)]
            if not sub.empty:
                fig.add_trace(go.Scatter(
                    x=sub["data_leitura"], 
                    y=sub["valor_sensor"],
                    name=f"{d} - {v}",
                    mode='lines+markers' if len(sub) < 50 else 'lines'
                ))

    fig.update_layout(
        height=500,
        hovermode="x unified",
        template="plotly_white",
        margin=dict(t=10),
        legend=dict(orientation="h", y=-0.2)
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Aguardando seleção de variáveis para gerar análise.")

# ======================================================
# RODAPÉ ESTILO DASHBOARD
# ======================================================
with st.expander("📋 Ver logs de dados brutos"):
    st.dataframe(df_plot, use_container_width=True)
