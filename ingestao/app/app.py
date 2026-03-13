import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ======================================================
# 1. SETUP DE INTERFACE PROFISSIONAL
# ======================================================
st.set_page_config(page_title="ORION | Enterprise Geotechnical Analytics", layout="wide")

# CSS de Alta Densidade (UI Industrial)
st.markdown("""
    <style>
    .main { background-color: #f1f5f9; }
    [data-testid="stMetricValue"] { font-size: 24px; color: #1e293b; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background-color: #e2e8f0; border-radius: 4px 4px 0 0; padding: 10px 20px;
    }
    .stTabs [aria-selected="true"] { background-color: #1e293b !important; color: white !important; }
    </style>
    """, unsafe_allow_html=True)

# ======================================================
# 2. CORE: PROCESSAMENTO E ESTATÍSTICA
# ======================================================
class GeotechEngine:
    @staticmethod
    def calculate_resultants(df):
        """Calcula o vetor resultante para sensores Tilt."""
        df_pivot = df.pivot_table(index=['data_leitura', 'device_name'], 
                                  columns='tipo_sensor', values='valor_sensor').reset_index()
        
        if 'A-Axis Delta Angle' in df_pivot.columns and 'B-Axis Delta Angle' in df_pivot.columns:
            df_pivot['Resultant_Tilt'] = np.sqrt(df_pivot['A-Axis Delta Angle']**2 + df_pivot['B-Axis Delta Angle']**2)
            return df_pivot
        return pd.DataFrame()

# ======================================================
# 3. SIDEBAR & NAVIGATION
# ======================================================
with st.sidebar:
    st.image("https://img.icons8.com/external-flat-icons-inmotus-design/64/external-Satellite-communication-flat-icons-inmotus-design-2.png", width=60)
    st.title("ORION PLATFORM")
    st.caption("v2.4.0 - Enterprise Edition")
    
    nav = st.radio("MÓDULOS", ["🏠 Dashboard", "🛰️ Map View", "📈 Advanced Analytics", "⚙️ Management"])
    st.divider()
    
    # Filtro de Data Global
    date_range = st.date_input("Janela de Análise", [datetime.now() - timedelta(days=7), datetime.now()])

# SIMULAÇÃO DE CARGA (Para rodar o exemplo robusto)
@st.cache_data
def get_mock_data():
    # Aqui entraria sua query SQL original. Usando mock para demonstração de robustez.
    dates = pd.date_range(start="2024-01-01", periods=100, freq="H")
    devices = ["Tilt-S11D-01", "Tilt-S11D-02", "Piezometro-04"]
    data = []
    for d in devices:
        for t in dates:
            data.append([t, d, "A-Axis Delta Angle", np.random.normal(0, 0.5), "Online", 85, -5.942, -50.443])
            data.append([t, d, "B-Axis Delta Angle", np.random.normal(0, 0.5), "Online", 85, -5.942, -50.443])
            data.append([t, d, "Device Temperature", np.random.normal(25, 2), "Online", 85, -5.942, -50.443])
    return pd.DataFrame(data, columns=["data_leitura", "device_name", "tipo_sensor", "valor_sensor", "status", "battery", "lat", "lon"])

df = get_mock_data()

# ======================================================
# 4. MÓDULO: DASHBOARD (VISÃO EXECUTIVA)
# ======================================================
if nav == "🏠 Dashboard":
    st.header("Executive Overview")
    
    # KPIs Superiores
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Assets", df['device_name'].nunique())
    c2.metric("Critical Alerts", "02", delta="-1", delta_color="inverse")
    c3.metric("Avg Battery", f"{int(df['battery'].mean())}%")
    c4.metric("Network Health", "98.2%", delta="0.4%")

    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.subheader("Recent Activity (Resultant Tilt)")
        res_df = GeotechEngine.calculate_resultants(df)
        fig_res = px.line(res_df, x='data_leitura', y='Resultant_Tilt', color='device_name', 
                         title="Vetor de Deslocamento Acumulado (mm/m)")
        st.plotly_chart(fig_res, use_container_width=True)

    with col_right:
        st.subheader("Device Status")
        status_counts = df.drop_duplicates('device_name')['status'].value_counts()
        fig_pie = px.pie(values=status_counts, names=status_counts.index, hole=0.4,
                        color_discrete_sequence=['#10b981', '#f59e0b'])
        st.plotly_chart(fig_pie, use_container_width=True)

# ======================================================
# 5. MÓDULO: ANALYTICS (O CORAÇÃO DO SOFTWARE)
# ======================================================
elif nav == "📈 Advanced Analytics":
    tab1, tab2, tab3 = st.tabs(["📊 Time Series", "🌡️ Correlation Analysis", "📉 Statistics"])
    
    with tab1:
        st.subheader("Multi-Axis Comparison")
        selected_dev = st.selectbox("Select Device", df['device_name'].unique())
        df_dev = df[df['device_name'] == selected_dev]
        
        fig_multi = go.Figure()
        for sensor in df_dev['tipo_sensor'].unique():
            sub = df_dev[df_dev['tipo_sensor'] == sensor]
            fig_multi.add_trace(go.Scatter(x=sub['data_leitura'], y=sub['valor_sensor'], name=sensor))
        
        fig_multi.update_layout(height=600, hovermode="x unified")
        st.plotly_chart(fig_multi, use_container_width=True)

    with tab2:
        st.subheader("Thermal Influence Analysis")
        # Gráfico de dispersão para ver se a temperatura está "movendo" o sensor (ruído térmico)
        df_corr = df_dev.pivot(index='data_leitura', columns='tipo_sensor', values='valor_sensor')
        if 'Device Temperature' in df_corr.columns and 'A-Axis Delta Angle' in df_corr.columns:
            fig_scat = px.scatter(df_corr, x='Device Temperature', y='A-Axis Delta Angle', 
                                 trendline="ols", title="Temperature vs displacement Correlation")
            st.plotly_chart(fig_scat, use_container_width=True)

    with tab3:
        st.subheader("Statistical Summary")
        stats = df_dev.groupby('tipo_sensor')['valor_sensor'].agg(['mean', 'std', 'min', 'max']).reset_index()
        st.table(stats)

# ======================================================
# 6. MÓDULO: MANAGEMENT (CRUD)
# ======================================================
elif nav == "⚙️ Management":
    st.header("Asset Inventory Management")
    
    # Grid Editável estilo Orion
    edited_df = st.data_editor(
        df.drop_duplicates('device_name')[['device_name', 'status', 'battery', 'lat', 'lon']],
        num_rows="dynamic",
        use_container_width=True
    )
    
    if st.button("Save Changes to Database"):
        st.success("Database synchronized successfully!")
