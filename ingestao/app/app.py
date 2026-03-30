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
# FUNÇÕES DE BANCO DE DADOS
# ======================================================
@st.cache_data(ttl=300)
def carregar_dados_db():
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE devices ADD COLUMN IF NOT EXISTS reference TEXT;"))
        conn.execute(text("ALTER TABLE devices ADD COLUMN IF NOT EXISTS battery_percentage FLOAT;"))
        conn.commit()
    
    query = """
        SELECT l.data_leitura, l.valor_sensor, s.sensor_id, s.tipo_sensor, 
               d.device_name, d.reference, d.latitude, d.longitude, d.status,
               d.battery_percentage
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

def apagar_leitura_db(data_hora, device_name, tipo_sensor):
    try:
        with engine.connect() as conn:
            query_find = text("""
                SELECT s.sensor_id 
                FROM sensores s 
                JOIN devices d ON s.device_id = d.device_id 
                WHERE d.device_name = :dev AND s.tipo_sensor = :tipo
            """)
            result = conn.execute(query_find, {"dev": device_name, "tipo": tipo_sensor}).fetchone()
            
            if result:
                sid = result[0]
                query_del = text("""
                    DELETE FROM leituras 
                    WHERE sensor_id = :sid AND data_leitura = :dt
                """)
                conn.execute(query_del, {"sid": sid, "dt": data_hora})
                conn.commit()
                return True
    except Exception as e:
        st.error(f"Erro ao deletar no banco de dados: {e}")
    return False

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
# PROCESSAMENTO DE FILTROS
# ======================================================
df_raw = carregar_dados_db()
if df_raw.empty:
    st.warning("Sem dados disponíveis.")
    st.stop()

df_raw["data_leitura"] = pd.to_datetime(df_raw["data_leitura"]).dt.tz_localize(None)

st.sidebar.button("🔄 Atualizar Dados", on_click=st.cache_data.clear)

RAMAIS_PERMITIDOS = ["Humberto - S11D", "LPR - Brito", "LPR - Renan", "LPR - Witheney", "RBH - José", "RBR - José", "RFA - Léo Silva", "RFA - Thiago"]

with st.sidebar.expander("📍 Ramal", expanded=True):
    opcoes_no_banco = df_raw["reference"].unique().tolist()
    opcoes_finais = sorted([r for r in RAMAIS_PERMITIDOS if r in opcoes_no_banco])
    if not opcoes_finais: st.stop()
    ramal_selecionado = st.selectbox("Selecionar Ramal", opcoes_finais)

df_ramal = df_raw[df_raw["reference"] == ramal_selecionado]

with st.sidebar.expander("📶 Status", expanded=True):
    status_selecionados = st.multiselect("Status", df_ramal["status"].unique(), default=df_ramal["status"].unique())

df_status = df_ramal[df_ramal["status"].isin(status_selecionados)]

with st.sidebar.expander("🎛️ Dispositivo", expanded=True):
    tipos_selecionados = st.multiselect("Variáveis", sorted(df_status["tipo_sensor"].unique()), default=sorted(df_status["tipo_sensor"].unique()))
    dispositivos_filtrados = sorted(df_status["device_name"].unique())
    selecionar_todos = st.checkbox("Todos deste ramal")
    devices_selecionados = dispositivos_filtrados if selecionar_todos else [st.selectbox("Principal", dispositivos_filtrados)] + st.multiselect("Outros", [d for d in dispositivos_filtrados])

df_final = df_status[(df_status["device_name"].isin(devices_selecionados)) & (df_status["tipo_sensor"].isin(tipos_selecionados))].copy()

with st.sidebar.expander("📅 Período"):
    d_ini = st.date_input("Início", df_final["data_leitura"].min().date())
    d_fim = st.date_input("Fim", df_final["data_leitura"].max().date())

modo_escala = st.sidebar.radio("Escala", ["Absoluta", "Relativa (T0)"])
df_final = df_final[(df_final["data_leitura"].dt.date >= d_ini) & (df_final["data_leitura"].dt.date <= d_fim)]

if modo_escala == "Relativa (T0)":
    refs = df_final.sort_values("data_leitura").groupby("sensor_id")["valor_sensor"].transform("first")
    df_final["valor_grafico"] = df_final["valor_sensor"] - refs
else:
    df_final["valor_grafico"] = df_final["valor_sensor"]

# ======================================================
# GRÁFICO PRINCIPAL
# ======================================================
st.subheader("📈 Gráfico de Monitoramento")
st.info("💡 Clique em um ponto para excluir.")

fig = go.Figure()
num_devs = len(devices_selecionados)
dev_col_map = {dev: PALETA_DEVICES[i % len(PALETA_DEVICES)] for i, dev in enumerate(devices_selecionados)}

series_list = (df_final["device_name"] + " | " + df_final["tipo_sensor"]).unique()

for serie in series_list:
    d_plot = df_final[(df_final["device_name"] + " | " + df_final["tipo_sensor"]) == serie]
    tipo = d_plot["tipo_sensor"].iloc[0]
    nome_dev = d_plot["device_name"].iloc[0]
    
    eixo_2 = "Temperature" in tipo
    color = dev_col_map[nome_dev] if num_devs > 1 else CORES_SENSOR.get(tipo, "#6b7280")
    
    fig.add_trace(go.Scatter(
        x=d_plot["data_leitura"], 
        y=d_plot["valor_grafico"], 
        name=serie, 
        line=dict(width=2, color=color, dash="dot" if "Air" in tipo else None),
        mode='lines+markers',
        # Armazenamos os nomes reais nos metadados para recuperação segura
        customdata=[[nome_dev, tipo]] * len(d_plot),
        hovertemplate="<b>%{x}</b><br>Valor: %{y}<extra></extra>"
    ))

fig.update_layout(height=650, hovermode="closest", clickmode='event+select', legend=dict(orientation="h", y=-0.2))

selecao = st.plotly_chart(fig, use_container_width=True, on_select="rerun")

# LÓGICA DE EXCLUSÃO CORRIGIDA
if selecao and "selection" in selecao and len(selecao["selection"]["points"]) > 0:
    ponto = selecao["selection"]["points"][0]
    
    # Recuperação segura dos dados via customdata
    # O customdata foi definido como [nome_dev, tipo] no loop do Scatter
    dt_clicada = ponto["x"]
    idx_curva = ponto["curve_number"]
    
    # Buscamos a informação diretamente do objeto da curva clicada
    info_serie = fig.data[idx_curva].customdata[0]
    dev_clicado = info_serie[0]
    tipo_clicado = info_serie[1]
    
    st.warning(f"🗑️ Excluir ponto de **{dt_clicada}** do sensor **{dev_clicado}**?")
    c1, c2 = st.columns(2)
    if c1.button("Confirmar Exclusão"):
        if apagar_leitura_db(dt_clicada, dev_clicado, tipo_clicado):
            st.cache_data.clear()
            st.rerun()
    if c2.button("Cancelar"):
        st.rerun()

# ======================================================
# MAPA E TABELAS (MANTIDOS)
# ======================================================
st.subheader("🛰️ Localização")
df_mapa = df_status[["device_name", "latitude", "longitude", "status", "battery_percentage"]].drop_duplicates().dropna(subset=["latitude", "longitude"])
if not df_mapa.empty:
    df_mapa["label"] = df_mapa.apply(lambda r: f"{r['device_name']} ({int(r['battery_percentage'])}%)" if pd.notnull(r['battery_percentage']) else r['device_name'], axis=1)
    fig_mapa = go.Figure(go.Scattermapbox(
        lat=df_mapa["latitude"], lon=df_mapa["longitude"], mode="markers+text",
        marker=dict(size=12, color=df_mapa["status"].str.lower().map({"online": "#00FF00", "offline": "#FF0000"}).fillna("#888")),
        text=df_mapa["label"], textposition="top right"
    ))
    fig_mapa.update_layout(height=600, margin=dict(l=0,r=0,t=0,b=0), mapbox=dict(accesstoken=MAPBOX_TOKEN, style="satellite-streets", zoom=15, center=dict(lat=df_mapa["latitude"].mean(), lon=df_mapa["longitude"].mean())))
    st.plotly_chart(fig_mapa, use_container_width=True)

with st.expander("📋 Ver Tabela"):
    st.dataframe(df_final[["data_leitura", "device_name", "tipo_sensor", "valor_sensor"]], use_container_width=True)
    st.download_button("📥 CSV", df_final.to_csv(index=False).encode("utf-8"), "dados.csv")
