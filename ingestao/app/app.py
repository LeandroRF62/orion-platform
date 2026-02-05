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

engine = create_engine(DATABASE_URL)

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
    WHERE s.tipo_sensor IN ('A-Axis Delta Angle','B-Axis Delta Angle')
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
# 🎛️ DISPOSITIVO (EXPANDER)
# ======================================================
with st.sidebar.expander("🎛️ Dispositivo", expanded=True):

    tipos_selecionados = st.multiselect(
        "Variável do Dispositivo",
        sorted(df["tipo_sensor"].astype(str).unique()),
        default=sorted(df["tipo_sensor"].astype(str).unique())
    )

    df_tipo = df[df["tipo_sensor"].astype(str).isin(tipos_selecionados)]

    df_devices = df_tipo[["device_name","status"]].drop_duplicates()
    df_devices["status_lower"] = df_devices["status"].astype(str).str.lower()

    df_devices["status_str"] = df_devices["status_lower"].map({
        "online":"🟢 Online",
        "offline":"🔴 Offline"
    }).fillna("⚪ Desconhecido")

    df_devices["label"] = df_devices["device_name"]+" – "+df_devices["status_str"]

    device_label_map = dict(zip(df_devices["label"],df_devices["device_name"]))

    device_principal_label = st.selectbox(
        "Selecionar Dispositivo Principal",
        sorted(device_label_map.keys())
    )

    device_principal = device_label_map[device_principal_label]

    outros_labels = st.multiselect(
        "Adicionar Outros Dispositivos",
        sorted(device_label_map.keys()),
        default=[]
    )

devices_selecionados = list(dict.fromkeys(
    [device_principal]+[device_label_map[l] for l in outros_labels]
))

df_final = df_tipo[df_tipo["device_name"].isin(devices_selecionados)].copy()

# ======================================================
# 📅 PERÍODO (EXPANDER)
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
# 🚨 TARPs (EXPANDER)
# ======================================================
with st.sidebar.expander("🚨 Limites TARP", expanded=False):

    device_id_atual = int(df_final.iloc[-1]["device_id"])

    try:
        limites_existentes = pd.read_sql(
            text("""
                SELECT *
                FROM alert_limits
                WHERE device_id = :device_id
                ORDER BY tipo_sensor ASC, limite_valor ASC
            """),
            engine,
            params={"device_id":device_id_atual}
        )
    except:
        limites_existentes = pd.DataFrame()

    tipos_ordenados = sorted(
        df_final["tipo_sensor"].astype(str).unique(),
        key=lambda x: ("A" not in x, x)
    )

    novo_alerta_tipo = st.selectbox("Tipo de Sensor",tipos_ordenados)
    novo_valor = st.number_input("Valor do Limite",value=0.0,step=0.1)
    mostrar_linha = st.checkbox("Mostrar linha tracejada no gráfico",value=True)
    mensagem_alerta = st.text_input("Mensagem do alerta",value="Ex: Fazer inspeção")

    if st.button("➕ Adicionar Alerta"):
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO alert_limits
                    (device_id,tipo_sensor,limite_valor,mostrar_linha,mensagem)
                    VALUES (:device_id,:tipo,:valor,:mostrar,:mensagem)
                """),
                {
                    "device_id":device_id_atual,
                    "tipo":novo_alerta_tipo,
                    "valor":novo_valor,
                    "mostrar":mostrar_linha,
                    "mensagem":mensagem_alerta
                }
            )
        st.success("Alerta criado!")

# ======================================================
# 👥 CONTATOS (EXPANDER)
# ======================================================
with st.sidebar.expander("👥 Contatos", expanded=False):

    nome_contato = st.text_input("Nome do responsável")
    email_contato = st.text_input("Email")

    telefone_contato = ""
    receber_email = True
    receber_sms = False
    receber_whatsapp = False

    if st.button("➕ Adicionar Contato"):
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO alert_contacts
                    (device_id,nome,email,telefone,receber_email,receber_sms,receber_whatsapp)
                    VALUES (:device_id,:nome,:email,:telefone,:email_ok,:sms_ok,:wpp_ok)
                """),
                {
                    "device_id": device_id_atual,
                    "nome": nome_contato,
                    "email": email_contato,
                    "telefone": telefone_contato,
                    "email_ok": receber_email,
                    "sms_ok": receber_sms,
                    "wpp_ok": receber_whatsapp
                }
            )
        st.success("Contato adicionado!")

# ======================================================
# ⚙️ VISUALIZAÇÃO (EXPANDER)
# ======================================================
with st.sidebar.expander("⚙️ Visualização", expanded=False):

    modo_escala = st.radio(
        "Escala de Visualização",
        ["Absoluta","Relativa"]
    )

if modo_escala=="Relativa":
    refs = (
        df_final.sort_values("data_leitura")
        .groupby("sensor_id")["valor_sensor"]
        .first()
    )
    df_final["valor_grafico"]=df_final["valor_sensor"]-df_final["sensor_id"].map(refs)
else:
    df_final["valor_grafico"]=df_final["valor_sensor"]

# ======================================================
# 🚨 MOTOR TARP
# ======================================================
df_tipo_trigger = df_tipo.copy()

if modo_escala=="Relativa":
    refs_trigger = (
        df_tipo_trigger.sort_values("data_leitura")
        .groupby("sensor_id")["valor_sensor"]
        .first()
    )
    df_tipo_trigger["valor_grafico"] = df_tipo_trigger["valor_sensor"] - df_tipo_trigger["sensor_id"].map(refs_trigger)
else:
    df_tipo_trigger["valor_grafico"] = df_tipo_trigger["valor_sensor"]

ultimo_por_sensor = (
    df_tipo_trigger.sort_values("data_leitura")
    .groupby(["tipo_sensor"])
    .last()
    .reset_index()
)

maior_valor_atual = ultimo_por_sensor["valor_grafico"].abs().max()

limites_tarp = {
    "verde": 0,
    "amarelo": 5,
    "laranja": 10,
    "vermelho": 20
}

nivel_tarp = classificar_tarp(abs(maior_valor_atual), limites_tarp)

emoji_tarp = {
    "Verde": "🟢",
    "Amarelo": "🟡",
    "Laranja": "🟠",
    "Vermelho": "🔴"
}.get(nivel_tarp, "⚪")

# ===============================
# HEADER
# ===============================
info = df_final.sort_values("data_leitura").iloc[-1]

status = str(info["status"]).lower()
bateria = int(info["battery_percentage"]) if pd.notna(info["battery_percentage"]) else 0
ultima_tx = info["last_upload"]

if pd.notna(ultima_tx):
    ultima_tx = (ultima_tx - pd.Timedelta(hours=3)).strftime("%d-%m-%Y %H:%M:%S")

st.markdown(f"""
### {device_principal}
{emoji_tarp} TARP: {nivel_tarp} | 🟢 Status: {status.upper()} | 🔋 {bateria}% | ⏱ Última transmissão: {ultima_tx}
""")

# ===============================
# GRÁFICO
# ===============================
df_final["serie"]=df_final["device_name"].astype(str)+" | "+df_final["tipo_sensor"].astype(str)

fig=px.line(
    df_final,
    x="data_leitura",
    y="valor_grafico",
    color="serie",
    template="plotly_white"
)

if not limites_existentes.empty:
    for _,alerta in limites_existentes.iterrows():
        if alerta["mostrar_linha"]:
            fig.add_hline(
                y=alerta["limite_valor"],
                line_dash="dash",
                annotation_text=alerta["mensagem"],
                annotation_position="top left"
            )

fig.update_layout(
    height=780,
    legend=dict(
        orientation="h",
        y=-0.15,
        x=0.5,
        xanchor="center"
    )
)

st.plotly_chart(fig,use_container_width=True)
