import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine

# ===============================
# CONFIGURAÇÃO
# ===============================
st.set_page_config(
    page_title="Orion Platform",
    layout="wide"
)

st.title("📊 Orion Platform – Monitoramento")

# ===============================
# CONEXÃO COM O BANCO
# ===============================
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("DATABASE_URL não encontrada.")
    st.stop()

engine = create_engine(DATABASE_URL)

# ===============================
# CONSULTA DE TESTE
# ===============================
query = """
SELECT
    sensor_id,
    data_leitura,
    valor_sensor
FROM leituras
ORDER BY data_leitura DESC
LIMIT 100;
"""

try:
    df = pd.read_sql(query, engine)
except Exception as e:
    st.error("Erro ao conectar no banco")
    st.exception(e)
    st.stop()

# ===============================
# EXIBIÇÃO
# ===============================
st.success("✅ Conectado ao Supabase com sucesso")
st.write("Últimas 100 leituras:")
st.dataframe(df)
