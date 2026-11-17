"""Funções compartilhadas pelos scripts de ingestão."""

import os

import requests
import psycopg2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.oriondata.io/api"
REQUEST_TIMEOUT = 45


def get_api_key():
    chave = os.getenv("API_KEY")
    if not chave:
        raise SystemExit("API_KEY não definida (secret do GitHub).")
    return chave


def get_db_conn():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL não definida (secret do GitHub).")
    return psycopg2.connect(url)


def get_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def obter_token(session):
    r = session.get(
        f"{BASE_URL}/token",
        params={"apiKey": get_api_key()},
        timeout=REQUEST_TIMEOUT,
    )
    if r.status_code in (401, 403):
        raise SystemExit(
            f"❌ A Orion recusou a API_KEY ({r.status_code}): {r.text[:200]}")
    r.raise_for_status()
    return r.json()["token"]
