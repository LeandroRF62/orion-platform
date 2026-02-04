import os
import requests
import psycopg2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.oriondata.io/api"
REQUEST_TIMEOUT = 30

def get_api_key():
    return os.getenv("API_KEY")

def get_db_conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def get_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def obter_token(session):
    r = session.get(
        f"{BASE_URL}/token",
        params={"apiKey": get_api_key()},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    return r.json()["token"]
