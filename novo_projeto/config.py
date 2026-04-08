from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


# Carrega .env da raiz do repositório (best-effort)
_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env")


def _getenv_first(*keys: str, default: str | None = None) -> str | None:
    for k in keys:
        v = os.environ.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


# Omie credentials
APP_KEY = _getenv_first("APP_KEY", "OMIE_APP_KEY")
APP_SECRET = _getenv_first("APP_SECRET", "OMIE_APP_SECRET")


# GCS / GCP configs (aceita aliases)
GCS_BUCKET_NAME = _getenv_first("GCS_BUCKET_NAME", "GCS_BUCKET")
GCS_PROJECT_ID = _getenv_first("GCS_PROJECT_ID", "GOOGLE_CLOUD_PROJECT", "GCP_PROJECT")

# Caminho para credenciais JSON (aceita aliases e resolve relativo à raiz se necessário)
_cred = _getenv_first(
    "GCS_CREDENTIALS_PATH",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GCP_CREDENTIALS_PATH",
)
if _cred:
    p = Path(_cred)
    if not p.is_absolute():
        p = (_ROOT / p).resolve()
    GCS_CREDENTIALS_PATH = str(p)
else:
    GCS_CREDENTIALS_PATH = None

