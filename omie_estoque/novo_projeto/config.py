import json
import os
from pathlib import Path

import yaml

_ESTOQUE_DIR = Path(__file__).resolve().parent.parent
_REPO_ROOT = _ESTOQUE_DIR.parent

try:
    from dotenv import load_dotenv

    load_dotenv(_REPO_ROOT / ".env")
except ImportError:
    pass


def _strip_path_env(raw: str) -> str:
    s = (raw or "").strip().strip('"').strip("'")
    if s.startswith("\ufeff"):
        s = s.lstrip("\ufeff")
    return s


def _resolve_service_account_json(spec: str) -> Path | None:
    """
    Localiza o JSON da service account. Ordem (caminho relativo / só nome do arquivo):
    raiz do repo, pasta omie_estoque, cwd do processo (útil: subprocess com cwd=omie_estoque).
    """
    spec = _strip_path_env(spec)
    if not spec:
        return None
    p = Path(spec).expanduser()
    if p.is_absolute():
        return p.resolve() if p.is_file() else None
    name = p.name
    bases = [_REPO_ROOT, _ESTOQUE_DIR, Path.cwd()]
    if os.name == "nt":
        up = _strip_path_env(os.environ.get("USERPROFILE", ""))
        if up:
            bases.append(Path(up))
    for base in bases:
        for cand in (base / spec, base / name):
            try:
                if cand.is_file():
                    return cand.resolve()
            except OSError:
                continue
    return None


# GOOGLE_APPLICATION_CREDENTIALS no .env (só nome ou caminho relativo)
_gac = _strip_path_env(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""))
if _gac and not os.environ.get("GCS_CREDENTIALS_JSON_PATH") and not os.environ.get(
    "MACHTOOLS_JSON_PATH"
):
    _found_gac = _resolve_service_account_json(_gac)
    if _found_gac:
        os.environ["GCS_CREDENTIALS_JSON_PATH"] = str(_found_gac)

# Configurações da API do OMIE
APP_KEY = "1733209266789"
APP_SECRET = "14c2f271c839dfea25cfff4afb63b331"


def _cred_path_from_env_var(name: str) -> Path | None:
    raw = _strip_path_env(os.environ.get(name, ""))
    if not raw:
        return None
    p = Path(raw).expanduser()
    if p.is_absolute() and p.is_file():
        return p.resolve()
    return _resolve_service_account_json(raw)


_cred_path = (
    _cred_path_from_env_var("GCS_CREDENTIALS_JSON_PATH")
    or _cred_path_from_env_var("MACHTOOLS_JSON_PATH")
    or _resolve_service_account_json("machtools.json")
    or (_ESTOQUE_DIR / "machtools.json")
)
if not _cred_path.is_file():
    raise FileNotFoundError(
        f"Credenciais GCS não encontradas (último candidato: {_cred_path}). "
        "Opções: (1) omie_estoque/machtools.json com o JSON real da GCP; "
        "(2) GCS_CREDENTIALS_JSON_PATH ou MACHTOOLS_JSON_PATH com caminho absoluto; "
        "(3) na raiz do repo ou em omie_estoque, coloque o .json e defina "
        "GOOGLE_APPLICATION_CREDENTIALS=nome_do_arquivo.json no .env — "
        "o subprocess usa cwd=omie_estoque, então o arquivo pode ficar em omie_estoque/."
    )

with open(_cred_path, "r", encoding="utf-8") as f:
    machtools_config = json.load(f)

_yaml_path = _ESTOQUE_DIR / "config.yaml"
with open(_yaml_path, "r", encoding="utf-8") as f:
    yaml_config = yaml.safe_load(f)

GCS_PROJECT_ID = machtools_config["project_id"]
GCS_BUCKET_NAME = yaml_config["bucket-projeto"]
GCS_CREDENTIALS_PATH = str(_cred_path.resolve())
