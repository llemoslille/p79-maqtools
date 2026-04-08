import os
from pathlib import Path

import requests
from dotenv import load_dotenv
from msal import ConfidentialClientApplication
from logger_config import obter_logger

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Configurar logging
logger = obter_logger("atualiza_dataset.py")

TENANT_ID = (os.environ.get("POWERBI_TENANT_ID") or "").strip()
CLIENT_ID = (os.environ.get("POWERBI_CLIENT_ID") or "").strip()
CLIENT_SECRET = (os.environ.get("POWERBI_CLIENT_SECRET") or "").strip()
GROUP_ID = (os.environ.get("POWERBI_GROUP_ID") or "").strip()
DATASET_ID_REPOSICAO = (os.environ.get("DATASET_ID_REPOSICAO") or "").strip()
DATASET_ID_REPOSICAO_DE = (os.environ.get("DATASET_ID_REPOSICAO_DE") or "").strip()

MISSING = [n for n, v in [
                            ("POWERBI_TENANT_ID", TENANT_ID),
                            ("POWERBI_CLIENT_ID", CLIENT_ID),
                            ("POWERBI_CLIENT_SECRET", CLIENT_SECRET),
                            ("POWERBI_GROUP_ID", GROUP_ID),
                            ("DATASET_ID_REPOSICAO", DATASET_ID_REPOSICAO),
                        ] if not v]
if MISSING:
    logger.warning(
        "Power BI: variaveis ausentes (%s). Atualizacao de datasets ignorada; defina no .env (raiz do repo) se precisar refrescar.",
        ", ".join(MISSING),
    )
    raise SystemExit(0)

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
DATASET_ID = {"001": DATASET_ID_REPOSICAO}
if DATASET_ID_REPOSICAO_DE:
    DATASET_ID["002"] = DATASET_ID_REPOSICAO_DE

# Autenticação
app = ConfidentialClientApplication(
    CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
)
token_response = app.acquire_token_for_client(scopes=SCOPE)
# print(token_response)
access_token = token_response['access_token']

# Headers para as requisições
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

# Atualizar cada dataset
logger.info("Iniciando atualização dos datasets do Power BI")
for dataset_key, dataset_id in DATASET_ID.items():
    logger.info(f'Atualizando Dataset {dataset_key} (ID: {dataset_id})...')

    # Chamada para atualizar o dataset
    refresh_url = f'https://api.powerbi.com/v1.0/myorg/groups/{GROUP_ID}/datasets/{dataset_id}/refreshes'
    response = requests.post(refresh_url, headers=headers)

    if response.status_code == 202:
        logger.info(
            f'Dataset {dataset_key} - Atualização iniciada com sucesso!')
    else:
        logger.error(
            f'[ERRO] Dataset {dataset_key} - Erro ao iniciar atualização: {response.status_code}')
        logger.error(f'Resposta: {response.text}')

logger.info('Processo de atualização dos datasets concluído!')
