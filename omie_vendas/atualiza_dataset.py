import os
from pathlib import Path

import requests
from dotenv import load_dotenv
from msal import ConfidentialClientApplication

from configuracoes.setup_logging import get_logger

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Configurar logging
logger = get_logger("atualiza_dataset.py")

# Azure AD / Power BI (obrigatório no .env — nunca commitar segredos)
TENANT_ID = (os.environ.get("POWERBI_TENANT_ID") or "").strip()
CLIENT_ID = (os.environ.get("POWERBI_CLIENT_ID") or "").strip()
CLIENT_SECRET = (os.environ.get("POWERBI_CLIENT_SECRET") or "").strip()
GROUP_ID = (os.environ.get("POWERBI_GROUP_ID") or "").strip()

MISSING = [n for n, v in [
    ("POWERBI_TENANT_ID", TENANT_ID),
    ("POWERBI_CLIENT_ID", CLIENT_ID),
    ("POWERBI_CLIENT_SECRET", CLIENT_SECRET),
    ("POWERBI_GROUP_ID", GROUP_ID),
] if not v]
if MISSING:
    raise RuntimeError(
        "Defina no .env (raiz do repo): "
        + ", ".join(MISSING)
    )

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]

# IDs dos datasets (não segredo; ajuste se necessário)
DATASET_ID = {
    '001': '9fb0684f-1d19-46b8-91ed-fa3464b1f2bb',  # Projeto Vendas
    '002': '759f069b-cced-4f1d-929f-853ae8d6e205',  # Projeto Vendedor
    '003': 'be28dab0-291d-4b05-bce5-44d0f6bd7f5e',  # Projeto Vendas - Desenvolvimento
    '004': '1becf7e8-2a24-46d6-8a3e-83fb295b0bf6',  # Projeto Vendas - DEV
    '005': '7719c309-248f-4205-b7d3-5e8cbd243cc3',  # Projeto Orçamento Perdidos
    # '006': '1becf7e8-2a24-46d6-8a3e-83fb295b0bf6',  # Maqtools Vendas (Dev)
}

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
        logger.success(
            f'Dataset {dataset_key} - Atualização iniciada com sucesso!')
    else:
        logger.error(
            f'Dataset {dataset_key} - Erro ao iniciar atualização: {response.status_code}')
        logger.error(f'Resposta: {response.text}')

logger.success('Processo de atualização dos datasets concluído!')
