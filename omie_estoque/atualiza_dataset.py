import os
from pathlib import Path

import requests
from dotenv import load_dotenv
from msal import ConfidentialClientApplication

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

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
    print(
        "[AVISO] Power BI: variaveis ausentes ("
        + ", ".join(MISSING)
        + "). Atualizacao de datasets ignorada; defina no .env (raiz do repo) se precisar refrescar."
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
for dataset_key, dataset_id in DATASET_ID.items():
    print(f'\nAtualizando Dataset {dataset_key} (ID: {dataset_id})...')

    # Chamada para atualizar o dataset
    refresh_url = f'https://api.powerbi.com/v1.0/myorg/groups/{GROUP_ID}/datasets/{dataset_id}/refreshes'
    response = requests.post(refresh_url, headers=headers)

    if response.status_code == 202:
        print(
            f'[OK] Dataset {dataset_key} - Atualização iniciada com sucesso!')
    else:
        print(
            f'[ERRO] Dataset {dataset_key} - Erro ao iniciar atualização: {response.status_code}')
        print(f'   Resposta: {response.text}')

print('\n[SUCESSO] Processo de atualização concluído!')
