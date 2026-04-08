import requests
import pandas as pd
import os
from google.cloud import storage
import time
from configuracoes.setup_logging import get_logger

# Configurar logging
logger = get_logger("Omie_vendedores.py")

# Configurações da API
BASE_URL_GERAL = "https://app.omie.com.br/api/v1/geral/vendedores/"
BASE_URL_CRM = "https://app.omie.com.br/api/v1/crm/usuarios/"
APP_KEY = "1733209266789"
APP_SECRET = "14c2f271c839dfea25cfff4afb63b331"
HEADERS = {"Content-Type": "application/json"}

# Configuração do Google Cloud Storage
GCS_BUCKET_NAME = "maq_vendas"
GCS_DESTINATION_BLOB_NAME = "dim_vendedores/dimensao_vendedores.csv"


def _resolve_gcp_credentials_path() -> str:
    return (
        os.getenv("GCS_CREDENTIALS_JSON_PATH")
        or os.getenv("MACHTOOLS_JSON_PATH")
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or "machtools.json"
    )


GCS_CREDENTIALS_PATH = _resolve_gcp_credentials_path()

# Configura o ambiente com as credenciais do GCS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_CREDENTIALS_PATH

# Função para chamar a API com controle de paginação - VENDEDORES NFE
def fetch_vendedores(page, retry=3):
    payload = {
        "call": "ListarVendedores",
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "param": [{"pagina": page, "registros_por_pagina": 100, "apenas_importado_api": "N"}]
    }

    for attempt in range(retry):
        try:
            response = requests.post(BASE_URL_GERAL, headers=HEADERS, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Tentativa {attempt + 1}/{retry} falhou. Erro: {e}")
            time.sleep(10)
    raise Exception("Falha ao chamar a API após várias tentativas.")

# Função para chamar a API com controle de paginação - VENDEDORES CRM
def fetch_vendedores_crm(page, retry=3):
    payload = {
        "call": "ListarUsuarios",
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "param": [{"pagina": page, "registros_por_pagina": 20}]
    }

    for attempt in range(retry):
        try:
            response = requests.post(BASE_URL_CRM, headers=HEADERS, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Tentativa {attempt + 1}/{retry} falhou. Erro: {e}")
            time.sleep(10)
    raise Exception("Falha ao chamar a API após várias tentativas.")

# Função para processar os dados dos vendedores
def process_vendedores():
    all_records = []
    page = 1

    while True:
        print(f"Buscando dados dos vendedores - Página {page}...")
        data = fetch_vendedores(page)
        if "cadastro" not in data or not data["cadastro"]:
            print("Nenhum dado adicional encontrado. Finalizando.")
            break
        all_records.extend(data["cadastro"])
        if page >= data["total_de_paginas"]:
            break
        page += 1

    if not all_records:
        raise Exception("Nenhum dado foi retornado pela API.")

    # Criação do DataFrame
    df = pd.DataFrame(all_records)

    # Selecionar colunas relevantes
    df_vendedor_venda = df[["codigo", "nome", "email", "comissao", "inativo", "visualiza_pedido"]]

    ## INCLUI COLUNA IDENTICA O SISTEMA
    df_vendedor_venda.loc[:, "tp_cadastro_vendedor"] = "VENDAS"

    # Renomear colunas para melhor entendimento
    df_vendedor_venda = df_vendedor_venda.rename(columns={
        "codigo": "vendedor_codigo",
        "nome": "vendedor_nome",
        "email": "vendedor_email",
        "comissao": "vendedor_comissao",
        "inativo": "vendedor_inativo",
        "visualiza_pedido": "vendedor_visualiza_pedido"
    })

    ########################################################
    ## OBTER VENDEDORES (USUÁRIOS) CRM
    all_records = []
    page = 1

    while True:
        print(f"Buscando dados dos vendedores - Página {page}...")
        data = fetch_vendedores_crm(page)
        if "cadastros" not in data or not data["cadastros"]:
            print("Nenhum dado adicional encontrado. Finalizando.")
            break
        all_records.extend(data["cadastros"])
        if page >= data["total_de_paginas"]:
            break
        page += 1

    if not all_records:
        raise Exception("Nenhum dado foi retornado pela API.")

    # Criação do DataFrame
    df = pd.DataFrame(all_records)

    # Selecionar colunas relevantes
    df_vendedor_crm = df[["nCodigo", "cNome", "cEmail"]]

    ## INCLUI COLUNA IDENTICA O SISTEMA
    df_vendedor_crm.loc[:, "vendedor_inativo"] = "N"
    df_vendedor_crm.loc[:, "vendedor_visualiza_pedido"] = "N"
    df_vendedor_crm.loc[:, "tp_cadastro_vendedor"] = "CRM"

    # Renomear colunas para melhor entendimento
    df_vendedor_crm = df_vendedor_crm.rename(columns={
        "nCodigo": "vendedor_codigo",
        "cNome": "vendedor_nome",
        "cEmail": "vendedor_email",
    })

    df = pd.concat([df_vendedor_venda, df_vendedor_crm]).reset_index(drop=True)

    return df

# Função para salvar diretamente no GCS
def save_to_gcs():
    try:
        # Processar os dados dos vendedores
        df = process_vendedores()

        # Salvar o DataFrame em um arquivo temporário
        temp_csv_path = "temp_vendedores.csv"
        df.to_csv(temp_csv_path, index=False, encoding="utf-8", sep=";")
        print("Dados dos vendedores salvos temporariamente.")

        # Enviar o arquivo para o Google Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(GCS_DESTINATION_BLOB_NAME)
        blob.upload_from_filename(temp_csv_path)
        print(f"Arquivo enviado para o GCS: gs://{GCS_BUCKET_NAME}/{GCS_DESTINATION_BLOB_NAME}")

        # Remover o arquivo temporário após o upload
        os.remove(temp_csv_path)

    except Exception as e:
        print(f"Erro ao processar e salvar os dados: {e}")

# Executa o script
if __name__ == "__main__":
    save_to_gcs()
