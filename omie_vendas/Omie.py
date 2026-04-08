# import requests
# import pandas as pd
# import time
# import os
# from google.cloud import storage

# # Configurações da API
# BASE_URL = "https://app.omie.com.br/api/v1/produtos/nfconsultar/"
# APP_KEY = "1733209266789"
# APP_SECRET = "14c2f271c839dfea25cfff4afb63b331"
# HEADERS = {"Content-Type": "application/json"}

# # Configuração do Google Cloud Storage
# GCS_BUCKET_NAME = "maq_vendas"
# GCS_DESTINATION_BLOB_NAME = "row_vendas/dados_nf.csv"
# GCS_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "machtools.json")

# # Configura o ambiente com as credenciais do GCS
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_CREDENTIALS_PATH

# # Função para chamar a API com controle de limite de requisições
# def fetch_data(page, retry=3):
#     payload = {
#         "call": "ListarNF",
#         "app_key": APP_KEY,
#         "app_secret": APP_SECRET,
#         "param": [
#             {
#                 "pagina": page,
#                 "registros_por_pagina": 200,
#                 "apenas_importado_api": "N"
#             }
#         ]
#     }

#     for attempt in range(retry):
#         try:
#             response = requests.post(BASE_URL, headers=HEADERS, json=payload)
#             if response.status_code == 429:
#                 print("Limite de requisições atingido. Aguardando 60 segundos...")
#                 time.sleep(60)
#                 continue
#             response.raise_for_status()
#             return response.json()
#         except requests.exceptions.RequestException as e:
#             print(f"Tentativa {attempt + 1}/{retry} falhou. Erro: {e}")
#             time.sleep(10)
#     raise Exception("Falha ao chamar a API após várias tentativas.")

# # Função para processar os dados e criar um DataFrame estruturado
# def process_data():
#     all_records = []
#     page = 1

#     while True:
#         try:
#             print(f"Buscando dados da página {page}...")
#             data = fetch_data(page)
#             if "nfCadastro" not in data or not data["nfCadastro"]:
#                 print("Nenhum dado adicional encontrado. Finalizando.")
#                 break
#             all_records.extend(data["nfCadastro"])
#             page += 1
#         except Exception as e:
#             print(f"Erro ao buscar dados: {e}")
#             break

#     if not all_records:
#         raise Exception("Nenhum dado foi retornado pela API.")

#     # Criação do DataFrame
#     df = pd.json_normalize(all_records)

#     # Renomeação de colunas
#     rename_columns = {
#         "compl.cCodCateg": "categoria_codigo",
#         "compl.cModFrete": "frete_codigo",
#         "compl.nIdNF": "nf_id",
#         "compl.nIdPedido": "pedido_id",
#         "compl.nIdReceb": "recebimento_id",
#         "compl.nIdTransp": "transportadora_id",
#         "det.nCodItem": "item_codigo",
#         "det.nCodProd": "prod_codigo",
#         "prod.CFOP": "cfop",
#     }
#     df = df.rename(columns=rename_columns)

#     # Expandir campos aninhados
#     df_exploded = df.explode("det", ignore_index=True)
#     detalhe_normalize = pd.json_normalize(df_exploded["det"])
#     df_final = pd.concat([df_exploded.drop(columns=["det"]), detalhe_normalize], axis=1)

#     df_exploded = df_final.explode("titulos", ignore_index=True)
#     titulos_normalize = pd.json_normalize(df_exploded["titulos"])
#     df_expanded = pd.concat([df_exploded.drop(columns=["titulos"]), titulos_normalize], axis=1)

#     df = df_expanded

#     return df

# # Função para enviar diretamente para o GCS
# def save_to_gcs():
#     try:
#         # Processar os dados
#         df = process_data()

#         # Salvar diretamente no GCS
#         storage_client = storage.Client()
#         bucket = storage_client.bucket(GCS_BUCKET_NAME)
#         blob = bucket.blob(GCS_DESTINATION_BLOB_NAME)

#         # Converte o DataFrame para CSV no formato de bytes
#         csv_data = df.to_csv(index=False, encoding="utf-8", sep=";")
#         blob.upload_from_string(csv_data, content_type="text/csv")

#         print(f"Arquivo enviado para o GCS: gs://{GCS_BUCKET_NAME}/{GCS_DESTINATION_BLOB_NAME}")

#     except Exception as e:
#         print(f"Erro ao processar e salvar os dados: {e}")

# # Executa o script
# if __name__ == "__main__":
#     save_to_gcs()

import requests
import pandas as pd
import time
import os
from google.cloud import storage

# Configurações da API
BASE_URL = "https://app.omie.com.br/api/v1/produtos/nfconsultar/"
APP_KEY = "1733209266789"
APP_SECRET = "14c2f271c839dfea25cfff4afb63b331"
HEADERS = {"Content-Type": "application/json"}

# Configuração do Google Cloud Storage
GCS_BUCKET_NAME = "maq_vendas"
GCS_DESTINATION_BLOB_NAME = "row_vendas/dados_nf.csv"


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

# Função para chamar a API com controle de limite de requisições
def fetch_data(page, retry=3):
    payload = {
        "call": "ListarNF",
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "param": [
            {
                "pagina": page,
                "registros_por_pagina": 200,
                "apenas_importado_api": "N",
                # Inclua aqui os campos necessários para cidade e estado, se houver
            }
        ]
    }

    for attempt in range(retry):
        try:
            response = requests.post(BASE_URL, headers=HEADERS, json=payload)
            if response.status_code == 429:
                print("Limite de requisições atingido. Aguardando 60 segundos...")
                time.sleep(60)
                continue
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Tentativa {attempt + 1}/{retry} falhou. Erro: {e}")
            time.sleep(10)
    raise Exception("Falha ao chamar a API após várias tentativas.")

# Função para processar os dados e criar um DataFrame estruturado
def process_data():
    all_records = []
    page = 1

    while True:
        try:
            print(f"Buscando dados da página {page}...")
            data = fetch_data(page)
            if "nfCadastro" not in data or not data["nfCadastro"]:
                print("Nenhum dado adicional encontrado. Finalizando.")
                break
            all_records.extend(data["nfCadastro"])
            print(f"Total de registros coletados até agora: {len(all_records)}")
            page += 1
        except Exception as e:
            print(f"Erro ao buscar dados: {e}")
            break

    if not all_records:
        raise Exception("Nenhum dado foi retornado pela API.")

    print(f"Processando {len(all_records)} registros...")

    # Criação do DataFrame
    df = pd.json_normalize(all_records)
    print(f"DataFrame inicial criado com {len(df)} linhas e {len(df.columns)} colunas")

    # Renomeação de colunas
    rename_columns = {
        "compl.cCodCateg": "categoria_codigo",
        "compl.cModFrete": "frete_codigo",
        "compl.nIdNF": "nf_id",
        "compl.nIdPedido": "pedido_id",
        "compl.nIdReceb": "recebimento_id",
        "compl.nIdTransp": "transportadora_id",
        "det.nCodItem": "item_codigo",
        "det.nCodProd": "prod_codigo",
        "prod.CFOP": "cfop",
        # Adicione aqui os campos de cidade e estado, se existirem
        "endereco.cidade": "cidade",
        "endereco.estado": "estado",
    }
    df = df.rename(columns=rename_columns)

    # Expandir campos aninhados - detalhes dos produtos
    df_exploded = df.explode("det", ignore_index=True)
    print(f"Após expandir detalhes: {len(df_exploded)} linhas")
    
    # Criar índice sequencial para cada produto por pedido
    df_exploded['nr_item'] = df_exploded.groupby('pedido_id').cumcount() + 1
    print(f"Índice nr_item criado. Exemplo de valores: {df_exploded[['pedido_id', 'nr_item']].head(10).to_dict('records')}")
    
    # Normalizar os detalhes dos produtos
    detalhe_normalize = pd.json_normalize(df_exploded["det"].tolist())
    df_final = pd.concat([df_exploded.drop(columns=["det"]), detalhe_normalize], axis=1)
    print(f"Após normalizar detalhes: {len(df_final)} linhas e {len(df_final.columns)} colunas")

    # Expandir campos aninhados - títulos
    df_exploded = df_final.explode("titulos", ignore_index=True)
    titulos_normalize = pd.json_normalize(df_exploded["titulos"].tolist())
    df_expanded = pd.concat([df_exploded.drop(columns=["titulos"]), titulos_normalize], axis=1)
    print(f"DataFrame final: {len(df_expanded)} linhas e {len(df_expanded.columns)} colunas")

    df = df_expanded

    return df

# Função para enviar diretamente para o GCS
def save_to_gcs():
    try:
        # Processar os dados
        df = process_data()

        # Salvar diretamente no GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(GCS_DESTINATION_BLOB_NAME)

        # Converte o DataFrame para CSV no formato de bytes
        csv_data = df.to_csv(index=False, encoding="utf-8", sep=";")
        blob.upload_from_string(csv_data, content_type="text/csv")

        print(f"Arquivo enviado para o GCS: gs://{GCS_BUCKET_NAME}/{GCS_DESTINATION_BLOB_NAME}")

    except Exception as e:
        print(f"Erro ao processar e salvar os dados: {e}")

# Executa o script
if __name__ == "__main__":
    save_to_gcs()