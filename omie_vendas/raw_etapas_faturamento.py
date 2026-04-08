import requests
import pandas as pd
import time
import os
import pathlib
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account

# Configurações da API Omie
BASE_URL = "https://app.omie.com.br/api/v1/produtos/etapafat/"

APP_KEY = "1733209266789"  # Substitua pelo seu App Key
APP_SECRET = "14c2f271c839dfea25cfff4afb63b331"  # Substitua pelo seu App Secret
HEADERS = {"Content-Type": "application/json"}

# Configurações do Google BigQuery


def _resolve_gcp_credentials_path() -> str:
    return (
        os.getenv("GCS_CREDENTIALS_JSON_PATH")
        or os.getenv("MACHTOOLS_JSON_PATH")
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or "machtools.json"
    )


GCP_CREDENTIALS_PATH = _resolve_gcp_credentials_path()
PROJECT_ID = "machtools"  # Substitua pelo ID do seu projeto no Google Cloud
BUCKET_NAME = "raw_machtools"  # Nome do bucket no GCS
DESTINATION_FOLDER = "etapas_faturamento/"  # Pasta dentro do bucket

# Configura o ambiente com as credenciais do Google Cloud
credentials = service_account.Credentials.from_service_account_file(GCP_CREDENTIALS_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Função para chamar a API Omie
def fetch_data(page, retry=3):
    payload = {
        "call": "ListarEtapasFaturamento",
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "param": [
            {
                "pagina": page,
                "registros_por_pagina": 100  # Ajuste conforme necessário

            }
        ]
    }

    for attempt in range(retry):
        try:
            # print(f"Enviando payload: {payload}")  # Log do payload
            response = requests.post(BASE_URL, headers=HEADERS, json=payload)
            # print(f"Resposta da API: {response.status_code}, {response.text}")  # Log da resposta
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

# Função para processar os dados e criar um DataFrame
def process_data():
    all_records = []
    page = 1
    total_requests = 0  # Contador de requisições
    max_requests = 200  # Limite de requisições para teste

    while True:
        try:
            print(f"Buscando dados da página {page}...")
            data = fetch_data(page)
            # print(data)

            nr_pagina = data.get("pagina", 0)
            total_de_paginas = data.get("total_de_paginas", 0)
            registros = data.get("registros", 0)
            total_de_registros = data.get("registros", 0)
            
            # Verifica se a resposta contém pedidos
            if "cadastros" not in data or not data["cadastros"]:
                print("Nenhum dado adicional encontrado. Finalizando.")
                break

            # Se a resposta for um único pedido, transforma em uma lista
            if isinstance(data["cadastros"], dict):
                data["cadastros"] = [data["cadastros"]]

            # Adiciona os pedidos à lista de registros
            all_records.extend(data["cadastros"])
            total_requests += 1  # Incrementa o contador de requisições

            if total_requests >= max_requests:
                print("Limite de 200 requisições atingido. Finalizando.")
                break

            if page < total_de_paginas:
                page += 1
            else:
                break
        except Exception as e:
            print(f"Erro ao buscar dados: {e}")
            break

    if not all_records:
        raise Exception("Nenhum dado foi retornado pela API.")

    # Criação do DataFrame
    df = pd.json_normalize(all_records)

    df_final = df
    

    # Expandir campos aninhados det
    df_exploded = df.explode("etapas", ignore_index=True)
    itens_normalize = pd.json_normalize(df_exploded["etapas"])
    df_final = pd.concat([df_exploded.drop(columns=["etapas"]), itens_normalize], axis=1)

    # Remover as linhas onde a coluna 'cCodOperacao' é NaN
    df_final = df_final.dropna(subset=['cCodOperacao'])

    # print(df_final)
        
    # Renomeação de colunas (ajuste conforme a estrutura da API)
    rename_columns = {
        "cCodOperacao" : "cod_operacao",
        "cDescOperacao" : "de_operacao",
        "cCodigo" : "cod_etapa_faturamento",
        "cDescrPadrao" : "de_padrao",
        "cDescricao" : "de_etapa_faturamento",
        "cInativo": "fl_inativo"
    }

    df_final=  df_final.rename(columns=rename_columns)




    return df_final

def save_to_gcs(df):
    try:
        # Inicializa o cliente do GCS
        storage_client = storage.Client(credentials=credentials, project=PROJECT_ID)

        # Cria o caminho completo do arquivo no GCS
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        destination_blob_name = f"{DESTINATION_FOLDER}etapa_faturamento.csv"

        # Converte o DataFrame para CSV em memória
        csv_data = df.to_csv(index=False, encoding='windows-1252', sep=';')

        # Carrega o CSV para o Google Cloud Storage
        bucket = storage_client.get_bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(csv_data, content_type='text/csv')

        print(f"Dados enviados para o GCS: gs://{BUCKET_NAME}/{destination_blob_name}")

    except Exception as e:
        print(f"Erro ao enviar dados para o GCS: {e}")


# Executa o script
if __name__ == "__main__":
    try:
        # Processar os dados
        df = process_data()

        # Enviar os dados para o BigQuery
        # save_to_gcs(df)

    except Exception as e:
        print(f"Erro ao processar e salvar os dados: {e}")


