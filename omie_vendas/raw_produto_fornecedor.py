import requests
import pandas as pd
import time
import os
import pathlib
import re
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from configuracoes.setup_logging import get_logger

# Configurar logging
logger = get_logger("raw_produto_fornecedor.py")


# Configurações da API Omie
BASE_URL = "https://app.omie.com.br/api/v1/estoque/produtofornecedor/"

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
DESTINATION_FOLDER = "fornecedor_produto/"  # Pasta dentro do bucket

# Configura o ambiente com as credenciais do Google Cloud
credentials = service_account.Credentials.from_service_account_file(
    GCP_CREDENTIALS_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)


# Função para limpar nome da coluna
def limpar_nome_coluna(nome):
    """
    Limpa e padroniza o nome da coluna.
    - Substitui pontos por underscores
    - Converte camelCase para snake_case
    - Remove caracteres especiais
    - Remove underscores duplicados
    """
    # Substitui pontos por underscores
    nome = nome.replace('.', '_')

    # Converte camelCase para snake_case
    nome = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', nome)

    # Remove caracteres especiais e espaços
    nome = ''.join(c for c in nome if c.isalnum() or c == '_')

    # Remove underscores duplicados
    while '__' in nome:
        nome = nome.replace('__', '_')

    # Remove underscores no início e fim
    nome = nome.strip('_')

    # Converte para minúsculas
    nome = nome.lower()

    return nome


# FUNÇÃO PARA NORMALIZAR COLUNAS
def normalizar_coluna(df, coluna):
    """
    Normaliza uma coluna específica que contém dicionários ou listas em um DataFrame.

    Args:
        df (pandas.DataFrame): DataFrame contendo a coluna a ser normalizada
        coluna (str): Nome da coluna a ser normalizada

    Returns:
        pandas.DataFrame: DataFrame com a coluna normalizada
    """
    try:
        logger.info(
            f"Normalizando coluna: '{coluna}', tipo: '{df[coluna].dtype}'")

        # Função para extrair o primeiro item de uma lista ou retornar o próprio valor
        def extrair_primeiro(valor):
            if isinstance(valor, list) and len(valor) > 0:
                return valor[0]
            if isinstance(valor, dict):
                return valor
            return {}

        # Normaliza a coluna
        valores_normalizados = df[coluna].apply(extrair_primeiro)

        # Cria um DataFrame com as colunas normalizadas
        colunas_normalizadas = {}
        primeiro_valor = valores_normalizados.iloc[0]

        if isinstance(primeiro_valor, dict):
            for chave, valor in primeiro_valor.items():
                nome_coluna = f'{coluna}_{chave}'

                # Se o valor for um dicionário, normaliza os campos aninhados
                if isinstance(valor, dict):
                    for sub_chave, sub_valor in valor.items():
                        sub_nome_coluna = f'{nome_coluna}_{sub_chave}'
                        colunas_normalizadas[sub_nome_coluna] = valores_normalizados.apply(
                            lambda x: x.get(chave, {}).get(
                                sub_chave) if isinstance(x, dict) else None
                        )
                else:
                    colunas_normalizadas[nome_coluna] = valores_normalizados.apply(
                        lambda x: x.get(chave) if isinstance(x, dict) else None
                    )

        df_normalizado = pd.DataFrame(colunas_normalizadas)

        # Limpa os nomes das colunas
        df_normalizado.columns = [limpar_nome_coluna(
            col) for col in df_normalizado.columns]

        # Concatena os DataFrames
        df_final = pd.concat([
            df.drop(columns=[coluna]).reset_index(drop=True),
            df_normalizado.reset_index(drop=True)
        ], axis=1)

        return df_final

    except Exception as e:
        logger.error(f"Erro ao normalizar coluna {coluna}: {str(e)}")
        print(f"Erro detalhado: {type(e).__name__}: {str(e)}")
        return df


# Função para normalizar colunas que contêm dicionários
def normalizar_colunas_dict(df):
    """
    Normaliza todas as colunas que contêm dicionários em um DataFrame.

    Args:
        df (pandas.DataFrame): DataFrame a ser normalizado

    Returns:
        pandas.DataFrame: DataFrame com as colunas normalizadas
    """
    df_normalizado = df.copy()
    colunas_normalizadas = set()
    max_iteracoes = 10  # Limite de iterações para evitar loop infinito
    iteracao = 0

    while iteracao < max_iteracoes:
        iteracao += 1
        colunas_para_normalizar = []

        # Verifica todas as colunas do DataFrame
        for coluna in df_normalizado.columns:
            if coluna in colunas_normalizadas:
                continue

            try:
                primeiro_valor = df_normalizado[coluna].iloc[0]
                if isinstance(primeiro_valor, (dict, list)):
                    colunas_para_normalizar.append(coluna)
            except Exception:
                continue

        # Se não houver mais colunas para normalizar, sai do loop
        if not colunas_para_normalizar:
            break

        # Normaliza as colunas encontradas
        for coluna in colunas_para_normalizar:
            df_normalizado = normalizar_coluna(df_normalizado, coluna)
            colunas_normalizadas.add(coluna)

    # Limpa os nomes das colunas finais
    df_normalizado.columns = [limpar_nome_coluna(
        col) for col in df_normalizado.columns]

    return df_normalizado


# Função para chamar a API Omie
def fetch_data(page, registros_por_pagina, retry=3):
    payload = {
        "call": "ListarProdutoFornecedor",
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "param": [
            {
                "pagina": page,
                "registros_por_pagina": registros_por_pagina,  # Ajuste conforme necessário
            }
        ]
    }

    for attempt in range(retry):
        try:
            # print(f"Enviando payload: {payload}")  # Log do payload
            response = requests.post(BASE_URL, headers=HEADERS, json=payload)
            # print(f"Resposta da API: {response.status_code}, {response.text}")  # Log da resposta
            if response.status_code == 429:
                logger.info(
                    "Limite de requisições atingido. Aguardando 60 segundos...")
                time.sleep(60)
                continue
            response.raise_for_status()

            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Tentativa {attempt + 1}/{retry} falhou. Erro: {e}")
            time.sleep(10)
    raise Exception("Falha ao chamar a API após várias tentativas.")


# Função para processar os dados e criar um DataFrame
def process_data():
    all_records = []
    df_fornecedor_produto = pd.DataFrame()
    page = 1
    total_requests = 0  # Contador de requisições
    max_requests = 200  # Limite de requisições para teste
    registros_por_pagina = 10

    while True:
        try:
            data = fetch_data(page, registros_por_pagina)
            registros = data["cadastros"]
            total_de_paginas = data.get("total_de_paginas", 0)

            if len(registros) > 0:
                # Para cada fornecedor, expande seus produtos
                for fornecedor in registros:
                    produtos = fornecedor.get('produtos', [])
                    for produto in produtos:
                        # Cria um registro combinando dados do fornecedor e do produto
                        registro = {
                            'cod_fornecedor': fornecedor.get('nCodForn'),
                            'nome_fantasia': fornecedor.get('cNomeFantasia'),
                            'razao_social': fornecedor.get('cRazaoSocial'),
                            'cpf_cnpj': fornecedor.get('cCpfCnpj'),
                            'id_produto': produto.get('nCodProd'),
                            'cod_produto_fornecedor': produto.get('cCodigo'),
                            'de_produto': produto.get('cDescricao')
                        }
                        all_records.append(registro)

                if all_records:  # Só cria o DataFrame se houver registros
                    df_stg = pd.DataFrame(all_records)
                    df_fornecedor_produto = pd.concat(
                        [df_fornecedor_produto, df_stg], ignore_index=True)

                    # IMPRIME A PRIMEIRA LINHA DO DATAFRAME
                    def print_primeira_linha(df):
                        if not df.empty:
                            logger.info("Primeiro registro do DataFrame:")
                            for coluna, valor in df.iloc[0].items():
                                logger.info(f"{coluna}: {valor}")
                            logger.info("#" * 100)
                            return df.iloc[0]
                        else:
                            logger.warning("O DataFrame está vazio.")
                            logger.warning("#" * 100)
                            return None

                    logger.info(
                        f"Total de registros: {len(df_fornecedor_produto)}")

                page += 1
                total_requests += 1

                if total_requests >= max_requests:
                    logger.warning(
                        "Limite de 200 requisições atingido. Finalizando.")
                    break

                if page > total_de_paginas:
                    break
            else:
                logger.warning("Nenhum dado encontrado. Finalizando.")
                break

            logger.info(f"Página {page} de {total_de_paginas}")

        except Exception as e:
            logger.error(f"Erro ao buscar dados: {e}")
            break

    if df_fornecedor_produto.empty:
        raise Exception("Nenhum dado foi retornado pela API.")

    # Remove zeros à esquerda da coluna codigo_produto
    df_fornecedor_produto['cod_produto_fornecedor'] = df_fornecedor_produto['cod_produto_fornecedor'].astype(
        str).str.lstrip('0')

    df_fornecedor_produto = df_fornecedor_produto.drop_duplicates()

    return df_fornecedor_produto


def save_to_gcs(df):
    try:
        # Inicializa o cliente do GCS
        storage_client = storage.Client(
            credentials=credentials, project=PROJECT_ID)

        # Cria o caminho completo do arquivo no GCS
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        destination_blob_name = f"{DESTINATION_FOLDER}fornecedor_produto.csv"

        # Converte o DataFrame para CSV em memória
        csv_data = df.to_csv(index=False, encoding='windows-1252', sep=';')

        # Carrega o CSV para o Google Cloud Storage
        bucket = storage_client.get_bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(csv_data, content_type='text/csv')

        logger.success(
            f"Dados enviados para o GCS: gs://{BUCKET_NAME}/{destination_blob_name}")

    except Exception as e:
        logger.error(f"Erro ao enviar dados para o GCS: {e}")


# Executa o script
if __name__ == "__main__":
    try:
        # Processar os dados
        df = process_data()

        # Enviar os dados para o BigQuery
        save_to_gcs(df)

    except Exception as e:
        logger.error(f"Erro ao processar e salvar os dados: {e}")
