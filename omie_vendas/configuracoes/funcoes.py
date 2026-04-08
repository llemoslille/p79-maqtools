from configuracoes.variaveis import *
from loguru import logger
from google.cloud import storage
import pandas as pd
import yaml
import requests
import json
import os
import io

## FUNÇÃO PARA CARREGAR AQUIVO YAML
def load_yaml():
    with open(arquivo_yaml) as file:
        config = yaml.safe_load(file)

    project_id = config['project-id']
    project_name = config['project-name']
    cloud = config['cloud']
    credentials_path = config['credentials-path']
    bucket_projeto = config['bucket-projeto']
    bucket_raw = config['bucket-raw']
    bucket_silver = config['bucket-silver']
    bucket_gold = config['bucket-gold']
    bucket_processed = config['bucket-processed']
    bucket_dw = config['bucket-dw']

    return{
        'project_id' : project_id,
        'project_name' : project_name,
        'cloud' : cloud,
        'credentials_path' : credentials_path,
        'bucket_projeto' : bucket_projeto,
        'bucket_raw' : bucket_raw,
        'bucket_silver' : bucket_silver,
        'bucket_gold' : bucket_gold,
        'bucket_processed' : bucket_processed,
        'bucket_dw' : bucket_dw,
    }

## FUNÇÃO PARA CONSULTAR ENDPOINT NFSE
def endpoint_nfse(nr_pagina):
    url = f"{omie_v1}{nfse}"

    payload = json.dumps({
    "call": f"{lista_nfse}",
    "app_key": f"{app_key}",
    "app_secret": f"{app_secret}",
    "param": [
        {
        "nPagina": nr_pagina,
        "nRegPorPagina": 50,
        }
    ]
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    
    return {
        'status_endpoint' : response.status_code,
        'retorno_endpoint' : response.text,
    }

## FUNÇÃO PARA CONSULTAR ENDPOINT ORDEM DE SERVIÇO (OS)
def endpoint_os(nr_pagina):
    url = f"{omie_v1}{ordem_servico}"

    payload = json.dumps({
    "call": f"{lista_os}",
    "app_key": f"{app_key}",
    "app_secret": f"{app_secret}",
    "param": [
        {
            "pagina": nr_pagina,
            "registros_por_pagina": 50,
            "apenas_importado_api": "N"
        }
    ]
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    
    return {
        'status_endpoint' : response.status_code,
        'retorno_endpoint' : response.text,
    }

## FUNÇÃO PARA RETORNAR AQUIVOS DA
def load_gcs(bucket, pasta, file_name_load):
    ### Load o arquivo da GCS
    blob_list = list(bucket.list_blobs(prefix=f'{pasta}/{file_name_load}.csv'))
    total_arquivos = len(blob_list)
    arquivo_atual = 1


    df_concat = pd.DataFrame()
    arquivo_atual = 1
    for blob in blob_list:
        logger.info(f"Arquivo atual {arquivo_atual} de {total_arquivos} : {os.path.basename(blob.name)}")

        files_bytes = blob.download_as_bytes()
        file_buffer = io.BytesIO(files_bytes)
        df = pd.read_csv(file_buffer, sep=";")

        df_concat = pd.concat([df_concat, df], ignore_index=True)
        
        arquivo_atual = int(arquivo_atual)
        arquivo_atual += 1
    return df_concat