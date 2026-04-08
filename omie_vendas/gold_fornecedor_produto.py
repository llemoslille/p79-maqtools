import pandas as pd
import ast
from  configuracoes.variaveis import *
from  configuracoes.funcoes import *
from google.cloud import storage
from configuracoes.setup_logging import get_logger

# Configurar logging
logger = get_logger("gold_fornecedor_produto.py")

logger.info("PROCESSO PARA SALVAR NA CAMADA GOLD")
logger.info(f"Buscando os arquivos para a criar fato ordem de serviço")

## CARREGA DADOS DO ARQUIVO YAML
logger.info("Carregando dados da GCS")
config_yaml = load_yaml()
credentials_path = config_yaml.get("credentials_path")
raw = config_yaml.get("bucket_raw")
gold = config_yaml.get("bucket_gold")
client_storage = storage.Client.from_service_account_json(credentials_path)
bucket_raw = client_storage.bucket(raw)
bucket_gold = client_storage.bucket(gold)
pasta_bucket = "fornecedor_produto"
csv_fornecedor_produto_stg = "fornecedor_produto"


## CARREGA OS DADOS JÁ TRATADOS DA NOTA FSICAL - CAMADA SILVER
df_fornecedor_produto_stg = load_gcs(bucket_raw, pasta_bucket, csv_fornecedor_produto_stg)


### CRIA LISTA DE DATAFRAMES
data_frames = [
   (df_fornecedor_produto_stg, "fornecedor_produto")
]

### SALVANDO ARQUIVO NA GCS
for df, nm_arquivo in data_frames:
  pasta_destino = f"{pasta_bucket}/{nm_arquivo}.csv"

  try:
     csv_data = df.to_csv(index=False, encoding="windows-1252", sep=";")
     logger.info("Arquivo convertido com sucesso")
  except UnicodeDecodeError as e:
     logger.info(f"Fala ao salvar com 'windows-1252. Erro {e}. Tentando com 'utf-8'")
     csv_data = df.to_csv(index=False, encoding='utf-8', sep=";")
     logger.info("Arquivo convertido com sucesso")

  ### CARREGA ARQUIVO CSV PARA GCS
  blob = bucket_gold.blob(pasta_destino)
  blob.upload_from_string(csv_data, content_type='text/csv')
  logger.info(f"Arquivo {pasta_destino} salvo com sucesso")