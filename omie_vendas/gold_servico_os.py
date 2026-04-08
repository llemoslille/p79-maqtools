import pandas as pd
import ast
from  configuracoes.variaveis import *
from  configuracoes.funcoes import *
from google.cloud import storage
from configuracoes.setup_logging import get_logger

# Configurar logging
logger = get_logger("gold_servico_os.py")

logger.info("PROCESSO PARA SALVAR NA CAMADA GOLD")
logger.info(f"Buscando os arquivos para a criar fato ordem de serviço")

## CARREGA DADOS DO ARQUIVO YAML
logger.info("Carregando dados da GCS")
config_yaml = load_yaml()
credentials_path = config_yaml.get("credentials_path")
silver = config_yaml.get("bucket_silver")
gold = config_yaml.get("bucket_gold")
client_storage = storage.Client.from_service_account_json(credentials_path)
bucket_silver = client_storage.bucket(silver)
bucket_gold = client_storage.bucket(gold)
pasta_bucket = "servicos"
csv_ordem_stg = "ordem_servico"
csv_nfse_stg = "nfse"


## CARREGA OS DADOS JÁ TRATADOS DA NOTA FSICAL - CAMADA SILVER
df_ordem_servico_stg = load_gcs(bucket_silver, pasta_bucket, csv_ordem_stg)

## CARREGA OS DADOS JÁ TRATADOS DA NOTA FSICAL - CAMADA SILVER
df_nfse_stg = load_gcs(bucket_silver, pasta_bucket, csv_nfse_stg)

nfse_colunas_selecionadas = ["fl_ambiente_nfse","de_cidade_emissor","cod_verificador_nfse","fl_status_nfse","cod_nfse","num_nfse","vlr_nfse","dt_cancelamento_nfse",
                             "dt_emissao_nfse","dt_inclusao_nfse","cod_ordem_servico","serie_rps","fl_status_rps","num_lote","num_rps","fl_status_lote","de_cidade_prestacao",
                             "flag_iss_retido","vlr_liquido_servico"]

df_nfse_stg = df_nfse_stg[nfse_colunas_selecionadas]

df_fact_ordem_servico = df_ordem_servico_stg.merge(
   df_nfse_stg,
   on="cod_ordem_servico",
   how="left"
).drop_duplicates()


## PRINT O DATAFRAME NO SENTIDO VERTICAL
# for _, row in df_ordem_servico_stg.iterrows():
#     print("\n".join(f"{col:<25} {row[col]}" for col in df_ordem_servico_stg.columns))
#     print("-" * 40)

df_final = df_fact_ordem_servico.copy().drop(columns=['de_servico'])

### CRIA LISTA DE DATAFRAMES
data_frames = [
   (df_final, "fact_ordem_servico")
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