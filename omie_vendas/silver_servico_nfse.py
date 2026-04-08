import pandas as pd
import ast
from  configuracoes.variaveis import *
from  configuracoes.funcoes import *
from google.cloud import storage
from configuracoes.setup_logging import get_logger

# Configurar logging
logger = get_logger("silver_servico_nfse.py")

logger.info("TRANSFORMAÇÃO NFSE")
logger.info(f"Buscando os arquivos para a transformação")

## CARREGA DADOS DO ARQUIVO YAML
logger.info("Carregando dados da GCS")
config_yaml = load_yaml()
credentials_path = config_yaml.get("credentials_path")
raw = config_yaml.get("bucket_raw")
silver = config_yaml.get("bucket_silver")
client_storage = storage.Client.from_service_account_json(credentials_path)
bucket_raw = client_storage.bucket(raw)
bucket_silver = client_storage.bucket(silver)
pasta_bucket = "servicos"
csv_nfse_stg = "nfse"


## CARREGA OS DADOS JÁ COLETADOS DA NOTA FSICAL - CAMADA RAW(BRONZE)
df_nfse_stg = load_gcs(bucket_raw, pasta_bucket, csv_nfse_stg)

colunas_selecionadas = [ "Cabecalho_cAmbienteNFSe","Cabecalho_cCidadeEmissor","Cabecalho_cCodigoVerifNFSe","Cabecalho_cStatusNFSe","Cabecalho_nCodNF","Cabecalho_nCodigoCliente",
                         "Cabecalho_nNumeroNFSe","Cabecalho_nValorNFSe","Cancelamento_cDataCancelamento","Emissao_cDataEmissao","Inclusao_cDataInclusao","OrdemServico_nCodigoOS",
                         "OrdemServico_nNumeroOS","OrdemServico_nValorOS","RPS_cSerieRPS","RPS_cStatusRPS","RPS_nNumeroLote","RPS_nNumeroRPS","RPS_nStatusLote","Servicos_CidadePrestacao",
                         "Valores_cIssRetido","Valores_nValorLiquido","Valores_nValorTotalServicos"]

df_nfse_stg = df_nfse_stg[colunas_selecionadas].rename(columns={
   "Cabecalho_cAmbienteNFSe" : "fl_ambiente_nfse",
   "Cabecalho_cCidadeEmissor" : "de_cidade_emissor",
   "Cabecalho_cCodigoVerifNFSe" : "cod_verificador_nfse",
   "Cabecalho_cStatusNFSe" : "fl_status_nfse",
   "Cabecalho_nCodNF" : "cod_nfse",
   "Cabecalho_nCodigoCliente" : "cod_cliente",
   "Cabecalho_nNumeroNFSe" : "num_nfse",
   "Cabecalho_nValorNFSe" : "vlr_nfse",
   "Cancelamento_cDataCancelamento" : "dt_cancelamento_nfse",
   "Emissao_cDataEmissao" : "dt_emissao_nfse",
   "Inclusao_cDataInclusao" : "dt_inclusao_nfse",
   "OrdemServico_nCodigoOS" : "cod_ordem_servico",
   "OrdemServico_nNumeroOS" : "num_ordem_servico",
   "OrdemServico_nValorOS" : "vlr_ordem_servico",
   "RPS_cSerieRPS" : "serie_rps",
   "RPS_cStatusRPS" : "fl_status_rps",
   "RPS_nNumeroLote" : "num_lote",
   "RPS_nNumeroRPS" : "num_rps",
   "RPS_nStatusLote" : "fl_status_lote",
   "Servicos_CidadePrestacao" : "de_cidade_prestacao",
   "Valores_cIssRetido" : "flag_iss_retido",
   "Valores_nValorLiquido" : "vlr_liquido_servico",
   "Valores_nValorTotalServicos" : "vlr_total_servico",   
})


## PRINT O DATAFRAME NO SENTIDO VERTICAL
# for _, row in df_ordem_servico_stg.iterrows():
#     print("\n".join(f"{col:<25} {row[col]}" for col in df_ordem_servico_stg.columns))
#     print("-" * 40)

df_final = df_nfse_stg.copy()

### CRIA LISTA DE DATAFRAMES
data_frames = [
   (df_final, "nfse")
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
  blob = bucket_silver.blob(pasta_destino)
  blob.upload_from_string(csv_data, content_type='text/csv')
  logger.info(f"Arquivo {pasta_destino} salvo com sucesso")