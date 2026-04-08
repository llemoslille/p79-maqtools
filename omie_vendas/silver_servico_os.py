import pandas as pd
import ast
from  configuracoes.variaveis import *
from  configuracoes.funcoes import *
from google.cloud import storage
from configuracoes.setup_logging import get_logger

# Configurar logging
logger = get_logger("silver_servico_os.py")

logger.info("TRANSFORMAÇÃO ORDEM DE SERVIÇO")
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
csv_ordem_servico_stg = "ordem_servico"

## CARREGA JÁ COLETADOS DA ORDEM DE SERVICO - CAMADA RAW(BRONZE)
df_ordem_servico_stg = load_gcs(bucket_raw, pasta_bucket, csv_ordem_servico_stg)


###############################################
### EXPANDIR COLUNAS DE SERVICOS PRESTADOS  ###
###############################################

## CRIA UM DATAFRAME PARA TRATARMOS OS DADOS
df_servicos_prestados_stg = df_ordem_servico_stg.copy()

## SELECIONA AS COLUNAS DESTE NOVO DATAFRAME
colunas_selecionadas = ['Cabecalho_nCodOS'] + [col for col in df_servicos_prestados_stg.columns if col.startswith('ServicosPrestados_')]
df_servicos_prestados_stg = df_servicos_prestados_stg[colunas_selecionadas]

## FAZ UM UNPIVOT DAS COLUNAS DE SERVIÇOS PRESTADOS, MANTENDO A Cabecalho_nCodOS
df_servicos_prestados_unpivot = df_servicos_prestados_stg.melt(id_vars=['Cabecalho_nCodOS'], 
                      value_vars=[col for col in df_servicos_prestados_stg.columns if col.startswith('ServicosPrestados_')],
                      var_name='Servico_Index', 
                      value_name='Servico_Detalhe')

## REMOVENDO NaNs DESNECESSÁRIOS
df_servicos_prestados_unpivot = df_servicos_prestados_unpivot.dropna(subset=['Servico_Detalhe']).reset_index(drop=True)


## CONVERTENR STRING EM DICIONÁRIOS PARA EXPANDIRConverter strings em dicionários
df_servicos_prestados_unpivot['Servico_Detalhe'] = df_servicos_prestados_unpivot['Servico_Detalhe'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

## EXPANDE A COLUNA SERVICO_DETALHE EM MÚLTIPLAS COLUNAS
df_servicos_prestados_expandido = df_servicos_prestados_unpivot.join(pd.json_normalize(df_servicos_prestados_unpivot['Servico_Detalhe'])).drop(columns=['Servico_Index', 'Servico_Detalhe'])

## SELECIONANDO APENAS AS COLUNAS NECESSÁRIAS PARA INCLUIR NO DATAFRAME ORDEM DE SERVIÇO
colunas_selecionadas = ['Cabecalho_nCodOS', 'cCodCategItem', 'cCodServLC116', 'cCodServMun', 'cDescServ',
                        'nAliqDesconto', 'nCodServico', 'nIdItem', 'nQtde', 'nSeqItem', 'nValUnit', 'nValorAcrescimos', 'nValorDesconto', 'nValorOutrasRetencoes']
df_servicos_prestados_expandido = df_servicos_prestados_expandido[colunas_selecionadas]

df_servicos_prestados = df_servicos_prestados_expandido.copy().drop_duplicates()

df_servicos_prestados = df_servicos_prestados.rename(columns={
    "Cabecalho_nCodOS" : "cod_ordem_servico",
    "cCodCategItem" : "cod_categoria_item",
    "cCodServLC116" : "cod_servi_lc116",
    "cCodServMun" : "cod_servico_municipio",
    "cDescServ" : "de_servico",
    "nAliqDesconto" : "aliquota_desconto",
    "nCodServico" : "cod_servico",
    "nIdItem" : "id_item",
    "nQtde" : "qtde_servico",
    "nSeqItem" : "sequecial_item",
    "nValUnit" : "vlr_unitario_servico",
    "nValorAcrescimos" : "vlr_acrescimo_servico",
    "nValorDesconto" : "vlr_desconto_servico",
    "nValorOutrasRetencoes" : "vlr_outras_retencoes_servico",
})

#####################################
### EXPANDIR COLUNAS DE PARCELAS  ###
#####################################

## CRIA UM DATAFRAME PARA TRATARMOS OS DADOS
df_parcelas_stg = df_ordem_servico_stg.copy()

## SELECIONA AS COLUNAS DESTE NOVO DATAFRAME
colunas_selecionadas = ['Cabecalho_nCodOS'] + [col for col in df_parcelas_stg.columns if col.startswith('Parcelas_')]
df_parcelas_stg = df_parcelas_stg[colunas_selecionadas]

## FAZ UM UNPIVOT DAS COLUNAS DE SERVIÇOS PRESTADOS, MANTENDO A Cabecalho_nCodOS
df_parcelas_unpivot = df_parcelas_stg.melt(id_vars=['Cabecalho_nCodOS'], 
                      value_vars=[col for col in df_parcelas_stg.columns if col.startswith('Parcelas_')],
                      var_name='Parcela_Index', 
                      value_name='Parcela_Detalhe')

## REMOVENDO NaNs DESNECESSÁRIOS
df_parcelas_unpivot =  df_parcelas_unpivot.dropna(subset=['Parcela_Detalhe']).reset_index(drop=True)


## CONVERTENR STRING EM DICIONÁRIOS PARA EXPANDIRConverter strings em dicionários
df_parcelas_unpivot['Parcela_Detalhe'] =  df_parcelas_unpivot['Parcela_Detalhe'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

## EXPANDE A COLUNA Parcela_Detalhe EM MÚLTIPLAS COLUNAS
df_parcelas_expandido =  df_parcelas_unpivot.join(pd.json_normalize(df_parcelas_unpivot['Parcela_Detalhe'])).drop(columns=['Parcela_Index', 'Parcela_Detalhe'])

df_parcelas = df_parcelas_expandido.copy()

df_parcelas = df_parcelas.rename(columns={
    "Cabecalho_nCodOS" : "cod_ordem_servico",
    "dDtVenc" : "dt_vencimento_parcela",
    "nDias" : "nr_dias_parcela",
    "nParcela" : "nr_parcela",
    "nPercentual" : "percentual",
    "nValor" : "vlr_parcela",
    "tipo_documento" : "tp_documento",
})


#################################################################
### UNIR SERVIÇOS PRESTADOS E PARCELAS COM A ORDEM DE SERVIÇO  ##
#################################################################

## SELECIONAR AS COLUNAS DA ORDEM DE SERVICO
colunas_selecionadas = ['Cabecalho_cCodParc', 'Cabecalho_cEtapa', 'Cabecalho_cNumOS', 'Cabecalho_dDtPrevisao', 'Cabecalho_nCodCli', 'Cabecalho_nCodOS', 'Cabecalho_nCodVend', 
                        'Cabecalho_nQtdeParc', 'Cabecalho_nValorTotal', 'Cabecalho_nValorTotalImpRet', 'InfoCadastro_cAmbiente', 'InfoCadastro_cFaturada', 'InfoCadastro_cCancelada', 
                        'InfoCadastro_dDtCanc', 'InfoCadastro_dDtFat', 'InfoCadastro_dDtInc']

df_ordem_servico_stg = df_ordem_servico_stg[colunas_selecionadas].rename(columns={
    "Cabecalho_cCodParc" : "cod_condicao_pagamento",
    "Cabecalho_cEtapa" : "cod_etapa",
    "Cabecalho_cNumOS" : "num_ordem_servico",
    "Cabecalho_dDtPrevisao" : "dt_previsao",
    "Cabecalho_nCodCli" : "cod_cliente",
    "Cabecalho_nCodOS" : "cod_ordem_servico",
    "Cabecalho_nCodVend" : "cod_vendedor",
    "Cabecalho_nQtdeParc" : "qtd_parcela_ordem_servico",
    "Cabecalho_nValorTotal" : "vlr_total_ordem_servico",
    "Cabecalho_nValorTotalImpRet" : "vlr_total_imposto_retencao",
    "InfoCadastro_cAmbiente" : "fl_ambiente",
    "InfoCadastro_cFaturada" : "fl_faturada",
    "InfoCadastro_cCancelada" : "fl_cancelada",
    "InfoCadastro_dDtCanc" : "dt_cancelamento_ordem_servico",
    "InfoCadastro_dDtFat" : "dt_faturamento_ordem_servico",
    "InfoCadastro_dDtInc" : "dt_cadastro_ordem_servico",
})


## UINIR PARCELAS COM A ORDEM DE SERVICO
df_ordem_servico_stg = df_ordem_servico_stg.merge(
   df_parcelas,
   on="cod_ordem_servico",
   how="left"
).drop_duplicates()

### UNIR SERVICOS PRESTADOS COM ORDEM DE SERVICO
df_ordem_servico_stg = df_ordem_servico_stg.merge(
   df_servicos_prestados,
   on="cod_ordem_servico",
   how="left"
).drop_duplicates()

## PRINT O DATAFRAME NO SENTIDO VERTICAL
# for _, row in df_ordem_servico_stg.iterrows():
#     print("\n".join(f"{col:<25} {row[col]}" for col in df_ordem_servico_stg.columns))
#     print("-" * 40)



df_final = df_ordem_servico_stg.copy()

### CRIA LISTA DE DATAFRAMES
data_frames = [
   (df_final, "ordem_servico")
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