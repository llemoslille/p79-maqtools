import pandas as pd
import json
import ast
import time
from  configuracoes.variaveis import *
from  configuracoes.funcoes import *
from google.cloud import storage
from configuracoes.setup_logging import get_logger

# Configurar logging
logger = get_logger("raw_servico_os.py")

logger.info("REQUISIÇÃO DE ORDEM DE SERVIÇO")

## CONSULTA ENDPOIN
endpoint = f"{omie_v1}{ordem_servico}"
logger.info(f"Consultando o Endpoint {endpoint}")

## CARREGA DADOS DO ARQUIVO YAML
logger.info("Carregando dados da GCS")
config_yaml = load_yaml()
credentials_path = config_yaml.get("credentials_path")
raw = config_yaml.get("bucket_raw")
client_storage = storage.Client.from_service_account_json(credentials_path)
bucket = client_storage.bucket(raw)
pasta_bucket = "servicos"

## CONSULTA ENDPOINT DAS NOTAS FISCAIS DE SERVIÇOS
nr_pagina = 1
return_api = endpoint_os(nr_pagina)

## RETORNO DA CONSULTA
status = return_api.get("status_endpoint")
response = return_api.get("retorno_endpoint")
data = json.loads(response)

if "pagina" not in data or "total_de_paginas" not in data:
    logger.error("A resposta da API não contém os dados esperados.")
    exit()


nr_pagina = data["pagina"]
total_paginas = data["total_de_paginas"]
nr_registros = data["registros"]
total_registros = data["total_de_registros"]

## CRIA DATAFRAME OS VAZIO
df_os = pd.DataFrame()

if status == 200:
  logger.info(f"Total de registros {total_registros}")
  while nr_pagina <= total_paginas:
    logger.info(f"Consultando página {nr_pagina} de {total_paginas}")
    return_api = endpoint_os(nr_pagina)
    response_status = return_api.get("status_endpoint")
    
    # Verifica se a requisição foi bem-sucedida
    if response_status != 200:
        logger.warning(f"Erro ao acessar API na página {nr_pagina}: {response_status} - {return_api.get('retorno_endpoint', '')}")
        nr_pagina += 1
        continue
    
    data = json.loads(return_api.get("retorno_endpoint"))
    
    # Verifica se há erros na resposta da API
    if "faultstring" in data or "faultcode" in data:
        error_msg = data.get("faultstring", "Erro desconhecido")
        logger.warning(f"Erro na resposta da API na página {nr_pagina}: {error_msg}")
        
        # Se for erro de consumo redundante, aguarda e tenta novamente
        if "Consumo redundante" in error_msg or "aguarde" in error_msg.lower():
            # Tenta extrair o tempo de espera da mensagem
            try:
                import re
                wait_match = re.search(r'(\d+)\s*segundos?', error_msg, re.IGNORECASE)
                if wait_match:
                    wait_time = int(wait_match.group(1)) + 2  # Adiciona 2 segundos de margem
                    logger.info(f"Aguardando {wait_time} segundos antes de continuar...")
                    time.sleep(wait_time)
                    continue
            except:
                logger.info("Aguardando 30 segundos antes de continuar...")
                time.sleep(30)
                continue
        
        # Se for erro de página inexistente, para o loop
        if "não existem registros para a página" in error_msg.lower() or "não existem mais registros" in error_msg.lower():
            logger.info(f"Não existem mais registros para a página {nr_pagina}")
            break
        
        nr_pagina += 1
        continue
    
    # Usa .get() com lista vazia como padrão para evitar KeyError
    os_cadastro = data.get("osCadastro", [])
    
    if os_cadastro:
        df = pd.DataFrame(os_cadastro)
        df_os = pd.concat([df_os, df], ignore_index=True)
        logger.info(f"Registros coletados da página {nr_pagina}: {len(df)}")
    else:
        logger.warning(f"Nenhuma OS encontrada na página {nr_pagina}")
    
    nr_pagina += 1

else:
  logger.info(f"Status: {status}")

# Verifica se há dados para processar
if df_os.empty:
    logger.warning("Nenhuma OS foi coletada. Criando arquivo vazio.")
    df_final = pd.DataFrame()
    df_teste = pd.DataFrame()
else:
    logger.info(f"Total de registros coletados: {len(df_os)}")

    df_teste = df_os.copy() 
    ### EXPANDINDO COLUNA ANINHADAS 'Adicionais', 'Alteracao', 'Cabecalho', 'Cancelamento', 'Emissao', 'Inclusao', 'ListaServicos', 'OrdemServico', 'RPS', 'Servicos','Valores'
    colunas_para_expandir = ['Cabecalho', 'Departamentos', 'Email', 'InfoCadastro', 'InformacoesAdicionais',
           'Observacoes', 'Parcelas', 'ServicosPrestados']

    for col in colunas_para_expandir:
       if col in df_os:
          try:
              df_expandido = df_os[col].apply(pd.Series)
              df_expandido = df_expandido.add_prefix(f"{col}_")  # Adiciona o nome da coluna original como prefixo
              df_os = df_os.drop(columns=[col]).join(df_expandido)
          except Exception as e:
              logger.warning(f"Erro ao expandir coluna {col}: {e}")

    df_final = df_os.copy()

### CRIA LISTA DE DATAFRAMES
data_frames = [
   (df_final, "ordem_servico"),
   (df_teste, "ordem_servico_teste")
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
  blob = bucket.blob(pasta_destino)
  blob.upload_from_string(csv_data, content_type='text/csv')
  logger.info(f"Arquivo {pasta_destino} salvo com sucesso")