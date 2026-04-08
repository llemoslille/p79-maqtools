import pandas as pd
import json
import ast
from configuracoes import variaveis
from configuracoes import funcoes
from google.cloud import storage
from configuracoes.setup_logging import get_logger

# Configurar logging
logger = get_logger("raw_servico_nfse.py")

logger.info("REQUISIÇÃO DE NFSE")

# CONSULTA ENDPOIN
endpoint = f"{variaveis.omie_v1}{variaveis.nfse}"
logger.info(f"Consultando o Endpoint {endpoint}")

# CARREGA DADOS DO ARQUIVO YAML
logger.info("Carregando dados da GCS")
pasta_bucket = "servicos"
config_yaml = funcoes.load_yaml()
credentials_path = config_yaml.get("credentials_path")
raw = config_yaml.get("bucket_raw")
client_storage = storage.Client.from_service_account_json(credentials_path)
bucket = client_storage.bucket(raw)
pasta_bucket = "servicos"

# CONSULTA ENDPOINT DAS NOTAS FISCAIS DE SERVIÇOS
nr_pagina = 1
return_api = funcoes.endpoint_nfse(nr_pagina)

# RETORNO DA CONSULTA
status = return_api.get("status_endpoint")
response = return_api.get("retorno_endpoint")
data = json.loads(response)

if "nPagina" not in data or "nTotPaginas" not in data:
    logger.error("A resposta da API não contém os dados esperados.")
    exit()


nr_pagina = data["nPagina"]
total_paginas = data["nTotPaginas"]
nr_registros = data["nRegistros"]
total_registros = data["nTotRegistros"]

# CRIA DATAFRAME NFSE VAZIO
df_nfse = pd.DataFrame()

if status == 200:
    while nr_pagina <= total_paginas:
        logger.info(f"Consultando página {nr_pagina} de {total_paginas}")
        return_api = funcoes.endpoint_nfse(nr_pagina)
        response_status = return_api.get("status_endpoint")

        # Verifica se a requisição foi bem-sucedida
        if response_status != 200:
            logger.warning(
                f"Erro ao acessar API na página {nr_pagina}: {response_status} - {return_api.get('retorno_endpoint', '')}")
            nr_pagina += 1
            continue

        data = json.loads(return_api.get("retorno_endpoint"))

        # Verifica se há erros na resposta da API
        if "faultstring" in data or "faultcode" in data:
            logger.warning(
                f"Erro na resposta da API na página {nr_pagina}: {data.get('faultstring', 'Erro desconhecido')}")
            nr_pagina += 1
            continue

        # Usa .get() com lista vazia como padrão para evitar KeyError
        nfse_encontradas = data.get("nfseEncontradas", [])

        if nfse_encontradas:
            df = pd.DataFrame(nfse_encontradas)
            df_nfse = pd.concat([df_nfse, df], ignore_index=True)
            logger.info(
                f"Registros coletados da página {nr_pagina}: {len(df)}")
        else:
            logger.warning(f"Nenhuma NFSE encontrada na página {nr_pagina}")

        nr_pagina += 1

else:
    logger.info(f"Status: {status}")

# Verifica se há dados para processar
if df_nfse.empty:
    logger.warning("Nenhuma NFSE foi coletada. Criando arquivo vazio.")
    df_nfse_final = pd.DataFrame()
else:
    logger.info(f"Total de registros coletados: {len(df_nfse)}")

    # EXPANDINDO COLUNA ANINHADAS 'Adicionais', 'Alteracao', 'Cabecalho', 'Cancelamento', 'Emissao', 'Inclusao', 'ListaServicos', 'OrdemServico', 'RPS', 'Servicos','Valores'

    colunas_para_expandir = ['Adicionais', 'Alteracao', 'Cabecalho', 'Cancelamento', 'Emissao',
                             'Inclusao', 'ListaServicos', 'OrdemServico', 'RPS', 'Servicos', 'Valores']

    df_nfse_stg = df_nfse.copy()
    df_nfse_final = df_nfse.copy()

    for col in colunas_para_expandir:
        if col in df_nfse:
            df_expandido = df_nfse[col].apply(pd.Series)
            # Adiciona o nome da coluna original como prefixo
            df_expandido = df_expandido.add_prefix(f"{col}_")
            df_nfse_final = df_nfse_final.drop(
                columns=[col]).join(df_expandido)


# CRIA LISTA DE DATAFRAMES
data_frames = [
    (df_nfse_final, "nfse")
]

# SALVANDO ARQUIVO NA GCS
for df, nm_arquivo in data_frames:
    pasta_destino = f"{pasta_bucket}/{nm_arquivo}.csv"

    try:
        csv_data = df.to_csv(index=False, encoding="windows-1252", sep=";")
        logger.info("Arquivo convertido com sucesso")
    except UnicodeDecodeError as e:
        logger.info(
            f"Fala ao salvar com 'windows-1252. Erro {e}. Tentando com 'utf-8'")
        csv_data = df.to_csv(index=False, encoding='utf-8', sep=";")
        logger.info("Arquivo convertido com sucesso")

    # CARREGA ARQUIVO CSV PARA GCS
    blob = bucket.blob(pasta_destino)
    blob.upload_from_string(csv_data, content_type='text/csv')
    logger.info(f"Arquivo {pasta_destino} salvo com sucesso")
