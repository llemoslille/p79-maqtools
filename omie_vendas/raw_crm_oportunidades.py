import requests
import json
import os
import time
import pandas as pd
import numpy as np
from google.cloud import storage
from configuracoes.setup_logging import get_logger

# Configurar logging
logger = get_logger("raw_crm_oportunidades.py")

# Configuração do Google Cloud Storage
GCS_BUCKET_NAME = "maq_vendas"  # Substitua pelo nome do seu bucket
GCS_DESTINATION_BLOB_NAME = "dados_crm/oportunidades.csv"  # Caminho do arquivo no GCS


def _resolve_gcp_credentials_path() -> str:
    return (
        os.getenv("GCS_CREDENTIALS_JSON_PATH")
        or os.getenv("MACHTOOLS_JSON_PATH")
        or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or "machtools.json"
    )


GCS_CREDENTIALS_PATH = _resolve_gcp_credentials_path()

# Verifica se o arquivo de credenciais do GCS existe
if not os.path.exists(GCS_CREDENTIALS_PATH):
    erro_msg = f"Arquivo de credenciais não encontrado: {GCS_CREDENTIALS_PATH}"
    logger.error(erro_msg)
    raise FileNotFoundError(erro_msg)

# Configura o ambiente com as credenciais do GCS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_CREDENTIALS_PATH

logger.info("Iniciando coleta de oportunidades do CRM")

# URL da API
url = "https://app.omie.com.br/api/v1/crm/oportunidades/"
app_key = '1733209266789'
app_secret = '14c2f271c839dfea25cfff4afb63b331'

# Configuração inicial da paginação
pagina = 1
registros_por_pagina = 50
todas_oportunidades = []


def fazer_requisicao_api(pagina_atual, max_retries=3, backoff_base=2):
    """
    Faz requisição à API com retry e tratamento de erros

    Args:
        pagina_atual (int): Número da página a consultar
        max_retries (int): Número máximo de tentativas
        backoff_base (int): Base para cálculo de backoff exponencial

    Returns:
        dict: Dados da resposta da API ou None em caso de falha
    """
    payload = json.dumps({
        "call": "ListarOportunidades",
        "app_key": app_key,
        "app_secret": app_secret,
        "param": [
            {
                "pagina": pagina_atual,
                "registros_por_pagina": registros_por_pagina
            }
        ]
    })

    headers = {'Content-Type': 'application/json'}

    for tentativa in range(max_retries):
        try:
            response = requests.post(
                url, headers=headers, data=payload, timeout=30)

            # Tratamento específico para erro 500 (como visto no log)
            if response.status_code == 500:
                try:
                    error_data = response.json()
                    error_msg = error_data.get(
                        "faultstring", "Erro desconhecido")
                    logger.warning(
                        f"Erro 500 na página {pagina_atual}: {error_msg}")

                    # Se for erro de página inexistente, retorna None para parar o loop
                    if "não existem registros para a página" in error_msg.lower():
                        logger.info(
                            f"Não existem mais registros para a página {pagina_atual}")
                        return None
                except:
                    logger.warning(
                        f"Erro 500 na página {pagina_atual} - resposta não é JSON válido")

                if tentativa < max_retries - 1:
                    wait_time = (backoff_base ** tentativa) + 2
                    logger.info(
                        f"Aguardando {wait_time}s antes de tentar novamente (tentativa {tentativa + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(
                        f"Erro 500 persistente na página {pagina_atual} após {max_retries} tentativas")
                    return None

            # Tratamento para limite de requisições (429)
            if response.status_code == 429:
                wait_time = 60
                logger.warning(
                    f"Limite de requisições atingido. Aguardando {wait_time}s...")
                time.sleep(wait_time)
                continue

            # Verifica se a resposta foi bem-sucedida
            if response.status_code == 200:
                try:
                    data = response.json()
                    return data
                except json.JSONDecodeError as e:
                    logger.error(
                        f"Erro ao decodificar JSON da página {pagina_atual}: {e}")
                    if tentativa < max_retries - 1:
                        time.sleep(backoff_base ** tentativa)
                        continue
                    return None
            else:
                response.raise_for_status()

        except requests.exceptions.Timeout:
            logger.warning(
                f"Timeout na requisição da página {pagina_atual} (tentativa {tentativa + 1}/{max_retries})")
            if tentativa < max_retries - 1:
                wait_time = (backoff_base ** tentativa) + 1
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"Timeout persistente na página {pagina_atual}")
                return None

        except requests.exceptions.ConnectionError as e:
            logger.warning(
                f"Erro de conexão na página {pagina_atual} (tentativa {tentativa + 1}/{max_retries}): {e}")
            if tentativa < max_retries - 1:
                wait_time = (backoff_base ** tentativa) + 1
                time.sleep(wait_time)
                continue
            else:
                logger.error(
                    f"Erro de conexão persistente na página {pagina_atual}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(
                f"Erro na requisição da página {pagina_atual} (tentativa {tentativa + 1}/{max_retries}): {e}")
            if tentativa < max_retries - 1:
                wait_time = (backoff_base ** tentativa) + 1
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"Erro persistente na página {pagina_atual}")
                return None

    return None


# Loop principal de coleta
while True:
    logger.info(f"Buscando dados das oportunidades - Página {pagina}...")

    data = fazer_requisicao_api(pagina)

    # Se retornar None, significa que não há mais dados ou houve erro persistente
    if data is None:
        logger.info("Finalizando coleta de dados")
        break

    # Obtém as oportunidades da página atual
    oportunidades = data.get("cadastros", [])

    # Se não houver mais registros, interrompe o loop
    if not oportunidades:
        logger.info(f"Nenhuma oportunidade encontrada na página {pagina}")
        break

    # Adiciona os dados da página atual à lista geral
    todas_oportunidades.extend(oportunidades)
    logger.info(
        f"Coletadas {len(oportunidades)} oportunidades da página {pagina} (Total: {len(todas_oportunidades)})")

    # Avança para a próxima página
    pagina += 1

# Verifica se há dados coletados
if not todas_oportunidades:
    logger.warning("Nenhuma oportunidade foi coletada. Criando arquivo vazio.")
    df_expandido = pd.DataFrame()
else:
    logger.info(
        f"Total de oportunidades coletadas: {len(todas_oportunidades)}")

    # Converte os dados para DataFrame
    df_oportunidades = pd.DataFrame(todas_oportunidades)

    # Supondo que df_oportunidades seja o DataFrame original
    df_expandido = df_oportunidades.copy()

    # Identifica colunas que contêm dicionários e expande
    colunas_dict = ["concorrentes", "envolvidos", "previsaoTemp",
                    "ticket", "fasesStatus", "identificacao", "observacoes"]

    for col in colunas_dict:
        if col in df_expandido.columns:
            try:
                df_expandido = pd.concat([df_expandido.drop(
                    columns=[col]), df_expandido[col].apply(pd.Series).add_prefix(f"{col}_")], axis=1)
            except Exception as e:
                logger.warning(f"Erro ao expandir coluna {col}: {e}")

    df_expandido = df_expandido.copy()
    colunas_dict = ["outrasInf"]

    for col in colunas_dict:
        if col in df_expandido.columns:
            try:
                df_expandido = pd.concat([df_expandido.drop(
                    columns=[col]), df_expandido[col].apply(pd.Series).add_prefix(f"{col}_")], axis=1)
            except Exception as e:
                logger.warning(f"Erro ao expandir coluna {col}: {e}")

# Exibe o número total de registros coletados
print(f"Total de oportunidades coletadas: {len(todas_oportunidades)}")

# print(df_expandido.columns)
# Renomeação de colunas
rename_columns = {
    "envolvidos_nCodFinder": "codigo_finder",
    "envolvidos_nCodParceiro": "codigo_parceiro",
    "envolvidos_nCodPrevenda": "codigo_prevenda",
    "previsaoTemp_nAnoPrev": "ano_previsao",
    "previsaoTemp_nMesPrev": "mes_previsao",
    "previsaoTemp_nTemperatura": "temperatura",
    "ticket_nMeses": "meses_ticket",
    "ticket_nProdutos": "produtos_ticket",
    "ticket_nRecorrencia": "recorrencia_ticket",
    "ticket_nServicos": "servicos_ticket",
    "ticket_nTicket": "valor_ticket",
    "fasesStatus_dConclusao": "data_conclusao",
    "fasesStatus_dNovoLead": "data_novo_lead",
    "fasesStatus_dProjeto": "data_projeto",
    "fasesStatus_dQualificacao": "data_qualificacao",
    "fasesStatus_dShowRoom": "data_showroom",
    "fasesStatus_dTreinamento": "data_treinamento",
    "fasesStatus_nCodFase": "codigo_fase",
    "fasesStatus_nCodMotivo": "codigo_motivo",
    "fasesStatus_nCodStatus": "codigo_status",
    "identificacao_cCodIntOp": "codigo_interno_oportunidade",
    "identificacao_cDesOp": "descricao_oportunidade",
    "identificacao_nCodConta": "codigo_conta",
    "identificacao_nCodContato": "codigo_contato",
    "identificacao_nCodOp": "codigo_oportunidade",
    "identificacao_nCodOrigem": "codigo_origem",
    "identificacao_nCodSolucao": "codigo_solucao",
    "identificacao_nCodVendedor": "codigo_vendedor",
    "observacoes_cObs": "observacoes",
    "outrasInf_cEmailOp": "email_oportunidade",
    "outrasInf_dAlteracao": "data_alteracao",
    "outrasInf_dInclusao": "data_inclusao",
    "outrasInf_hAlteracao": "hora_alteracao",
    "outrasInf_hInclusao": "hora_inclusao",
    "outrasInf_nCodTipo": "codigo_tipo",
}
if not df_expandido.empty:
    df_expandido = df_expandido.rename(columns=rename_columns)

    # Cria dt_previsao apenas se as colunas necessárias existirem e tiverem valores
    if 'ano_previsao' in df_expandido.columns and 'mes_previsao' in df_expandido.columns:
        try:
            # Filtra apenas linhas com valores válidos
            mask = df_expandido['ano_previsao'].notna(
            ) & df_expandido['mes_previsao'].notna()
            df_expandido.loc[mask, 'dt_previsao'] = pd.to_datetime(
                df_expandido.loc[mask, 'ano_previsao'].astype(str) + '-' +
                df_expandido.loc[mask, 'mes_previsao'].astype(str) + '-01',
                errors='coerce'
            )
            logger.info(
                f"Coluna dt_previsao criada para {mask.sum()} registros")
        except Exception as e:
            logger.warning(f"Erro ao criar dt_previsao: {e}")

# Função para gerar parcelas mensais


def gerar_parcelas(row):
    try:
        meses_ticket = row.get("meses_ticket", 0)
        dt_previsao = row.get("dt_previsao")

        # Verifica se meses_ticket é válido
        if pd.isna(meses_ticket) or meses_ticket == 0:
            row["dt_recorrencia"] = dt_previsao if pd.notna(
                dt_previsao) else None
            return pd.DataFrame([row])

        # Verifica se dt_previsao é válida
        if pd.isna(dt_previsao):
            row["dt_recorrencia"] = None
            return pd.DataFrame([row])

        parcelas = []
        for i in range(int(meses_ticket)):
            nova_linha = row.copy()
            nova_linha["dt_recorrencia"] = dt_previsao + \
                pd.DateOffset(months=i)
            parcelas.append(nova_linha)

        return pd.DataFrame(parcelas)
    except Exception as e:
        logger.warning(f"Erro ao gerar parcelas para uma linha: {e}")
        # Retorna a linha original em caso de erro
        row["dt_recorrencia"] = row.get("dt_previsao")
        return pd.DataFrame([row])


# Aplicar a função para expandir o dataframe apenas se não estiver vazio
if not df_expandido.empty:
    try:
        df_expandido = pd.concat(df_expandido.apply(
            gerar_parcelas, axis=1).to_list(), ignore_index=True)
        logger.info(
            f"DataFrame expandido com parcelas. Total de registros: {len(df_expandido)}")
    except Exception as e:
        logger.error(f"Erro ao expandir parcelas: {e}")
        raise


# Salvar diretamente no GCS
try:
    logger.info("Salvando arquivo no GCS...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_DESTINATION_BLOB_NAME)

    # Converte o DataFrame para CSV
    if df_expandido.empty:
        # Cria um CSV vazio com pelo menos o cabeçalho das colunas esperadas
        csv_data = "codigo_oportunidade;codigo_conta;descricao_oportunidade\n"
        logger.warning("DataFrame vazio - criando arquivo CSV vazio")
    else:
        csv_data = df_expandido.to_csv(index=False, encoding="utf-8", sep=";")

    blob.upload_from_string(csv_data, content_type="text/csv")

    logger.success(
        f"Arquivo enviado para o GCS: gs://{GCS_BUCKET_NAME}/{GCS_DESTINATION_BLOB_NAME}")
    print(
        f"Arquivo enviado para o GCS: gs://{GCS_BUCKET_NAME}/{GCS_DESTINATION_BLOB_NAME}")

except Exception as e:
    logger.error(f"Erro ao salvar arquivo no GCS: {e}")
    raise
