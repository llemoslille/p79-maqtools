import requests
import pandas as pd
import time
import os
import numpy as np
from google.cloud import storage
from configuracoes.setup_logging import get_logger

# Configurar logging
logger = get_logger("Omie_crm.py")

# Configurações da API do OMIE
BASE_URL = "https://app.omie.com.br/api/v1/crm/oportunidades/"
APP_KEY = "1733209266789"  # Substitua pela sua App Key
APP_SECRET = "14c2f271c839dfea25cfff4afb63b331"  # Substitua pela sua App Secret
HEADERS = {"Content-Type": "application/json"}


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
    raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {GCS_CREDENTIALS_PATH}")

# Configura o ambiente com as credenciais do GCS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCS_CREDENTIALS_PATH

# Função para chamar a API do OMIE
def fetch_data(page, retry=3):
    payload = {
        "call": "ListarOportunidades",
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "param": [
            {
                "pagina": page,
                "registros_por_pagina": 200,
                "apenas_importado_api": "N",
            }
        ]
    }

    # print("Payload enviado:", payload)  # Log do payload

    for attempt in range(retry):
        try:
            response = requests.post(BASE_URL, headers=HEADERS, json=payload)
            # print("Resposta da API:", response.status_code, response.text)  # Log da resposta

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
    logger.info("Iniciando processamento de dados CRM - Oportunidades")
    all_records = []
    page = 1

    while True:
        try:
            logger.info(f"Buscando dados da página {page}...")
            data = fetch_data(page)

            # Verifica se há registros na resposta
            if "cadastros" not in data or not data["cadastros"]:
                logger.info("Nenhum dado adicional encontrado. Finalizando.")
                break

            all_records.extend(data["cadastros"])
            logger.info(f"Página {page} processada - {len(data['cadastros'])} registros coletados")

            # Verifica se há mais páginas
            total_paginas = int(data.get("total_de_paginas", 1))
            if page >= total_paginas:
                logger.info(f"Todas as {total_paginas} páginas foram processadas. Finalizando.")
                break

            page += 1
        except Exception as e:
            logger.error(f"Erro ao buscar dados: {e}")
            break

    if not all_records:
        logger.error("Nenhum dado foi retornado pela API.")
        raise Exception("Nenhum dado foi retornado pela API.")
    
    logger.success(f"Total de {len(all_records)} registros coletados")

    # Criação do DataFrame
    df = pd.json_normalize(all_records)

    # Renomeação de colunas
    rename_columns = {
        "identificacao.cCodIntOp": "codigo_interno_oportunidade",
        "identificacao.cDesOp": "descricao_oportunidade",
        "identificacao.nCodConta": "codigo_conta",
        "identificacao.nCodContato": "codigo_contato",
        "identificacao.nCodOp": "codigo_oportunidade",
        "identificacao.nCodOrigem": "codigo_origem",
        "identificacao.nCodSolucao": "codigo_solucao",
        "identificacao.nCodVendedor": "codigo_vendedor",
        "fasesStatus.dConclusao": "data_conclusao",
        "fasesStatus.dNovoLead": "data_novo_lead",
        "fasesStatus.dProjeto": "data_projeto",
        "fasesStatus.dQualificacao": "data_qualificacao",
        "fasesStatus.dShowRoom": "data_showroom",
        "fasesStatus.dTreinamento": "data_treinamento",
        "fasesStatus.nCodFase": "codigo_fase",
        "fasesStatus.nCodMotivo": "codigo_motivo",
        "fasesStatus.nCodStatus": "codigo_status",
        "outrasInf.cEmailOp": "email_oportunidade",
        "outrasInf.dAlteracao": "data_alteracao",
        "outrasInf.dInclusao": "data_inclusao",
        "outrasInf.hAlteracao": "hora_alteracao",
        "outrasInf.hInclusao": "hora_inclusao",
        "outrasInf.nCodTipo": "codigo_tipo",
        "previsaoTemp.nAnoPrev": "ano_previsao",
        "previsaoTemp.nMesPrev": "mes_previsao",
        "previsaoTemp.nTemperatura": "temperatura",
        "ticket.nMeses": "meses_ticket",
        "ticket.nProdutos": "produtos_ticket",
        "ticket.nRecorrencia": "recorrencia_ticket",
        "ticket.nServicos": "servicos_ticket",
        "ticket.nTicket": "valor_ticket",
    }
    
    # Renomeia apenas as colunas que existem no DataFrame
    rename_columns = {k: v for k, v in rename_columns.items() if k in df.columns}
    df = df.rename(columns=rename_columns)
    
    # Normaliza a coluna concorrentes (expande array de concorrentes)
    # Verifica se a coluna concorrentes existe (pode vir como array ou já expandida)
    concorrentes_cols = [col for col in df.columns if 'concorrentes' in col.lower()]
    
    if 'concorrentes' in df.columns:
        print("Normalizando coluna 'concorrentes'...")
        expanded_concorrentes_rows = []
        
        for _, row in df.iterrows():
            concorrentes = row.get('concorrentes')
            
            # Verifica se está vazio/None de forma segura (evita erro com arrays)
            is_empty_or_none = False
            try:
                if concorrentes is None:
                    is_empty_or_none = True
                elif isinstance(concorrentes, np.ndarray):
                    # Para arrays numpy, verifica tamanho
                    is_empty_or_none = concorrentes.size == 0
                elif isinstance(concorrentes, (list, tuple)):
                    # Para listas/tuplas, verifica comprimento
                    is_empty_or_none = len(concorrentes) == 0
                else:
                    # Para outros tipos, tenta pd.isna de forma segura
                    try:
                        if hasattr(concorrentes, '__len__') and len(concorrentes) == 0:
                            is_empty_or_none = True
                        else:
                            is_empty_or_none = pd.isna(concorrentes) if not isinstance(concorrentes, (str, dict)) else False
                    except (TypeError, ValueError):
                        is_empty_or_none = False
            except (TypeError, ValueError, AttributeError):
                is_empty_or_none = True
            
            if is_empty_or_none:
                new_row = row.to_dict()
                new_row['concorrentes_codigo'] = None
                new_row['concorrentes_codigo_interno'] = None
                new_row['concorrentes_observacao'] = None
                expanded_concorrentes_rows.append(new_row)
            else:
                # Converte para lista de forma segura
                concorrentes_list = None
                try:
                    # Se já for lista, usa diretamente
                    if isinstance(concorrentes, list):
                        concorrentes_list = concorrentes
                    # Se for array numpy, converte para lista
                    elif isinstance(concorrentes, np.ndarray):
                        concorrentes_list = concorrentes.tolist()
                    # Se for iterável (mas não string nem dict), converte
                    elif hasattr(concorrentes, '__iter__') and not isinstance(concorrentes, (str, dict, bytes)):
                        concorrentes_list = list(concorrentes)
                    else:
                        concorrentes_list = []
                except (TypeError, ValueError, AttributeError):
                    concorrentes_list = []
                
                # Verifica se a lista está vazia de forma segura
                if concorrentes_list is None or len(concorrentes_list) == 0:
                    new_row = row.to_dict()
                    new_row['concorrentes_codigo'] = None
                    new_row['concorrentes_codigo_interno'] = None
                    new_row['concorrentes_observacao'] = None
                    expanded_concorrentes_rows.append(new_row)
                else:
                    # Cria uma linha para cada concorrente
                    for concorrente in concorrentes_list:
                        new_row = row.to_dict()
                        if isinstance(concorrente, dict):
                            new_row['concorrentes_codigo'] = concorrente.get('nCodConc')
                            new_row['concorrentes_codigo_interno'] = concorrente.get('cCodIntConc')
                            new_row['concorrentes_observacao'] = concorrente.get('cObservacao')
                        else:
                            new_row['concorrentes_codigo'] = None
                            new_row['concorrentes_codigo_interno'] = None
                            new_row['concorrentes_observacao'] = None
                        expanded_concorrentes_rows.append(new_row)
        
        df = pd.DataFrame(expanded_concorrentes_rows)
        
        # Remove a coluna original 'concorrentes' se ainda existir
        if 'concorrentes' in df.columns:
            df = df.drop(columns=['concorrentes'])
        
        # Remove também colunas expandidas pelo json_normalize relacionadas a concorrentes
        concorrentes_cols_to_remove = [col for col in df.columns if col.startswith('concorrentes.')]
        if concorrentes_cols_to_remove:
            df = df.drop(columns=concorrentes_cols_to_remove)
        
        print(f"Coluna 'concorrentes' normalizada. Total de linhas: {len(df)}")
    elif concorrentes_cols:
        # Se já vier expandida pelo json_normalize (ex: concorrentes.0.nCodConc)
        print(f"Colunas de concorrentes já expandidas encontradas: {concorrentes_cols}")
        print("Renomeando colunas de concorrentes...")
        # Renomeia as colunas expandidas
        for col in concorrentes_cols:
            if '.nCodConc' in col:
                df = df.rename(columns={col: 'concorrentes_codigo'})
            elif '.cCodIntConc' in col:
                df = df.rename(columns={col: 'concorrentes_codigo_interno'})
            elif '.cObservacao' in col:
                df = df.rename(columns={col: 'concorrentes_observacao'})
        print("Colunas de concorrentes renomeadas.")
    else:
        print("Coluna 'concorrentes' não encontrada no DataFrame.")

    df['dt_previsao'] = pd.to_datetime(df['ano_previsao'].astype(str) + '-' + df['mes_previsao'].astype(str) + '-01')
    
    expanded_rows = []
    for _, row in df.iterrows():
        datas_recorrencia = pd.date_range(start=row['dt_previsao'], periods=row['meses_ticket'], freq='MS')
        for data in datas_recorrencia:
            new_row = row.to_dict()  # Copia os valores originais
            new_row['dt_recorrencia'] = data  # Adiciona a data da recorrência
            expanded_rows.append(new_row)

    # Criando o novo DataFrame expandido
    df = pd.DataFrame(expanded_rows)
    
    
    # print(df)

    return df

# Função para salvar os dados no Google Cloud Storage
def save_to_gcs():
    try:
        # Processar os dados
        df = process_data()

        # Salvar diretamente no GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(GCS_DESTINATION_BLOB_NAME)

        # Converte o DataFrame para CSV no formato de bytes
        csv_data = df.to_csv(index=False, encoding="utf-8", sep=";")
        blob.upload_from_string(csv_data, content_type="text/csv")

        print(f"Arquivo enviado para o GCS: gs://{GCS_BUCKET_NAME}/{GCS_DESTINATION_BLOB_NAME}")

    except Exception as e:
        print(f"Erro ao processar e salvar os dados: {e}")

# Executa o script
if __name__ == "__main__":
    save_to_gcs()
