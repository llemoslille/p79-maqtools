import requests
import pandas as pd
import time
import os
import pathlib
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from configuracoes.setup_logging import get_logger

# Configurar logging
logger = get_logger("raw_pedido_vendas.py")

# Configurações da API Omie
BASE_URL = "https://app.omie.com.br/api/v1/produtos/pedido/"

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
DESTINATION_FOLDER = "pedidos/"  # Pasta dentro do bucket

# Configura o ambiente com as credenciais do Google Cloud
credentials = service_account.Credentials.from_service_account_file(
    GCP_CREDENTIALS_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Função para chamar a API Omie


def fetch_data(page, retry=3):
    payload = {
        "call": "ListarPedidos",
        "app_key": APP_KEY,
        "app_secret": APP_SECRET,
        "param": [
            {
                "pagina": page,
                "registros_por_pagina": 200,  # Ajuste conforme necessário
                "apenas_importado_api": "N"
                # ,"numero_pedido_de": 29883,
                # "numero_pedido_ate": 29883
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
    page = 1
    total_requests = 0  # Contador de requisições
    max_requests = 200  # Limite de requisições para teste

    while True:
        try:
            # print(f"Buscando dados da página {page}...")
            data = fetch_data(page)

            nr_pagina = data.get("pagina", 0)
            total_de_paginas = data.get("total_de_paginas", 0)
            registros = data.get("registros", 0)
            total_de_registros = data.get("registros", 0)

            logger.info(f"Página {page} de {total_de_paginas}")

            # Verifica se a resposta contém pedidos
            if "pedido_venda_produto" not in data or not data["pedido_venda_produto"]:
                logger.warning(
                    "Nenhum dado adicional encontrado. Finalizando.")
                break

            # Se a resposta for um único pedido, transforma em uma lista
            if isinstance(data["pedido_venda_produto"], dict):
                data["pedido_venda_produto"] = [data["pedido_venda_produto"]]

            # Adiciona os pedidos à lista de registros
            all_records.extend(data["pedido_venda_produto"])
            total_requests += 1  # Incrementa o contador de requisições

            if total_requests >= max_requests:
                logger.warning(
                    "Limite de 200 requisições atingido. Finalizando.")
                break

            if page < total_de_paginas:
                page += 1
            else:
                break
        except Exception as e:
            logger.error(f"Erro ao buscar dados: {e}")
            break

    if not all_records:
        raise Exception("Nenhum dado foi retornado pela API.")

    # Criação do DataFrame
    df = pd.json_normalize(all_records)

    df_final = df

    # Expandir campos aninhados det
    df_exploded = df.explode("det", ignore_index=True)
    itens_normalize = pd.json_normalize(df_exploded["det"])
    df = pd.concat(
        [df_exploded.drop(columns=["det"]), itens_normalize], axis=1)

    # # Print das colunas na vertical
    # print("\nColunas do DataFrame:")
    # for coluna in df.columns:
    #     print(f"- {coluna}")
    # Expandir campos aninhado departamentos
    df_exploded = df.explode("departamentos", ignore_index=True)
    itens_normalize = pd.json_normalize(df_exploded["departamentos"])
    df = pd.concat(
        [df_exploded.drop(columns=["departamentos"]), itens_normalize], axis=1)

    # Expandir campos aninhado lista_parcelas.parcela
    df_exploded = df.explode("lista_parcelas.parcela", ignore_index=True)
    itens_normalize = pd.json_normalize(df_exploded["lista_parcelas.parcela"])
    df = pd.concat(
        [df_exploded.drop(columns=["lista_parcelas.parcela"]), itens_normalize], axis=1)

    # Renomeação de colunas (ajuste conforme a estrutura da API)
    rename_columns = {
        "cabecalho.bloqueado": "fl_bloqueado",
        "cabecalho.codigo_cliente": "cod_cliente",
        "cabecalho.codigo_empresa": "cod_empresa",
        "cabecalho.codigo_parcela": "cod_parcela",
        "cabecalho.codigo_pedido": "cod_pedido",
        "cabecalho.data_previsao": "dt_previsao",
        "cabecalho.enc_data": "dt_encerrado",
        "cabecalho.enc_motivo": "de_motivo_encerrado",
        "cabecalho.encerrado": "fl_encerrado",
        "cabecalho.etapa": "cod_etapa_faturamento",
        "cabecalho.numero_pedido": "nr_pedido",
        "cabecalho.origem_pedido": "origem_pedido",
        "cabecalho.qtde_parcelas": "qtd_parcelas",
        "cabecalho.quantidade_itens": "qtd_itens",
        "frete.modalidade": "cod_frete",
        "frete.valor_frete": "vlr_frete",
        "infoCadastro.autorizado": "fl_autorizado",
        "infoCadastro.cancelado": "fl_cancelado",
        "infoCadastro.dAlt": "dt_alteracao_pedido",
        "infoCadastro.dInc": "dt_pedido",
        "infoCadastro.faturado": "fl_faturado",
        "informacoes_adicionais.codVend": "cod_vendedor",
        "informacoes_adicionais.codigo_categoria": "cod_categoria",
        "informacoes_adicionais.codigo_conta_corrente": "cod_cc",
        "informacoes_adicionais.numero_pedido_cliente": "nr_pedido_cliente",
        "total_pedido.base_calculo_icms": "total_base_calculo_icms",
        "total_pedido.base_calculo_st": "total_base_calculo_st",
        "total_pedido.valor_IPI": "total_vlr_ipi",
        "total_pedido.valor_cofins": "total_vlr_cofins",
        "total_pedido.valor_csll": "total_vlr_csll",
        "total_pedido.valor_deducoes": "total_vlr_deducoes",
        "total_pedido.valor_descontos": "total_vlr_descontos",
        "total_pedido.valor_icms": "total_vlr_icms",
        "total_pedido.valor_inss": "total_vlr_inss",
        "total_pedido.valor_mercadorias": "total_vlr_produtos",
        "total_pedido.valor_pis": "total_vlr_pis",
        "total_pedido.valor_st": "total_vlr_st",
        "total_pedido.valor_total_pedido": "total_vlr_pedido",
        "cabecalho.sequencial": "nr_sequencial_pedido",
        "infoCadastro.dFat": "dt_faturamento",
        "data_vencimento": "dt_vencimento",
        "meio_pagamento": "cod_meio_pagamento",
        "numero_parcela": "nr_parcela",
        "percentual": "percentual",
        "quantidade_dias": "qtd_dias_vencimento",
        "tipo_documento": "tp_documento",
        "valor": "vlr_total",
        "produto.cfop": "produto_cfop",
        "produto.cnpj_fabricante": "produto_cnpj_fabricante",
        "produto.codigo": "produto_codigo",
        "produto.codigo_produto": "produto_codigo_produto",
        "produto.codigo_tabela_preco": "produto_codigo_tabela_preco",
        "produto.descricao": "produto_descricao",
        "produto.ean": "produto_ean",
        "produto.indicador_escala": "produto_indicador_escala",
        "produto.motivo_icms_desonerado": "produto_motivo_icms_desonerado",
        "produto.ncm": "produto_ncm",
        "produto.percentual_desconto": "produto_percentual_desconto",
        "produto.quantidade": "produto_quantidade",
        "produto.reservado": "produto_reservado",
        "produto.tipo_desconto": "produto_tipo_desconto",
        "produto.unidade": "produto_unidade",
        "produto.valor_deducao": "produto_valor_deducao",
        "produto.valor_desconto": "produto_valor_desconto",
        "produto.valor_icms_desonerado": "produto_valor_icms_desonerado",
        "produto.valor_mercadoria": "produto_valor_mercadoria",
        "produto.valor_total": "produto_valor_total",
        "produto.valor_unitario": "produto_valor_unitario",
    }

    df_final = df.rename(columns=rename_columns)

    df_final = df_final[[
        "cod_pedido",
        "cod_etapa_faturamento",
        "fl_faturado",
        "fl_bloqueado",
        "fl_encerrado",
        "fl_autorizado",
        "fl_cancelado",
        "cod_cliente",
        "cod_empresa",
        "cod_parcela",
        "dt_previsao",
        "dt_encerrado",
        "de_motivo_encerrado",
        "nr_pedido",
        "origem_pedido",
        "qtd_parcelas",
        "qtd_itens",
        "cod_frete",
        "vlr_frete",
        "dt_alteracao_pedido",
        "dt_pedido",
        "cod_vendedor",
        "cod_categoria",
        "cod_cc",
        "nr_pedido_cliente",
        "total_base_calculo_icms",
        "total_base_calculo_st",
        "total_vlr_ipi",
        "total_vlr_cofins",
        "total_vlr_csll",
        "total_vlr_deducoes",
        "total_vlr_descontos",
        "total_vlr_icms",
        "total_vlr_inss",
        "total_vlr_produtos",
        "total_vlr_pis",
        "total_vlr_st",
        "total_vlr_pedido",
        "nr_sequencial_pedido",
        "dt_faturamento",
        "dt_vencimento",
        "cod_meio_pagamento",
        "nr_parcela",
        "percentual",
        "qtd_dias_vencimento",
        "tp_documento",
        "vlr_total",
        "produto_cfop",
        "produto_cnpj_fabricante",
        "produto_codigo",
        "produto_codigo_produto",
        "produto_codigo_tabela_preco",
        "produto_descricao",
        "produto_ean",
        "produto_indicador_escala",
        "produto_motivo_icms_desonerado",
        "produto_ncm",
        "produto_percentual_desconto",
        "produto_quantidade",
        "produto_reservado",
        "produto_tipo_desconto",
        "produto_unidade",
        "produto_valor_deducao",
        "produto_valor_desconto",
        "produto_valor_icms_desonerado",
        "produto_valor_mercadoria",
        "produto_valor_total",
        "produto_valor_unitario",
    ]]

    # pasta_saida = pathlib.Path("csv")
    # pasta_saida.mkdir(parents=True, exist_ok=True)
    # caminho_csv = os.path.join(pasta_saida, "pedidos_venda_29883.csv")
    # df_final.to_csv(caminho_csv, index=False, encoding='windows-1252', sep=';')

    return df_final


def save_to_gcs(df):
    try:
        # Inicializa o cliente do GCS
        storage_client = storage.Client(
            credentials=credentials, project=PROJECT_ID)

        # Cria o caminho completo do arquivo no GCS
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        destination_blob_name = f"{DESTINATION_FOLDER}pedidos_vendas.csv"

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
