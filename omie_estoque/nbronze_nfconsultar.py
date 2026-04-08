import requests
import pandas as pd
import os
from novo_projeto.config import APP_KEY, APP_SECRET, GCS_BUCKET_NAME
import json
from datetime import datetime, timedelta
import time
from gcs_utils import upload_to_gcs
from logger_config import obter_logger

ENDPOINT = 'https://app.omie.com.br/api/v1/produtos/nfconsultar/'
DIRETORIO_SAIDA = 'maq_prod_estoque/bronze/nota_fiscal'
os.makedirs(DIRETORIO_SAIDA, exist_ok=True)

# Configurar logging
logger = obter_logger(__name__)


def fazer_requisicao_com_retry(payload, max_tentativas=3):
    """Faz requisição com retry automático em caso de timeout"""
    for tentativa in range(max_tentativas):
        try:
            resp = requests.post(ENDPOINT, json=payload, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as e:
            logger.warning(
                f"Timeout na tentativa {tentativa + 1}/{max_tentativas}: {e}")
            if tentativa < max_tentativas - 1:
                # Backoff exponencial: 5s, 10s, 20s
                tempo_espera = (2 ** tentativa) * 5
                logger.info(
                    f"Aguardando {tempo_espera} segundos antes da próxima tentativa...")
                time.sleep(tempo_espera)
            else:
                logger.error(f"Falha após {max_tentativas} tentativas")
                raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na requisição: {e}")
            raise


def listar_nf(data_inicial, data_final):
    pagina = 1
    resultados = []

    while True:
        payload = {
            "call": "ListarNF",
            "param": [{
                "pagina": pagina,
                "registros_por_pagina": 100,
                "apenas_importado_api": "N",
                "ordenar_por": "DATA_LANCAMENTO",
                "dEmiInicial": data_inicial,
                "dEmiFinal": data_final,
                "tpNF": 0
            }],
            "app_key": APP_KEY,
            "app_secret": APP_SECRET
        }

        try:
            resp = fazer_requisicao_com_retry(payload)
            lista = resp.get('nfCadastro', [])

            if not lista:
                break

            resultados.extend(lista)
            logger.info(f"Página {pagina}: {len(lista)} notas encontradas")

            if len(lista) < 100:
                break

            pagina += 1

        except Exception as e:
            logger.error(
                f"Erro ao processar página {pagina} para data {data_inicial}: {e}")
            break

    return resultados


def processar_notas_fiscais(notas, data_str):
    """Processa e salva as notas fiscais de uma data específica"""
    if notas:
        df = pd.json_normalize(notas)
        # Substitui todos os dicionários vazios por None
        df = df.map(lambda x: None if isinstance(
            x, dict) and not x else x)
        # Converte todas as colunas do tipo list ou dict para string JSON
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(lambda x: json.dumps(
                    x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
        # Remove todas as colunas cujo nome começa com 'rastreabilidade'
        cols_rastreabilidade = [
            col for col in df.columns if col.startswith('rastreabilidade')]
        if cols_rastreabilidade:
            logger.info(
                f'Removendo colunas rastreabilidade: {cols_rastreabilidade}')
            df = df.drop(columns=cols_rastreabilidade)
        # Remove todas as colunas onde pelo menos um valor é do tipo list
        cols_list = [col for col in df.columns if df[col].apply(
            lambda x: isinstance(x, list)).any()]
        if cols_list:
            logger.info(f'Removendo colunas do tipo list: {cols_list}')
            df = df.drop(columns=cols_list)

        # Salva o arquivo localmente primeiro
        arquivo_local = f"{DIRETORIO_SAIDA}/notas_fiscais_{data_str}.parquet"
        df.to_parquet(arquivo_local, index=False)

        # Faz upload para o GCS
        gcs_caminho = f"bronze/nota_fiscal/notas_fiscais_{data_str}.parquet"
        sucesso_upload = upload_to_gcs(
            arquivo_local, GCS_BUCKET_NAME, gcs_caminho)

        if sucesso_upload:
            logger.info(
                f"[OK] Arquivo salvo no GCS: gs://{GCS_BUCKET_NAME}/{gcs_caminho} com {len(notas)} notas")
            # Remove arquivo local após upload bem-sucedido
            try:
                os.remove(arquivo_local)
                logger.info(f"Arquivo local removido: {arquivo_local}")
            except Exception as e:
                logger.warning(
                    f"Erro ao remover arquivo local {arquivo_local}: {e}")
        else:
            logger.error(
                f"Falha no upload para GCS. Arquivo mantido localmente: {arquivo_local}")

        return sucesso_upload
    return False


def main():
    # Define o período de busca: de hoje - 5 dias até hoje
    data_inicio = datetime.now() - timedelta(days=5)
    data_fim = datetime.now()

    logger.info(
        f"Buscando notas fiscais de {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}")
    logger.info(
        f"Destino GCS: gs://{GCS_BUCKET_NAME}/bronze/nota_fiscal/")

    total_arquivos_gerados = 0
    total_dias_processados = 0

    # Itera dia por dia
    data_atual = data_inicio
    while data_atual <= data_fim:
        data_str = data_atual.strftime('%d/%m/%Y')
        data_str_arquivo = data_atual.strftime('%Y%m%d')

        logger.info(f"Processando data: {data_str}")

        try:
            # Busca notas fiscais para o dia específico
            notas = listar_nf(data_str, data_str)

            if processar_notas_fiscais(notas, data_str_arquivo):
                total_arquivos_gerados += 1

            total_dias_processados += 1

            # Log de progresso a cada 30 dias
            if total_dias_processados % 30 == 0:
                progresso = ((data_atual - data_inicio).days /
                             (data_fim - data_inicio).days) * 100
                logger.info(
                    f"Progresso: {progresso:.1f}% - {total_dias_processados} dias processados, {total_arquivos_gerados} arquivos gerados")

        except Exception as e:
            logger.error(f"Erro ao processar data {data_str}: {e}")
            # Continua para o próximo dia mesmo com erro

        # Avança para o próximo dia
        data_atual += timedelta(days=1)

    logger.info(
        f"[RESUMO] Total de arquivos salvos no GCS: {total_arquivos_gerados}")
    logger.info(f"Total de dias processados: {total_dias_processados}")
    logger.info(
        f"Período processado: {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}")
    logger.info(
        f"Localização final: gs://{GCS_BUCKET_NAME}/bronze/nota_fiscal/")


if __name__ == '__main__':
    main()
