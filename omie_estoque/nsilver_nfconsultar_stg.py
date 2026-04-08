import pandas as pd
import json
import os
import glob
import tempfile
import re
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from novo_projeto.config import GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_PATH
from gcs_utils import upload_to_gcs

# Configurações
GCS_BRONZE_PREFIX = "bronze/nota_fiscal/"
ARQUIVO_SAIDA = "maq_prod_estoque/silver/nota_fiscal/silver_notas_fiscais_stg.parquet"
os.makedirs('maq_prod_estoque/silver/nota_fiscal', exist_ok=True)

# Configurações de otimização
MAX_WORKERS = 3  # Número de threads para download paralelo (reduzido)
BATCH_SIZE = 20  # Processa arquivos em lotes de 20 (reduzido)
# Se mais de 50 arquivos, consolida em lotes (reduzido)
# Se mais de 20 arquivos, consolida em lotes (otimizado para poucos arquivos)
CONSOLIDATE_THRESHOLD = 20


def listar_arquivos_gcs(bucket_name, prefix):
    """Lista todos os arquivos no GCS com o prefixo especificado (apenas da raiz)"""
    try:
        client = storage.Client.from_service_account_json(
            json_credentials_path=GCS_CREDENTIALS_PATH,
            project=GCS_PROJECT_ID
        )
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)

        arquivos = []
        for blob in blobs:
            # Ignora "pastas" (blobs que terminam com /)
            if not blob.name.endswith('/') and blob.name.endswith('.parquet'):
                # Filtra apenas arquivos da raiz (não em subpastas)
                # bronze/nota_fiscal/arquivo.parquet (3 partes)
                # bronze/nota_fiscal/processados/ano/mes/arquivo.parquet (5 partes)
                partes_caminho = blob.name.split('/')
                if len(partes_caminho) == 3:  # Apenas arquivos na raiz
                    arquivos.append(blob.name)

        return arquivos
    except Exception as e:
        print(f"[ERRO] Erro ao listar arquivos GCS: {e}")
        return []


def baixar_arquivo_gcs(bucket_name, source_blob_name, destination_file_name):
    """Baixa um arquivo do GCS para local temporário"""
    try:
        client = storage.Client.from_service_account_json(
            json_credentials_path=GCS_CREDENTIALS_PATH,
            project=GCS_PROJECT_ID
        )
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)

        blob.download_to_filename(destination_file_name)
        print(
            f"[GCS] Arquivo baixado: {source_blob_name} -> {destination_file_name}")
        return True
    except Exception as e:
        print(f"[ERRO] Erro ao baixar {source_blob_name}: {e}")
        return False


def baixar_arquivo_paralelo(args):
    """Função para download paralelo de arquivos"""
    bucket_name, source_blob_name, destination_file_name = args
    return baixar_arquivo_gcs(bucket_name, source_blob_name, destination_file_name)


def processar_lote_arquivos(lote_arquivos, bucket_name, numero_lote):
    """Processa um lote de arquivos em paralelo"""
    print(
        f"[LOTE] Processando lote {numero_lote} com {len(lote_arquivos)} arquivos...")

    # Prepara argumentos para download paralelo
    args_download = []
    arquivos_temp = []

    for arquivo_gcs in lote_arquivos:
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            arquivo_temp = temp_file.name
        args_download.append((bucket_name, arquivo_gcs, arquivo_temp))
        arquivos_temp.append(arquivo_temp)

    # Download paralelo
    dfs_lote = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submete todos os downloads
        future_to_args = {executor.submit(
            baixar_arquivo_paralelo, args): args for args in args_download}

        # Processa resultados conforme completam
        for future in as_completed(future_to_args):
            args = future_to_args[future]
            arquivo_gcs, arquivo_temp = args[1], args[2]

            try:
                sucesso = future.result()
                if sucesso:
                    print(f"[OK] Baixado: {os.path.basename(arquivo_gcs)}")
                    df_temp = pd.read_parquet(arquivo_temp)
                    dfs_lote.append(df_temp)
                else:
                    print(f"[ERRO] Falha: {os.path.basename(arquivo_gcs)}")
            except Exception as e:
                print(f"[ERRO] Erro: {os.path.basename(arquivo_gcs)} - {e}")

    # Remove arquivos temporários
    for arquivo_temp in arquivos_temp:
        try:
            os.unlink(arquivo_temp)
        except Exception as e:
            print(
                f"[AVISO] Erro ao remover arquivo temporário {arquivo_temp}: {e}")

    # Concatena DataFrames do lote
    if dfs_lote:
        df_lote = pd.concat(dfs_lote, ignore_index=True)
        print(
            f"[INFO] Lote {numero_lote} processado: {len(df_lote)} registros")
        return df_lote
    else:
        print(
            f"[AVISO] Lote {numero_lote} não teve arquivos processados com sucesso")
        return None


def mover_arquivo_processado(bucket_name, source_blob_name):
    """Move arquivo processado para pasta organizada por ano/mês"""
    try:
        client = storage.Client.from_service_account_json(
            json_credentials_path=GCS_CREDENTIALS_PATH,
            project=GCS_PROJECT_ID
        )
        bucket = client.bucket(bucket_name)

        # Extrai data do nome do arquivo (formato: notas_fiscais_YYYYMMDD.parquet)
        match = re.search(r'notas_fiscais_(\d{8})\.parquet', source_blob_name)
        if not match:
            print(
                f"[AVISO] Não foi possível extrair data do arquivo: {source_blob_name}")
            return False

        data_str = match.group(1)
        ano = data_str[:4]
        mes = data_str[4:6]

        # Define novo caminho: bronze/nota_fiscal/processados/ANO/MES/
        novo_caminho = f"bronze/nota_fiscal/processados/{ano}/{mes}/{os.path.basename(source_blob_name)}"

        # Copia arquivo para novo local
        source_blob = bucket.blob(source_blob_name)
        destination_blob = bucket.blob(novo_caminho)

        # Copia o arquivo
        destination_blob.upload_from_string(source_blob.download_as_bytes())

        # Remove arquivo original
        source_blob.delete()

        print(f"[GCS] Arquivo movido: {source_blob_name} -> {novo_caminho}")
        return True

    except Exception as e:
        print(f"[ERRO] Erro ao mover arquivo {source_blob_name}: {e}")
        return False


def expandir_coluna_json(df, coluna, prefixo):
    """Expande uma coluna JSON em múltiplas colunas"""
    print(f"  Processando coluna: {coluna}")

    # Converte string JSON para lista de dicionários
    df[coluna] = df[coluna].apply(lambda x: json.loads(
        x) if pd.notna(x) and x != '' else [])

    # Explode a lista
    df_exploded = df.explode(coluna)
    print(f"  Registros após explode: {len(df_exploded)}")

    # Normaliza os dicionários da coluna expandida
    if not df_exploded.empty and df_exploded[coluna].notna().any():
        # Filtra apenas registros não nulos
        df_valid = df_exploded[df_exploded[coluna].notna()]
        if not df_valid.empty:
            print(f"  Registros válidos para normalizar: {len(df_valid)}")

            # Normaliza os dicionários
            normalized = pd.json_normalize(df_valid[coluna])
            print(f"  Colunas normalizadas: {list(normalized.columns)}")

            # Adiciona prefixo às colunas
            normalized.columns = [
                f"{prefixo}_{col}" for col in normalized.columns]

            # Concatena com o DataFrame original
            df_result = pd.concat([df_valid.reset_index(
                drop=True), normalized.reset_index(drop=True)], axis=1)

            # Remove a coluna original
            df_result = df_result.drop(columns=[coluna])

            # Adiciona registros que não tinham dados na coluna (valores nulos)
            df_null = df_exploded[df_exploded[coluna].isna()].drop(columns=[
                coluna])
            if not df_null.empty:
                print(f"  Adicionando {len(df_null)} registros nulos")
                # Adiciona colunas vazias para manter consistência
                for col in normalized.columns:
                    df_null[col] = None
                df_result = pd.concat([df_result, df_null], ignore_index=True)

            print(f"  Resultado final: {len(df_result)} registros")
            return df_result

    # Se não há dados válidos, retorna o DataFrame original sem a coluna
    print(f"  Nenhum dado válido encontrado, removendo coluna {coluna}")
    return df_exploded.drop(columns=[coluna])


def main():
    print("[INFO] Iniciando processamento SILVER otimizado - Notas Fiscais (Staging)")
    start_time = time.time()

    # Lista todos os arquivos parquet no GCS
    print(
        f"[GCS] Buscando arquivos em: gs://{GCS_BUCKET_NAME}/{GCS_BRONZE_PREFIX}")
    arquivos_gcs = listar_arquivos_gcs(GCS_BUCKET_NAME, GCS_BRONZE_PREFIX)

    if not arquivos_gcs:
        print(
            f"[ERRO] Nenhum arquivo parquet encontrado no GCS em: {GCS_BRONZE_PREFIX}")
        return

    print(f"[INFO] Encontrados {len(arquivos_gcs)} arquivos para processar")
    print(f"[INFO] Threshold para lotes: {CONSOLIDATE_THRESHOLD} arquivos")

    # Decide se deve processar em lotes ou consolidar
    if len(arquivos_gcs) > CONSOLIDATE_THRESHOLD:
        print(
            f"[INFO] Muitos arquivos ({len(arquivos_gcs)}) - usando processamento em lotes")

        # Divide arquivos em lotes
        lotes = [arquivos_gcs[i:i + BATCH_SIZE]
                 for i in range(0, len(arquivos_gcs), BATCH_SIZE)]
        print(
            f"[INFO] Criados {len(lotes)} lotes de até {BATCH_SIZE} arquivos")

        # Processa cada lote
        dfs_finais = []
        for i, lote in enumerate(lotes, 1):
            df_lote = processar_lote_arquivos(lote, GCS_BUCKET_NAME, i)
            if df_lote is not None:
                dfs_finais.append(df_lote)

            # Mostra progresso
            progresso = (i / len(lotes)) * 100
            print(f"[PROGRESSO] {progresso:.1f}% ({i}/{len(lotes)} lotes)")

        # Concatena todos os lotes
        if dfs_finais:
            print("[INFO] Consolidando todos os lotes...")
            df = pd.concat(dfs_finais, ignore_index=True)
        else:
            print("[ERRO] Nenhum lote foi processado com sucesso")
            return
    else:
        print(
            f"[INFO] Poucos arquivos ({len(arquivos_gcs)}) - usando processamento direto")

        # Processamento direto para poucos arquivos
        dfs = []
        arquivos_temp = []

        for arquivo_gcs in arquivos_gcs:
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
                arquivo_temp = temp_file.name

            if baixar_arquivo_gcs(GCS_BUCKET_NAME, arquivo_gcs, arquivo_temp):
                print(f"[OK] Baixado: {os.path.basename(arquivo_gcs)}")
                df_temp = pd.read_parquet(arquivo_temp)
                dfs.append(df_temp)
                arquivos_temp.append(arquivo_temp)
            else:
                print(f"[ERRO] Falha: {os.path.basename(arquivo_gcs)}")

        # Remove arquivos temporários
        for arquivo_temp in arquivos_temp:
            try:
                os.unlink(arquivo_temp)
            except Exception as e:
                print(
                    f"[AVISO] Erro ao remover arquivo temporário {arquivo_temp}: {e}")

        if dfs:
            df = pd.concat(dfs, ignore_index=True)
        else:
            print("[ERRO] Nenhum arquivo foi processado com sucesso")
            return

    print(f"[INFO] Total de registros após consolidação: {len(df):,}")

    # Mostra as colunas originais
    print(f"[INFO] Colunas originais: {len(df.columns)}")

    # Verifica se existem as colunas JSON para expandir
    colunas_json = ['det', 'titulos']
    colunas_existentes = [col for col in colunas_json if col in df.columns]

    if not colunas_existentes:
        print("[AVISO] Nenhuma coluna JSON encontrada para expandir")
    else:
        print(f"[INFO] Colunas JSON encontradas: {colunas_existentes}")

        # Expande as colunas JSON encontradas
        for coluna in colunas_existentes:
            if coluna == 'det':
                print("[INFO] Expandindo coluna 'det' (detalhes dos produtos)...")
                df = expandir_coluna_json(df, 'det', 'det')
            elif coluna == 'titulos':
                print("[INFO] Expandindo coluna 'titulos'...")
                df = expandir_coluna_json(df, 'titulos', 'titulo')

    # Normalização adicional: converte colunas JSON restantes para string
    print("[INFO] Normalizando colunas JSON restantes...")
    for col in df.columns:
        if df[col].dtype == 'object':
            amostra = df[col].dropna().head(10)
            if len(amostra) > 0:
                if amostra.astype(str).str.match(r'^[\{\[]').any():
                    print(f"  [INFO] Normalizando coluna JSON: {col}")
                    df[col] = df[col].apply(lambda x: json.dumps(
                        x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)

    # Mostra as colunas após expansão
    print(f"[INFO] Colunas após expansão: {len(df.columns)}")

    # Mostra informações do DataFrame
    print(f"[INFO] Shape final: {df.shape}")
    print(f"[INFO] Tipos de dados:")
    for col, dtype in df.dtypes.items():
        print(f"  - {col}: {dtype}")

    # Cria diretório se não existir
    os.makedirs(os.path.dirname(ARQUIVO_SAIDA), exist_ok=True)

    # Salva o arquivo silver
    df.to_parquet(ARQUIVO_SAIDA, index=False)
    print(f"[OK] Arquivo SILVER STAGING gerado: {ARQUIVO_SAIDA}")

    # Mostra amostra dos dados
    print("[INFO] Amostra dos dados:")
    print(df.head())

    # Move arquivos processados para pasta organizada por ano/mês
    print("\n[INFO] Movendo arquivos processados para pasta organizada...")
    arquivos_movidos = 0
    for arquivo_gcs in arquivos_gcs:
        if mover_arquivo_processado(GCS_BUCKET_NAME, arquivo_gcs):
            arquivos_movidos += 1

    print(
        f"[OK] {arquivos_movidos} arquivos movidos para bronze/nota_fiscal/processados/ANO/MES/")

    # Mostra tempo de execução
    end_time = time.time()
    tempo_execucao = end_time - start_time
    print(f"\n[INFO] Tempo total de execução: {tempo_execucao:.2f} segundos")
    print(f"[SUCESSO] Processamento SILVER STAGING otimizado concluído com sucesso!")


if __name__ == "__main__":
    main()
