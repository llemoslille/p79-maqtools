import pandas as pd
import json
import os
import tempfile
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from novo_projeto.config import GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_PATH
from gcs_utils import upload_to_gcs

# Configurações
GCS_BRONZE_PREFIX = "bronze/nota_fiscal/"
ARQUIVO_SAIDA = "maq_prod_estoque/silver/nota_fiscal/silver_notas_fiscais.parquet"
os.makedirs('maq_prod_estoque/silver/nota_fiscal', exist_ok=True)

# Configurações de otimização
MAX_WORKERS = 5  # Número de threads para download paralelo
BATCH_SIZE = 50  # Processa arquivos em lotes de 50
CONSOLIDATE_THRESHOLD = 100  # Se mais de 100 arquivos, consolida em lotes


def listar_arquivos_gcs(bucket_name, prefix):
    """Lista todos os arquivos no GCS com o prefixo especificado"""
    try:
        client = storage.Client.from_service_account_json(
            json_credentials_path=GCS_CREDENTIALS_PATH,
            project=GCS_PROJECT_ID
        )
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)

        arquivos = []
        for blob in blobs:
            if not blob.name.endswith('/') and blob.name.endswith('.parquet'):
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
        return True, None
    except Exception as e:
        return False, str(e)


def processar_lote_arquivos(arquivos_lote, bucket_name, lote_num):
    """Processa um lote de arquivos em paralelo"""
    print(
        f"  📦 Processando lote {lote_num} ({len(arquivos_lote)} arquivos)...")

    dfs_lote = []
    arquivos_temp = []

    def download_e_carregar(arquivo_gcs):
        try:
            # Cria arquivo temporário
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
                arquivo_temp = temp_file.name

            # Baixa arquivo do GCS
            sucesso, erro = baixar_arquivo_gcs(
                bucket_name, arquivo_gcs, arquivo_temp)
            if sucesso:
                df_temp = pd.read_parquet(arquivo_temp)
                return df_temp, arquivo_temp, None
            else:
                return None, arquivo_temp, erro
        except Exception as e:
            return None, None, str(e)

    # Download paralelo
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_arquivo = {
            executor.submit(download_e_carregar, arquivo): arquivo
            for arquivo in arquivos_lote
        }

        for future in as_completed(future_to_arquivo):
            arquivo_original = future_to_arquivo[future]
            try:
                df_temp, arquivo_temp, erro = future.result()
                if df_temp is not None:
                    dfs_lote.append(df_temp)
                    if arquivo_temp:
                        arquivos_temp.append(arquivo_temp)
                    print(
                        f"    ✅ {arquivo_original} - {len(df_temp)} registros")
                else:
                    print(f"    ❌ {arquivo_original} - Erro: {erro}")
            except Exception as e:
                print(f"    ❌ {arquivo_original} - Exceção: {e}")

    # Limpa arquivos temporários
    for arquivo_temp in arquivos_temp:
        try:
            os.unlink(arquivo_temp)
        except:
            pass

    # Concatena DataFrames do lote
    if dfs_lote:
        df_lote = pd.concat(dfs_lote, ignore_index=True)
        print(f"  📊 Lote {lote_num} consolidado: {len(df_lote)} registros")
        return df_lote
    else:
        print(f"  ⚠️  Lote {lote_num} sem dados válidos")
        return None


def expandir_coluna_json(df, coluna, prefixo):
    """Expande uma coluna JSON em múltiplas colunas (versão otimizada)"""
    print(f"  🔄 Expandindo coluna: {coluna}")

    # Converte string JSON para lista de dicionários
    df[coluna] = df[coluna].apply(lambda x: json.loads(
        x) if pd.notna(x) and x != '' else [])

    # Explode a lista
    df_exploded = df.explode(coluna)
    print(f"    📈 Registros após explode: {len(df_exploded)}")

    # Normaliza os dicionários da coluna expandida
    if not df_exploded.empty and df_exploded[coluna].notna().any():
        df_valid = df_exploded[df_exploded[coluna].notna()]
        if not df_valid.empty:
            print(f"    🔧 Normalizando {len(df_valid)} registros válidos...")

            # Normaliza os dicionários
            normalized = pd.json_normalize(df_valid[coluna])
            print(f"    📋 Colunas normalizadas: {list(normalized.columns)}")

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
                print(f"    ➕ Adicionando {len(df_null)} registros nulos")
                for col in normalized.columns:
                    df_null[col] = None
                df_result = pd.concat([df_result, df_null], ignore_index=True)

            print(f"    ✅ Resultado final: {len(df_result)} registros")
            return df_result

    # Se não há dados válidos, retorna o DataFrame original sem a coluna
    print(f"    ⚠️  Nenhum dado válido, removendo coluna {coluna}")
    return df_exploded.drop(columns=[coluna])


def main():
    print("Iniciando processamento SILVER otimizado - Notas Fiscais")
    start_time = time.time()

    # Lista todos os arquivos parquet no GCS
    print(
        f"Buscando arquivos em: gs://{GCS_BUCKET_NAME}/{GCS_BRONZE_PREFIX}")
    arquivos_gcs = listar_arquivos_gcs(GCS_BUCKET_NAME, GCS_BRONZE_PREFIX)

    if not arquivos_gcs:
        print(
            f"❌ Nenhum arquivo parquet encontrado no GCS em: {GCS_BRONZE_PREFIX}")
        return

    print(f"📊 Encontrados {len(arquivos_gcs)} arquivos para processar")

    # Decide se deve processar em lotes ou consolidar
    if len(arquivos_gcs) > CONSOLIDATE_THRESHOLD:
        print(
            f"⚡ Muitos arquivos ({len(arquivos_gcs)}) - usando processamento em lotes")

        # Divide arquivos em lotes
        lotes = [arquivos_gcs[i:i + BATCH_SIZE]
                 for i in range(0, len(arquivos_gcs), BATCH_SIZE)]
        print(f"📦 Criados {len(lotes)} lotes de até {BATCH_SIZE} arquivos")

        # Processa cada lote
        dfs_finais = []
        for i, lote in enumerate(lotes, 1):
            df_lote = processar_lote_arquivos(lote, GCS_BUCKET_NAME, i)
            if df_lote is not None:
                dfs_finais.append(df_lote)

            # Mostra progresso
            progresso = (i / len(lotes)) * 100
            print(f"📈 Progresso: {progresso:.1f}% ({i}/{len(lotes)} lotes)")

        # Concatena todos os lotes
        if dfs_finais:
            print("🔗 Consolidando todos os lotes...")
            df = pd.concat(dfs_finais, ignore_index=True)
        else:
            print("❌ Nenhum lote foi processado com sucesso")
            return
    else:
        print("📁 Poucos arquivos - processamento direto")
        # Processa todos os arquivos de uma vez (método original otimizado)
        dfs = []
        arquivos_temp = []

        for i, arquivo_gcs in enumerate(arquivos_gcs, 1):
            print(f"  📥 Baixando {i}/{len(arquivos_gcs)}: {arquivo_gcs}")

            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
                arquivo_temp = temp_file.name

            if baixar_arquivo_gcs(GCS_BUCKET_NAME, arquivo_gcs, arquivo_temp)[0]:
                df_temp = pd.read_parquet(arquivo_temp)
                dfs.append(df_temp)
                arquivos_temp.append(arquivo_temp)
                print(f"    ✅ {len(df_temp)} registros")
            else:
                print(f"    ❌ Falha no download")

        # Remove arquivos temporários
        for arquivo_temp in arquivos_temp:
            try:
                os.unlink(arquivo_temp)
            except:
                pass

        if not dfs:
            print("❌ Nenhum arquivo foi carregado com sucesso")
            return

        df = pd.concat(dfs, ignore_index=True)

    print(f"📊 Total de registros após consolidação: {len(df):,}")

    # Mostra as colunas originais
    print(f"📋 Colunas originais: {len(df.columns)}")

    # Verifica se existem as colunas JSON para expandir
    colunas_json = ['det', 'titulos']
    colunas_existentes = [col for col in colunas_json if col in df.columns]

    if not colunas_existentes:
        print("⚠️  Nenhuma coluna JSON encontrada para expandir")
    else:
        print(f"🔧 Colunas JSON encontradas: {colunas_existentes}")

        # Expande as colunas JSON encontradas
        for coluna in colunas_existentes:
            if coluna == 'det':
                print("🔄 Expandindo coluna 'det' (detalhes dos produtos)...")
                df = expandir_coluna_json(df, 'det', 'det')
            elif coluna == 'titulos':
                print("🔄 Expandindo coluna 'titulos'...")
                df = expandir_coluna_json(df, 'titulos', 'titulo')

    # Normalização adicional: converte colunas JSON restantes para string
    print("🔧 Normalizando colunas JSON restantes...")
    for col in df.columns:
        if df[col].dtype == 'object':
            amostra = df[col].dropna().head(10)
            if len(amostra) > 0:
                if amostra.astype(str).str.match(r'^[\{\[]').any():
                    print(f"  🔄 Normalizando coluna JSON: {col}")
                    df[col] = df[col].apply(lambda x: json.dumps(
                        x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)

    # Mostra informações finais
    print(f"📊 Shape final: {df.shape}")
    print(f"📋 Colunas finais: {len(df.columns)}")

    # Salva o arquivo silver
    print("💾 Salvando arquivo SILVER...")
    df.to_parquet(ARQUIVO_SAIDA, index=False)
    print(f"✅ Arquivo SILVER gerado: {ARQUIVO_SAIDA}")

    # Estatísticas de tempo
    tempo_total = time.time() - start_time
    print(
        f"⏱️  Tempo total de processamento: {tempo_total:.2f} segundos ({tempo_total/60:.1f} minutos)")
    print(f"📈 Performance: {len(df)/tempo_total:.0f} registros/segundo")

    # Mostra amostra dos dados
    print("📋 Amostra dos dados:")
    print(df.head())


if __name__ == "__main__":
    main()
