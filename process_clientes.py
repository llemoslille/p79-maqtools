"""
Processamento de Clientes - Camada Silver
Processa dados brutos da camada Bronze e salva na camada Silver em formato normalizado.
"""
import sys
from pathlib import Path
from typing import Optional
import io

import pandas as pd
import numpy as np
from google.cloud import storage

from src.etl.bronze.extract_clientes import carregar_config
from src.utils.logger import setup_logger

# Função helper para print seguro no Windows (mesma do bronze)
def safe_print(*args, **kwargs):
    """Print que funciona corretamente no Windows com caracteres especiais"""
    try:
        print(*args, **kwargs)
    except UnicodeEncodeError:
        # Se falhar, remove acentos das strings
        safe_args = []
        for arg in args:
            if isinstance(arg, str):
                # Remove acentos básicos
                replacements = {
                    'á': 'a', 'à': 'a', 'ã': 'a', 'â': 'a', 'ä': 'a',
                    'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
                    'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
                    'ó': 'o', 'ò': 'o', 'õ': 'o', 'ô': 'o', 'ö': 'o',
                    'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u',
                    'ç': 'c', 'ñ': 'n',
                    'Á': 'A', 'À': 'A', 'Ã': 'A', 'Â': 'A', 'Ä': 'A',
                    'É': 'E', 'È': 'E', 'Ê': 'E', 'Ë': 'E',
                    'Í': 'I', 'Ì': 'I', 'Î': 'I', 'Ï': 'I',
                    'Ó': 'O', 'Ò': 'O', 'Õ': 'O', 'Ô': 'O', 'Ö': 'O',
                    'Ú': 'U', 'Ù': 'U', 'Û': 'U', 'Ü': 'U',
                    'Ç': 'C', 'Ñ': 'N'
                }
                for old, new in replacements.items():
                    arg = arg.replace(old, new)
            safe_args.append(arg)
        print(*safe_args, **kwargs)

logger = setup_logger(__name__)


def salvar_gcs_silver(df: pd.DataFrame, subpasta: str = "clientes"):
    """
    Salva o DataFrame processado na camada silver do GCS em formato Parquet.
    
    Args:
        df: DataFrame processado a ser salvo
        subpasta: Nome da subpasta no bucket (padrão: "clientes")
        
    Returns:
        Caminho do blob no GCS
    """
    config = carregar_config()

    # Caminho para as credenciais do GCS (usa do config.yaml se disponível)
    if "credentials-path" in config and config["credentials-path"]:
        credentials_path = Path(config["credentials-path"])
    else:
        credentials_path = Path(__file__).parent.parent.parent.parent / "config" / "machtools.json"
    
    if not credentials_path.exists():
        logger.error(f"Arquivo de credenciais não encontrado: {credentials_path}")
        raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {credentials_path}")
    
    client = storage.Client.from_service_account_json(str(credentials_path))
    bucket_name = config.get("bucket-projeto", "")
    
    if not bucket_name:
        raise ValueError("Bucket do projeto não configurado. Verifique config.yaml ou variável GCS_BUCKET_PROJETO")
    
    bucket = client.bucket(bucket_name)

    # Salva na camada silver sem data no nome
    nome_arquivo = f"{subpasta}.parquet"
    blob_path = f"{config.get('bucket-silver', 'silver')}/{subpasta}/{nome_arquivo}"
    blob = bucket.blob(blob_path)

    # Converte e salva como Parquet
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    blob.upload_from_string(
        parquet_buffer.getvalue(), content_type="application/octet-stream"
    )

    logger.info(
        "Arquivo salvo na camada Silver",
        bucket=bucket_name,
        path=blob_path,
        registros=len(df),
    )
    safe_print(f"Arquivo salvo em: gs://{bucket_name}/{blob_path}")
    return blob_path


def listar_arquivos_gcs(subpasta: str = "clientes"):
    """
    Lista todos os arquivos Parquet no caminho especificado do GCS (camada Bronze).
    
    Args:
        subpasta: Nome da subpasta no bucket (padrão: "clientes")
        
    Returns:
        Lista de caminhos dos arquivos Parquet encontrados
    """
    config = carregar_config()

    # Caminho para as credenciais do GCS
    if "credentials-path" in config and config["credentials-path"]:
        credentials_path = Path(config["credentials-path"])
    else:
        credentials_path = Path(__file__).parent.parent.parent.parent / "config" / "machtools.json"
    
    client = storage.Client.from_service_account_json(str(credentials_path))
    bucket_name = config.get("bucket-projeto", "")
    bucket = client.bucket(bucket_name)

    # Mantém a mesma estrutura: bucket-raw/subpasta/
    caminho = f"{config.get('bucket-raw', 'bronze')}/{subpasta}/"
    safe_print(f"Buscando arquivos em: gs://{bucket_name}/{caminho}")

    blobs = bucket.list_blobs(prefix=caminho)
    todos_blobs = list(blobs)
    safe_print(f"Total de blobs encontrados no prefixo: {len(todos_blobs)}")

    # Lista todos os arquivos encontrados
    safe_print("Todos os arquivos encontrados:")
    for blob in todos_blobs:
        extensao = blob.name.split(".")[-1] if "." in blob.name else "sem extensao"
        safe_print(f"  - {blob.name} (tipo: {extensao})")

    # Filtra apenas arquivos Parquet (case insensitive)
    arquivos_parquet = [
        blob.name for blob in todos_blobs if blob.name.lower().endswith(".parquet")
    ]

    safe_print(f"\nArquivos Parquet encontrados ({len(arquivos_parquet)}):")
    for arquivo in arquivos_parquet:
        safe_print(f"  - {arquivo}")

    return arquivos_parquet


def ler_arquivo_gcs(caminho_arquivo: str):
    """
    Lê um arquivo Parquet específico do GCS.
    
    Args:
        caminho_arquivo: Caminho completo do arquivo no GCS
        
    Returns:
        DataFrame com os dados do arquivo
    """
    config = carregar_config()

    # Caminho para as credenciais do GCS
    if "credentials-path" in config and config["credentials-path"]:
        credentials_path = Path(config["credentials-path"])
    else:
        credentials_path = Path(__file__).parent.parent.parent.parent / "config" / "machtools.json"
    
    client = storage.Client.from_service_account_json(str(credentials_path))
    bucket_name = config.get("bucket-projeto", "")
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(caminho_arquivo)
    parquet_bytes = blob.download_as_bytes()

    df = pd.read_parquet(io.BytesIO(parquet_bytes))
    return df


def ler_todos_clientes(subpasta: str = "clientes"):
    """
    Lê todos os arquivos Parquet de clientes do GCS (camada Bronze) e retorna um DataFrame consolidado.
    
    Args:
        subpasta: Nome da subpasta no bucket (padrão: "clientes")
        
    Returns:
        DataFrame consolidado com todos os clientes
    """
    arquivos = listar_arquivos_gcs(subpasta)

    if not arquivos:
        config = carregar_config()
        caminho = f"{config.get('bucket-raw', 'bronze')}/{subpasta}/"
        safe_print(f"Nenhum arquivo encontrado em {caminho}")
        return pd.DataFrame()

    safe_print(f"Encontrados {len(arquivos)} arquivos Parquet")

    dataframes = []
    for arquivo in arquivos:
        safe_print(f"Lendo: {arquivo}")
        df = ler_arquivo_gcs(arquivo)
        safe_print(f"  - Registros neste arquivo: {len(df)}")
        dataframes.append(df)

    # Concatena todos os DataFrames mantendo a estrutura
    df_final = pd.concat(dataframes, ignore_index=True)

    # Verifica duplicatas por código_cliente_omie (sem remover ainda, será feito no processamento)
    if "codigo_cliente_omie" in df_final.columns:
        duplicatas = df_final.duplicated(subset=["codigo_cliente_omie"], keep=False)
        if duplicatas.any():
            total_duplicatas = duplicatas.sum()
            safe_print(
                f"\nATENCAO: Encontrados {total_duplicatas} registros duplicados por codigo_cliente_omie"
            )
            safe_print(
                "  - As duplicatas serao resolvidas no processamento mantendo a ultima alteracao"
            )

    safe_print(f"\nTotal de registros consolidados: {len(df_final)}")
    return df_final


def normalizar_coluna_info(df: pd.DataFrame):
    """
    Normaliza a coluna info expandindo em colunas separadas.
    
    Args:
        df: DataFrame com coluna 'info' a ser normalizada
        
    Returns:
        DataFrame com coluna 'info' expandida em colunas separadas com prefixo 'info_'
    """
    if "info" not in df.columns:
        return df

    # Cria um DataFrame temporário com as informações normalizadas
    info_df = pd.json_normalize(df["info"])

    # Adiciona prefixo 'info_' às colunas normalizadas
    info_df.columns = [f"info_{col}" for col in info_df.columns]

    # Remove a coluna info original e adiciona as colunas normalizadas
    df_normalizado = df.drop(columns=["info"])
    df_normalizado = pd.concat([df_normalizado, info_df], axis=1)

    return df_normalizado


def normalizar_coluna_tags(df: pd.DataFrame):
    """
    Normaliza a coluna tags extraindo os valores e criando linhas separadas quando há múltiplas tags.
    
    Args:
        df: DataFrame com coluna 'tags' a ser normalizada
        
    Returns:
        DataFrame com tags normalizadas (uma linha por tag)
    """
    if "tags" not in df.columns:
        return df

    # Função para extrair tags de um array/dict
    def extrair_tags(valor):
        try:
            # Verifica se é None primeiro
            if valor is None:
                return []

            # Verifica se é numpy array antes de usar pd.isna()
            if isinstance(valor, np.ndarray):
                if valor.size == 0:
                    return []
                tags_lista = []
                for item in valor:
                    if isinstance(item, dict) and "tag" in item:
                        tags_lista.append(item["tag"])
                    elif isinstance(item, str):
                        tags_lista.append(item)
                return tags_lista if tags_lista else [None]

            # Para outros tipos, verifica se é NaN
            if pd.isna(valor):
                return []

            # Verifica se é lista ou Series
            if isinstance(valor, (list, pd.Series)):
                tags_lista = []
                for item in valor:
                    if isinstance(item, dict) and "tag" in item:
                        tags_lista.append(item["tag"])
                    elif isinstance(item, str):
                        tags_lista.append(item)
                return tags_lista if tags_lista else [None]

            # Verifica se é dict
            if isinstance(valor, dict) and "tag" in valor:
                return [valor["tag"]]

            # Verifica se é string
            if isinstance(valor, str):
                return [valor]

            return [None]
        except Exception:
            return [None]

    # Extrai as tags para uma lista de listas
    df["_tags_extracted"] = df["tags"].apply(extrair_tags)

    # Cria uma coluna temporária com o número de tags
    df["_num_tags"] = df["_tags_extracted"].apply(len)

    # Separa registros com uma única tag e múltiplas tags
    df_uma_tag = df[df["_num_tags"] <= 1].copy()
    df_multiplas_tags = df[df["_num_tags"] > 1].copy()

    # Para registros com uma tag, mantém simples
    if len(df_uma_tag) > 0:
        df_uma_tag["tag"] = df_uma_tag["_tags_extracted"].apply(
            lambda x: x[0] if x and len(x) > 0 else None
        )

    # Para registros com múltiplas tags, faz explode
    if len(df_multiplas_tags) > 0:
        # Cria lista de DataFrames, um para cada tag
        dfs_exploded = []
        for idx, row in df_multiplas_tags.iterrows():
            for tag in row["_tags_extracted"]:
                row_copy = row.copy()
                row_copy["tag"] = tag
                dfs_exploded.append(row_copy)

        if dfs_exploded:
            df_multiplas_tags_exploded = pd.DataFrame(dfs_exploded)
            df_final = pd.concat([df_uma_tag, df_multiplas_tags_exploded], ignore_index=True)
        else:
            df_final = df_uma_tag
    else:
        df_final = df_uma_tag

    # Remove colunas temporárias
    df_final = df_final.drop(columns=["_tags_extracted", "_num_tags", "tags"])

    return df_final.reset_index(drop=True)


def converter_campos_maiusculas(df: pd.DataFrame):
    """
    Converte todos os campos de texto para letras maiúsculas.
    
    Args:
        df: DataFrame a ser processado
        
    Returns:
        DataFrame com campos de texto convertidos para maiúsculas
    """
    df_copy = df.copy()

    # Identifica colunas que são strings (object) e converte para maiúsculas
    for coluna in df_copy.columns:
        if df_copy[coluna].dtype == "object":
            # Converte apenas valores não nulos que são strings
            df_copy[coluna] = df_copy[coluna].apply(
                lambda x: x.upper() if isinstance(x, str) else x
            )

    return df_copy


def agrupar_por_ultima_alteracao(df: pd.DataFrame):
    """
    Agrupa por codigo_cliente_omie mantendo o registro com a última data/hora de alteração.
    
    Args:
        df: DataFrame a ser processado
        
    Returns:
        DataFrame agrupado por codigo_cliente_omie com última alteração
    """
    if "codigo_cliente_omie" not in df.columns:
        return df

    # Cria uma coluna temporária combinando data e hora de alteração para ordenação
    if "info_dAlt" in df.columns and "info_hAlt" in df.columns:
        def combinar_data_hora(row):
            try:
                dAlt = row["info_dAlt"] if pd.notna(row["info_dAlt"]) else "01/01/1900"
                hAlt = row["info_hAlt"] if pd.notna(row["info_hAlt"]) else "00:00:00"
                # Converte formato DD/MM/YYYY HH:MM:SS para datetime
                data_hora_str = f"{dAlt} {hAlt}"
                return pd.to_datetime(
                    data_hora_str, format="%d/%m/%Y %H:%M:%S", errors="coerce"
                )
            except:
                return pd.Timestamp.min

        df["_data_hora_alt"] = df.apply(combinar_data_hora, axis=1)

        # Ordena por codigo_cliente_omie e data/hora de alteração (mais recente primeiro)
        df_ordenado = df.sort_values(
            ["codigo_cliente_omie", "_data_hora_alt"],
            ascending=[True, False],
            na_position="last",
        )

        # Mantém apenas o primeiro registro de cada codigo_cliente_omie (mais recente)
        df_agrupado = df_ordenado.drop_duplicates(
            subset=["codigo_cliente_omie"], keep="first"
        )

        # Remove a coluna temporária
        df_agrupado = df_agrupado.drop(columns=["_data_hora_alt"])

        return df_agrupado.reset_index(drop=True)
    else:
        # Se não tiver as colunas de data/hora, apenas remove duplicatas
        return df.drop_duplicates(subset=["codigo_cliente_omie"], keep="first")


def processar_clientes(subpasta: str = "clientes", salvar_silver: bool = True):
    """
    Processa os clientes da camada bronze e salva na camada silver.
    
    Args:
        subpasta: Nome da subpasta no bucket (padrão: "clientes")
        salvar_silver: Se True, salva o resultado na camada Silver (padrão: True)
        
    Returns:
        DataFrame processado
    """
    logger.info("Iniciando processamento da camada Silver", subpasta=subpasta)
    
    # Lê todos os arquivos da bronze (sem remover duplicatas ainda)
    df = ler_todos_clientes(subpasta)

    if df.empty:
        safe_print("Nenhum dado para processar")
        logger.warning("Nenhum dado encontrado para processar", subpasta=subpasta)
        return df

    # Normaliza a coluna info expandindo em colunas separadas
    safe_print("\nNormalizando coluna info...")
    logger.debug("Normalizando coluna info")
    df = normalizar_coluna_info(df)

    # Agrupa por codigo_cliente_omie mantendo o registro com última alteração
    safe_print("Agrupando por codigo_cliente_omie (mantendo ultima alteracao)...")
    logger.debug("Agrupando por codigo_cliente_omie")
    df = agrupar_por_ultima_alteracao(df)

    # Normaliza a coluna tags criando linhas separadas para múltiplas tags
    safe_print("Normalizando coluna tags (criando linhas para multiplas tags)...")
    logger.debug("Normalizando coluna tags")
    registros_antes = len(df)
    df = normalizar_coluna_tags(df)
    registros_depois = len(df)
    safe_print(f"  - Registros antes: {registros_antes}")
    safe_print(f"  - Registros depois: {registros_depois}")
    safe_print(f"  - Incremento: {registros_depois - registros_antes} linhas")
    logger.info(
        "Normalizacao de tags concluida",
        registros_antes=registros_antes,
        registros_depois=registros_depois,
    )

    # Converte todos os campos de texto para maiúsculas
    safe_print("Convertendo campos de texto para maiusculas...")
    logger.debug("Convertendo campos para maiusculas")
    df = converter_campos_maiusculas(df)

    safe_print(f"\nTotal de registros apos processamento: {len(df)}")
    logger.info("Processamento concluido", total_registros=len(df))

    if salvar_silver:
        blob_path = salvar_gcs_silver(df, subpasta=subpasta)
        logger.info("Dados salvos na camada Silver", path=blob_path)

    return df


if __name__ == "__main__":
    """
    Execução direta do script.
    Exemplo de uso:
        python -m src.etl.silver.process_clientes
    """
    try:
        df = processar_clientes()
        safe_print(f"\nDataFrame shape: {df.shape}")
        safe_print(f"Colunas: {list(df.columns)}")
        logger.info("Processamento finalizado", shape=df.shape, colunas=len(df.columns))
    except Exception as e:
        logger.error(f"Erro na execucao: {e}", exc_info=True)
        safe_print(f"\n[ERRO] Erro na execucao: {e}")
        sys.exit(1)

