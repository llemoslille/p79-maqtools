"""
Extração de Cadastro de Clientes - Camada Bronze
Extrai dados brutos da API Omie e salva no GCS em formato Parquet.
"""
import pandas as pd
from src.utils.logger import setup_logger
from src.utils.omie_config import (
    app_key,
    app_secret,
    base_url,
    version,
    endpoint_clientes,
)
from google.cloud import storage
import re
import time
import yaml
import requests
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
import io

# Função helper para print seguro no Windows


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


def resolver_caminho_credencial(raw_path: str) -> Path:
    spec = (raw_path or "").strip().strip('"').strip("'")
    if not spec:
        spec = "machtools.json"

    p = Path(spec).expanduser()
    if p.is_absolute():
        return p

    repo_root = Path(__file__).resolve().parent
    candidatos = [Path.cwd() / p, repo_root / p]
    for cand in candidatos:
        if cand.exists():
            return cand
    return repo_root / p


def carregar_config() -> dict:
    """Carrega as configurações do arquivo config.yaml"""
    config_path = Path(__file__).parent.parent.parent.parent / \
        "config" / "config.yaml"

    if not config_path.exists():
        logger.warning(
            "Arquivo config.yaml não encontrado, usando configurações padrão")
        return {
            "bucket-projeto": os.getenv("GCS_BUCKET_PROJETO", ""),
            "bucket-raw": "bronze",
        }

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Erro ao carregar config.yaml: {e}")
        raise


def normalizar_dados(dados: Any) -> Any:
    """
    Normaliza dados convertendo objetos vazios em None para compatibilidade com Parquet.

    Args:
        dados: Dados a serem normalizados (dict, list ou primitivo)

    Returns:
        Dados normalizados
    """
    if isinstance(dados, dict):
        if not dados:  # Se for um dict vazio
            return None
        return {k: normalizar_dados(v) for k, v in dados.items()}
    elif isinstance(dados, list):
        return [normalizar_dados(item) for item in dados]
    else:
        return dados


def salvar_gcs(
    clientes_lista: List[Dict[str, Any]],
    data_de: Optional[str] = None,
    data_ate: Optional[str] = None,
    subpasta: Optional[str] = None,
) -> tuple[List[str], str]:
    """
    Salva todos os clientes em um único arquivo no Google Cloud Storage em formato Parquet.

    O arquivo é sempre salvo em: gs://[bucket]/bronze/clientes/clientes.parquet
    Cada execução sobrescreve o arquivo anterior.

    Args:
        clientes_lista: Lista de clientes a serem salvos
        data_de: Data inicial do filtro (formato DD/MM/YYYY) - não usado no caminho, apenas para log
        data_ate: Data final do filtro (formato DD/MM/YYYY) - não usado no caminho, apenas para log
        subpasta: Nome da subpasta no bucket (padrão: "clientes")

    Returns:
        Tupla com (lista de IDs processados, caminho do blob no GCS)
    """
    config = carregar_config()

    # Extrai a subpasta do endpoint se não for informada
    if subpasta is None:
        # endpoint_clientes = "geral/clientes/" -> extrai "clientes"
        partes = endpoint_clientes.rstrip("/").split("/")
        subpasta = partes[-1] if partes else "clientes"

    credentials_spec = (
        (os.getenv("GCS_CREDENTIALS_JSON_PATH") or "").strip()
        or (os.getenv("MACHTOOLS_JSON_PATH") or "").strip()
        or (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
        or (config.get("credentials-path") or "").strip()
        or "machtools.json"
    )
    credentials_path = resolver_caminho_credencial(credentials_spec)

    if not credentials_path.exists():
        logger.error(
            f"Arquivo de credenciais não encontrado: {credentials_path}")
        raise FileNotFoundError(
            f"Arquivo de credenciais não encontrado: {credentials_path}")

    client = storage.Client.from_service_account_json(str(credentials_path))
    bucket_name = config.get("bucket-projeto", "")

    if not bucket_name:
        raise ValueError(
            "Bucket do projeto não configurado. Verifique config.yaml ou variável GCS_BUCKET_PROJETO")

    bucket = client.bucket(bucket_name)

    # Extrai os IDs dos clientes para o log
    ids_processados = []
    for cliente in clientes_lista:
        cliente_id = (
            cliente.get("codigo_cliente_omie")
            or cliente.get("codigo_cliente_integracao")
            or cliente.get("cnpj_cpf", "sem_id")
        )
        ids_processados.append(str(cliente_id))

    # Caminho fixo: bronze/clientes/clientes.parquet
    nome_arquivo = "clientes.parquet"
    blob_path = f"{config.get('bucket-raw', 'bronze')}/{subpasta}/{nome_arquivo}"
    blob = bucket.blob(blob_path)

    # Normaliza e converte todos os dados para DataFrame e salva como Parquet
    if clientes_lista:
        clientes_normalizados = [normalizar_dados(
            cliente) for cliente in clientes_lista]
        df = pd.DataFrame(clientes_normalizados)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        blob.upload_from_string(
            parquet_buffer.getvalue(), content_type="application/octet-stream"
        )
        logger.info(
            f"Arquivo salvo com sucesso",
            bucket=bucket_name,
            path=blob_path,
            registros=len(clientes_lista),
        )
    else:
        # Se não houver dados, cria um DataFrame vazio
        df = pd.DataFrame()
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        blob.upload_from_string(
            parquet_buffer.getvalue(), content_type="application/octet-stream"
        )
        logger.warning(
            "Nenhum cliente encontrado. Arquivo vazio criado.", path=blob_path)

    safe_print(f"\nArquivo salvo em: gs://{bucket_name}/{blob_path}")
    safe_print(
        f"IDs processados ({len(ids_processados)}): {', '.join(ids_processados[:10])}...")

    return ids_processados, blob_path


def buscar_pagina(
    pagina: int,
    registros_por_pagina: int = 500,
    apenas_importado_api: str = "N",
    filtrar_apenas_alteracao: str = "N",
    filtrar_por_data_de: Optional[str] = None,
    filtrar_por_data_ate: Optional[str] = None,
) -> dict:
    """
    Busca uma página específica de clientes da API Omie.

    Args:
        pagina: Número da página a ser buscada
        registros_por_pagina: Quantidade de registros por página (padrão: 500)
        apenas_importado_api: Filtrar apenas registros importados via API ("S" ou "N")
        filtrar_apenas_alteracao: Filtrar apenas alterações ("S" ou "N")
        filtrar_por_data_de: Data inicial do filtro (formato DD/MM/YYYY)
        filtrar_por_data_ate: Data final do filtro (formato DD/MM/YYYY)

    Returns:
        Resposta JSON da API Omie
    """
    url = f"{base_url}{version}{endpoint_clientes}"

    # Monta o payload base
    param = {
        "pagina": pagina,
        "registros_por_pagina": registros_por_pagina,
        "apenas_importado_api": apenas_importado_api,
        "filtrar_apenas_alteracao": filtrar_apenas_alteracao,
    }

    # Adiciona filtros de data apenas se forem informados
    if filtrar_por_data_de:
        param["filtrar_por_data_de"] = filtrar_por_data_de
    if filtrar_por_data_ate:
        param["filtrar_por_data_ate"] = filtrar_por_data_ate

    payload = {
        "call": "ListarClientes",
        "app_key": app_key,
        "app_secret": app_secret,
        "param": [param],
    }

    # Retry com backoff exponencial
    max_retries = 5
    retry_delay = 5  # Começa com 5 segundos

    for tentativa in range(max_retries):
        try:
            response = requests.post(
                url, headers={"Content-type": "application/json"}, json=payload, timeout=30
            )

            # Tratamento especial para erro 425 (Too Early / API bloqueada)
            if response.status_code == 425:
                try:
                    error_body = response.json()
                    faultstring = error_body.get("faultstring", "")

                    # Verifica se há mensagem de bloqueio com tempo de espera
                    if "tente novamente em" in faultstring.lower():
                        # Extrai o tempo de espera em segundos
                        match = re.search(r'(\d+)\s*segundos?',
                                          faultstring, re.IGNORECASE)
                        if match:
                            segundos_espera = int(match.group(1))
                            # Adiciona 10 segundos de margem
                            segundos_espera += 10
                            logger.warning(
                                f"API bloqueada (425). Aguardando {segundos_espera} segundos ({segundos_espera/60:.1f} minutos)..."
                            )
                            safe_print(
                                f"\n[ATENCAO] API bloqueada (425). Aguardando {segundos_espera} segundos ({segundos_espera/60:.1f} minutos)...")
                            time.sleep(segundos_espera)
                        else:
                            # Se não conseguir extrair o tempo, aguarda 30 minutos
                            logger.warning(
                                "API bloqueada (425). Aguardando 30 minutos (padrão)...")
                            safe_print(
                                "\n[ATENCAO] API bloqueada (425). Aguardando 30 minutos...")
                            time.sleep(1800)
                    else:
                        # Erro 425 genérico, aguarda 2 segundos
                        logger.warning(
                            f"Erro 425 (Too Early) na página {pagina}, aguardando 2 segundos...")
                        time.sleep(2)

                    # Continua para próxima tentativa
                    continue
                except Exception:
                    # Se não conseguir parsear o erro, aguarda e tenta novamente
                    logger.warning(
                        f"Erro 425 na página {pagina}, aguardando 2 segundos...")
                    time.sleep(2)
                    continue

            # Verifica outros erros HTTP
            if response.status_code >= 500:
                # Erros 5xx são temporários, devem ser retentados
                if tentativa < max_retries - 1:
                    segundos_espera = min(retry_delay * (2 ** tentativa), 60)
                    logger.warning(
                        f"Erro {response.status_code} na página {pagina} (tentativa {tentativa + 1}/{max_retries}). "
                        f"Aguardando {segundos_espera} segundos..."
                    )
                    safe_print(
                        f"\n[ATENCAO] Erro {response.status_code} na requisição. "
                        f"Aguardando {segundos_espera} segundos (tentativa {tentativa + 1}/{max_retries})..."
                    )
                    time.sleep(segundos_espera)
                    continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            # Se é a última tentativa, levanta o erro
            if tentativa == max_retries - 1:
                logger.error(
                    f"Erro ao buscar página {pagina} após {max_retries} tentativas: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"Response status: {e.response.status_code}")
                    try:
                        logger.error(f"Response body: {e.response.text[:500]}")
                    except:
                        pass
                raise

            # Se não é a última tentativa, aguarda e tenta novamente
            segundos_espera = min(retry_delay * (2 ** tentativa), 60)
            logger.warning(
                f"Erro na requisição (tentativa {tentativa + 1}/{max_retries}). Aguardando {segundos_espera} segundos..."
            )
            safe_print(
                f"\n[ATENCAO] Erro na requisição. Aguardando {segundos_espera} segundos (tentativa {tentativa + 1}/{max_retries})..."
            )
            time.sleep(segundos_espera)

    # Se chegou aqui, todas as tentativas falharam
    raise requests.exceptions.RequestException(
        f"Erro ao buscar página {pagina} após {max_retries} tentativas")


def extrair_clientes(
    registros_por_pagina: int = 500,
    apenas_importado_api: str = "N",
    filtrar_apenas_alteracao: str = "N",
    filtrar_por_data_de: Optional[str] = None,
    filtrar_por_data_ate: Optional[str] = None,
    salvar_gcs_flag: bool = True,
) -> dict:
    """
    Extrai todos os clientes da API Omie com paginação automática.

    Args:
        registros_por_pagina: Quantidade de registros por página (padrão: 500)
        apenas_importado_api: Filtrar apenas registros importados via API ("S" ou "N")
        filtrar_apenas_alteracao: Filtrar apenas alterações ("S" ou "N")
        filtrar_por_data_de: Data inicial do filtro (formato DD/MM/YYYY)
        filtrar_por_data_ate: Data final do filtro (formato DD/MM/YYYY)
        salvar_gcs_flag: Se True, salva os dados no GCS (padrão: True)

    Returns:
        Dicionário com total de registros, lista de clientes e IDs processados
    """
    todos_clientes = []
    pagina = 1
    total_registros = 0

    if filtrar_por_data_de and filtrar_por_data_ate:
        logger.info(
            "Iniciando extração de clientes com filtro de data",
            data_de=filtrar_por_data_de,
            data_ate=filtrar_por_data_ate,
        )
        safe_print(
            f"Consultando alteracoes de {filtrar_por_data_de} ate {filtrar_por_data_ate}")
    else:
        logger.info(
            "Iniciando extração completa de clientes (sem filtro de data)")
        safe_print("Consultando TODOS os clientes (sem filtro de data)")

    while True:
        try:
            resultado = buscar_pagina(
                pagina=pagina,
                registros_por_pagina=registros_por_pagina,
                apenas_importado_api=apenas_importado_api,
                filtrar_apenas_alteracao=filtrar_apenas_alteracao,
                filtrar_por_data_de=filtrar_por_data_de,
                filtrar_por_data_ate=filtrar_por_data_ate,
            )

            clientes_pagina = resultado.get("clientes_cadastro", [])
            if not clientes_pagina:
                logger.info("Nenhum cliente encontrado na página",
                            pagina=pagina)
                break

            todos_clientes.extend(clientes_pagina)
            total_registros = resultado.get(
                "total_de_registros", len(todos_clientes))

            logger.debug(
                "Página processada",
                pagina=pagina,
                registros_pagina=len(clientes_pagina),
                total_acumulado=len(todos_clientes),
                total_registros=total_registros,
            )
            safe_print(
                f"Pagina {pagina}: {len(clientes_pagina)} clientes | Total acumulado: {len(todos_clientes)}/{total_registros}"
            )

            # Verifica se há mais páginas
            total_paginas = resultado.get("total_de_paginas", None)
            if total_paginas:
                if pagina >= total_paginas:
                    break
            elif len(todos_clientes) >= total_registros:
                break

            # Adiciona um pequeno delay entre requisições para evitar sobrecarga na API
            time.sleep(1)  # 1 segundo entre requisições

            pagina += 1

        except Exception as e:
            logger.error(f"Erro ao processar página {pagina}: {e}")
            raise

    resultado_final = {
        "total_de_registros": len(todos_clientes),
        "clientes_cadastro": todos_clientes,
    }

    if salvar_gcs_flag:
        ids_processados, blob_path = salvar_gcs(
            todos_clientes,
            data_de=filtrar_por_data_de,
            data_ate=filtrar_por_data_ate,
        )
        resultado_final["ids_processados"] = ids_processados
        resultado_final["blob_path"] = blob_path

    logger.info(
        "Extração de clientes concluída",
        total_registros=len(todos_clientes),
        salvo_gcs=salvar_gcs_flag,
    )

    return resultado_final


if __name__ == "__main__":
    """
    Execução direta do script.
    Exemplo de uso:
        python -m src.ETL.bronze.extract_clientes
    """
    try:
        resultado = extrair_clientes()
        safe_print(
            f"\nProcessamento concluido: {resultado['total_de_registros']} clientes encontrados")
        if "ids_processados" in resultado:
            ids = resultado["ids_processados"]
            safe_print(f"\nTotal de IDs processados: {len(ids)}")
            if ids:
                safe_print(f"Primeiros IDs: {', '.join(ids[:10])}")
    except Exception as e:
        logger.error(f"Erro na execução: {e}", exc_info=True)
        sys.exit(1)
