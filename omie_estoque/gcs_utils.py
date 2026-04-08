"""
Utilitários para upload no Google Cloud Storage
Versão sem emojis para compatibilidade com terminal Windows
"""
import json
from google.cloud import storage
from novo_projeto.config import GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_PATH


def upload_to_gcs(local_file_path: str, bucket_name: str, gcs_file_path: str) -> bool:
    """
    Faz upload de um arquivo local para o Google Cloud Storage

    Args:
        local_file_path (str): Caminho do arquivo local
        bucket_name (str): Nome do bucket no GCS
        gcs_file_path (str): Caminho do arquivo no GCS

    Returns:
        bool: True se o upload foi bem-sucedido, False caso contrário
    """
    try:
        # Inicializa o cliente do GCS com credenciais específicas dos arquivos locais
        client = storage.Client.from_service_account_json(
            json_credentials_path=GCS_CREDENTIALS_PATH,
            project=GCS_PROJECT_ID
        )

        # Obtém o bucket
        bucket = client.bucket(bucket_name)

        # Cria o blob (objeto no GCS)
        blob = bucket.blob(gcs_file_path)

        # Faz o upload do arquivo
        blob.upload_from_filename(local_file_path)

        print(f"[GCS] [OK] Upload bem-sucedido!")
        print(f"[GCS] Arquivo local: {local_file_path}")
        print(f"[GCS] Bucket: gs://{bucket_name}/{gcs_file_path}")
        print(f"[GCS] Project ID: {GCS_PROJECT_ID}")
        return True

    except Exception as e:
        error_msg = str(e)
        if "403" in error_msg and "permission" in error_msg.lower():
            # Lê informações da service account do arquivo local
            try:
                with open(GCS_CREDENTIALS_PATH, 'r') as f:
                    creds = json.load(f)

                print(
                    f"[GCS] [AVISO] Erro de permissao: Service account nao tem acesso de escrita ao bucket")
                print(
                    f"[GCS] Solucao: Configure as permissoes 'storage.objects.create' para a service account")
                print(f"[GCS] Service Account: {creds['client_email']}")
                print(f"[GCS] Project ID: {GCS_PROJECT_ID}")
                print(f"[GCS] Bucket: {bucket_name}")
                print(f"[GCS] Credenciais: {GCS_CREDENTIALS_PATH}")
            except Exception:
                print(f"[GCS] [AVISO] Erro de permissao no upload")
        else:
            print(f"[GCS] [ERRO] Erro no upload: {e}")
        return False


def list_gcs_files(bucket_name: str, prefix: str = None) -> list:
    """
    Lista arquivos no Google Cloud Storage

    Args:
        bucket_name (str): Nome do bucket no GCS
        prefix (str): Prefixo/pasta para filtrar arquivos (ex: 'bronze/pedido_compra_cancelado/')

    Returns:
        list: Lista de dicionários com informações dos arquivos
    """
    try:
        # Inicializa o cliente do GCS com credenciais específicas
        client = storage.Client.from_service_account_json(
            json_credentials_path=GCS_CREDENTIALS_PATH,
            project=GCS_PROJECT_ID
        )

        # Obtém o bucket
        bucket = client.bucket(bucket_name)

        # Lista os blobs (arquivos)
        blobs = bucket.list_blobs(prefix=prefix)

        arquivos = []
        for blob in blobs:
            arquivos.append({
                'name': blob.name,
                'size': blob.size,
                'updated': blob.updated,
                'content_type': blob.content_type
            })

        return arquivos

    except Exception as e:
        print(f"[GCS] [ERRO] Erro ao listar arquivos: {e}")
        return []
