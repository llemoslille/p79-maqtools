import pandas as pd
import os
from google.cloud import storage
from novo_projeto.config import GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_PATH

ARQUIVO_BRONZE = 'maq_prod_estoque/bronze/produto_servico_fornecedor/bronze_produto_fornecedor.parquet'
ARQUIVO_SILVER_NF = 'maq_prod_estoque/silver/nota_fiscal/silver_notas_fiscais.parquet'
ARQUIVO_SILVER_OUT = 'maq_prod_estoque/silver/produto_servico_fornecedor/silver_produto_fornecedor.parquet'
os.makedirs('maq_prod_estoque/silver/produto_servico_fornecedor', exist_ok=True)


def normaliza_cod(x):
    try:
        return str(int(float(x))) if pd.notnull(x) else ''
    except Exception:
        return ''


def garantir_nf_local():
    """
    Garante que o arquivo de NF esteja localmente disponível.
    Se não existir local, baixa do GCS no mesmo caminho relativo.
    """
    if os.path.exists(ARQUIVO_SILVER_NF):
        return

    gcs_path = "silver/nota_fiscal/silver_notas_fiscais.parquet"
    os.makedirs(os.path.dirname(ARQUIVO_SILVER_NF), exist_ok=True)

    client = storage.Client.from_service_account_json(
        json_credentials_path=GCS_CREDENTIALS_PATH,
        project=GCS_PROJECT_ID
    )
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(gcs_path)

    if not blob.exists(client):
        raise FileNotFoundError(
            f"Arquivo nao encontrado localmente nem no GCS: gs://{GCS_BUCKET_NAME}/{gcs_path}"
        )

    blob.download_to_filename(ARQUIVO_SILVER_NF)
    print(f"[GCS] NF baixado para local: {ARQUIVO_SILVER_NF}")


def main():
    garantir_nf_local()

    df_bronze = pd.read_parquet(ARQUIVO_BRONZE)
    df_silver = pd.read_parquet(ARQUIVO_SILVER_NF)

    # Normaliza as chaves para string sem ponto/zero
    df_bronze['nCodProd_str'] = df_bronze['nCodProd'].apply(normaliza_cod)
    df_silver['nCodProd_str'] = df_silver['det_nfProdInt.nCodProd'].apply(
        normaliza_cod)

    # Faz o left join incluindo ide.tpNF e CNPJ
    df_result = pd.merge(
        df_bronze,
        df_silver[['nCodProd_str', 'det_prod.CFOP', 'ide.tpNF']],
        on='nCodProd_str',
        how='left'
    )

    df_result.to_parquet(ARQUIVO_SILVER_OUT, index=False)
    print('[OK] Silver fornecedor-produto gerado com sucesso!')
    print(df_result[['nCodProd', 'cCpfCnpj', 'cNomeFantasia',
          'det_prod.CFOP', 'ide.tpNF']].head())


if __name__ == "__main__":
    main()
