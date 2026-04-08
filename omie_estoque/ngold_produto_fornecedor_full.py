import pandas as pd
import os
from novo_projeto.config import GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_PATH
from gcs_utils import upload_to_gcs

# Caminhos dos arquivos
ARQ_GOLD = 'maq_prod_estoque/gold/servicos_produtos_fornecedor/gold_servicos_produtos.parquet'
ARQ_SILVER_FORN = 'maq_prod_estoque/silver/produto_servico_fornecedor/silver_produto_fornecedor.parquet'
ARQ_SAIDA = 'maq_prod_estoque/gold/servicos_produtos_fornecedor/gold_produto_fornecedor_full.parquet'

# Garante que o diretório de saída existe
os.makedirs('maq_prod_estoque/gold/servicos_produtos_fornecedor', exist_ok=True)

# Configurações do GCS
GCS_PATH = "gold/servicos_produtos_fornecedor/gold_produto_fornecedor_full.parquet"


# Funcao upload_to_gcs movida para gcs_utils.py


def main():
    # Carrega os arquivos
    df_gold = pd.read_parquet(ARQ_GOLD)
    df_forn = pd.read_parquet(ARQ_SILVER_FORN)

    # Normaliza as chaves para string (se necessário)
    df_gold['codigo_produto_str'] = df_gold['codigo_produto'].astype(str)
    if 'nCodProd_str' in df_forn.columns:
        chave_forn = 'nCodProd_str'
    else:
        chave_forn = 'nCodProd'
        df_forn['nCodProd'] = df_forn['nCodProd'].astype(str)

    # Faz o join (left join para manter todos os produtos da gold)
    df_merged = pd.merge(
        df_gold,
        df_forn,
        left_on='codigo_produto_str',
        right_on=chave_forn,
        how='left',
        suffixes=('', '_forn')
    )

    # Renomeia as colunas do fornecedor para o padrão gold
    df_merged = df_merged.rename(columns={
        'cNomeFantasia': 'nome_fantasia',
        'cRazaoSocial': 'razao_social',
        'cCpfCnpj': 'cnpj_fornecedor',
        'det_prod.CFOP': 'det_prod_CFOP'  # Renomeia para evitar problemas no BigQuery
    })

    # Mantém todas as colunas da gold + as novas de fornecedor + CFOP + CNPJ
    colunas_gold = list(df_gold.columns)
    colunas_adicionais = ['nome_fantasia',
                          'razao_social', 'cnpj_fornecedor', 'det_prod_CFOP']
    colunas_saida = colunas_gold + colunas_adicionais
    colunas_saida = [col for col in colunas_saida if col in df_merged.columns]
    df_saida = df_merged[colunas_saida]

    # Salva o resultado
    print('Colunas do DataFrame de saída:')
    print(df_saida.columns)
    df_saida.to_parquet(ARQ_SAIDA, index=False)
    print(f'[OK] Arquivo gerado: {ARQ_SAIDA}')

    # Faz upload para o GCS
    print(f"\n[GCS] Iniciando upload para o Google Cloud Storage...")
    upload_success = upload_to_gcs(ARQ_SAIDA, GCS_BUCKET_NAME, GCS_PATH)

    if upload_success:
        print(
            f"[GCS] [OK] Arquivo disponível no GCS: gs://{GCS_BUCKET_NAME}/{GCS_PATH}")
    else:
        print(
            f"[GCS] [AVISO] Upload falhou, mas arquivo local está disponível: {ARQ_SAIDA}")

    print(f'\n[INFO] Shape: {df_saida.shape}')
    print(f'[INFO] Colunas: {list(df_saida.columns)}')
    print('[INFO] Amostra dos dados:')
    print(df_saida.head())


if __name__ == '__main__':
    main()
