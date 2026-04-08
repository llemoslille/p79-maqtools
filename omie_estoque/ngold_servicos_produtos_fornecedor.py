import pandas as pd
import traceback
import os
from novo_projeto.config import GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_PATH
from gcs_utils import upload_to_gcs

# Caminhos dos arquivos
ARQ_GOLD = 'maq_prod_estoque/gold/servicos_produtos_fornecedor/gold_servicos_produtos.parquet'
ARQ_SILVER_FORN = 'maq_prod_estoque/silver/produto_servico_fornecedor/silver_produto_fornecedor.parquet'
ARQ_GOLD_OUT = 'maq_prod_estoque/gold/servicos_produtos_fornecedor/gold_servicos_produtos_fornecedor.parquet'

# Configurações do GCS
GCS_PATH = "gold/servicos_produtos_fornecedor/gold_servicos_produtos_fornecedor.parquet"


# Funcao upload_to_gcs movida para gcs_utils.py


try:
    print('[INFO] Lendo arquivos...')

    # Verifica se os arquivos existem
    if not os.path.exists(ARQ_GOLD):
        print(f'[ERRO] Arquivo gold não encontrado: {ARQ_GOLD}')
        exit(1)

    if not os.path.exists(ARQ_SILVER_FORN):
        print(f'[ERRO] Arquivo fornecedor não encontrado: {ARQ_SILVER_FORN}')
        exit(1)

    df_gold = pd.read_parquet(ARQ_GOLD)
    print('[OK] Arquivo gold lido.')
    df_forn = pd.read_parquet(ARQ_SILVER_FORN)
    print('[OK] Arquivo fornecedor lido.')
    print(f'[INFO] Gold: {df_gold.shape}, Fornecedor: {df_forn.shape}')

    # Verifica se as colunas necessárias existem
    print(f'[INFO] Colunas gold: {list(df_gold.columns)}')
    print(f'[INFO] Colunas fornecedor: {list(df_forn.columns)}')

    # Verifica se a coluna cod_prod existe no gold
    if 'cod_prod' not in df_gold.columns:
        print('[ERRO] Coluna cod_prod não encontrada no gold')
        print('[INFO] Colunas disponíveis no gold:', list(df_gold.columns))
        exit(1)

    # Verifica se a coluna nCodProd_str existe no fornecedor
    if 'nCodProd_str' not in df_forn.columns:
        print('[ERRO] Coluna nCodProd_str não encontrada no fornecedor')
        print('[INFO] Colunas disponíveis no fornecedor:', list(df_forn.columns))
        exit(1)

    print('[INFO] Usando pandas para join...')

    # Prepara as colunas para o join
    df_gold['cod_prod_str'] = df_gold['cod_prod'].astype(str)
    # A coluna nCodProd_str já existe no fornecedor, não precisa recriar

    # Seleciona colunas do fornecedor para incluir no resultado
    colunas_fornecedor = [
        'cCodIntForn', 'cCpfCnpj', 'cNomeFantasia', 'cRazaoSocial',
        'nCodForn', 'cCodIntProd', 'cCodigo', 'cDescricao', 'nCodProd',
        'det_prod.CFOP', 'ide.tpNF', 'nCodProd_str'
    ]

    # Filtra apenas colunas que existem
    colunas_fornecedor_existentes = [
        col for col in colunas_fornecedor if col in df_forn.columns]
    print(
        f'[INFO] Colunas do fornecedor que serão incluídas: {colunas_fornecedor_existentes}')

    # Faz o join usando pandas
    df_join = pd.merge(
        df_gold,
        df_forn[colunas_fornecedor_existentes],
        left_on='cod_prod_str',
        right_on='nCodProd_str',
        how='left',
        suffixes=('', '_forn')
    )

    print(
        f'[INFO] Resultado final: {df_join.shape[0]} linhas, {df_join.shape[1]} colunas')
    print('[INFO] Primeiras linhas do resultado:')
    print(df_join.head())

    # Salva o resultado
    os.makedirs(os.path.dirname(ARQ_GOLD_OUT), exist_ok=True)
    df_join.to_parquet(ARQ_GOLD_OUT, index=False)
    print(f'[OK] Arquivo gerado: {ARQ_GOLD_OUT}')

    # Faz upload para o GCS
    print(f"\n[GCS] Iniciando upload para o Google Cloud Storage...")
    upload_success = upload_to_gcs(ARQ_GOLD_OUT, GCS_BUCKET_NAME, GCS_PATH)

    if upload_success:
        print(
            f"[GCS] [OK] Arquivo disponível no GCS: gs://{GCS_BUCKET_NAME}/{GCS_PATH}")
    else:
        print(
            f"[GCS] [AVISO] Upload falhou, mas arquivo local está disponível: {ARQ_GOLD_OUT}")

except Exception as e:
    print(f'[ERRO] Erro durante o processamento: {e}')
    traceback.print_exc()
    exit(1)
