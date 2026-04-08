import pandas as pd
import os
from datetime import datetime
from novo_projeto.config import GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_PATH
from gcs_utils import upload_to_gcs
from gcs_utils import upload_to_gcs

# Caminhos dos arquivos
ARQ_SILVER_PROD = 'maq_prod_estoque/silver/produto_servico_fornecedor/silver_produto.parquet'
ARQ_SILVER_SERV = 'maq_prod_estoque/silver/produto_servico_fornecedor/silver_servicos.parquet'
ARQ_GOLD = 'maq_prod_estoque/gold/servicos_produtos_fornecedor/gold_servicos_produtos.parquet'
os.makedirs('maq_prod_estoque/gold/servicos_produtos_fornecedor', exist_ok=True)

# Configurações do GCS
GCS_PATH = "gold/servicos_produtos_fornecedor/gold_servicos_produtos.parquet"


# Funcao upload_to_gcs movida para gcs_utils.py


# Colunas padrão da silver de produtos
COLUNAS_PADRAO = [
    'codigo',
    'codigo_familia',
    'codigo_produto',
    'descricao',
    'unidade',
    'descricao_familia',
    'marca',
    'valor_unitario',
    'dAlt',
    'dInc',
    'hAlt',
    'hInc',
    'uAlt',
    'uInc',
    'data_processamento',
    'cmc',
    'qtd_min_estoque_fisico',
    'qtd_pendente',
    'qtd_reservado',
    'qtd_saldo',
    'camada'
]


def adaptar_servicos(df_serv):
    # Para serviços: codigo_produto recebe intListar_nCodServ (numérico), codigo recebe o código do serviço (string)
    df = pd.DataFrame()
    df['codigo'] = df_serv['cabecalho_cCodigo'].astype(str)
    df['codigo_familia'] = None
    df['codigo_produto'] = pd.to_numeric(
        df_serv['intListar_nCodServ'], errors='coerce')
    df['descricao'] = df_serv['cabecalho_cDescricao']
    df['unidade'] = None
    df['descricao_familia'] = None
    df['marca'] = None
    df['valor_unitario'] = df_serv['cabecalho_nPrecoUnit']
    df['dAlt'] = df_serv.get('info_dAlt', None)
    df['dInc'] = df_serv.get('info_dInc', None)
    df['hAlt'] = df_serv.get('info_hAlt', None)
    df['hInc'] = df_serv.get('info_hInc', None)
    df['uAlt'] = df_serv.get('info_uAlt', None)
    df['uInc'] = df_serv.get('info_uInc', None)
    df['data_processamento'] = df_serv['data_processamento'] if 'data_processamento' in df_serv else datetime.now()
    # Novas colunas para serviços: sempre 0
    df['cmc'] = 0
    df['qtd_min_estoque_fisico'] = 0
    df['qtd_pendente'] = 0
    df['qtd_reservado'] = 0
    df['qtd_saldo'] = 0
    df['tp_item'] = 'servico'
    df['camada'] = 'gold'
    return df


def main():
    df_prod = pd.read_parquet(ARQ_SILVER_PROD)
    df_prod = df_prod.copy()
    df_prod['tipo'] = 'produto'
    df_prod['camada'] = 'gold'
    df_prod['tp_item'] = df_prod['tipoItem']
    df_prod['codigo_produto'] = pd.to_numeric(
        df_prod['codigo_produto'], errors='coerce')
    # Garante que as colunas extras existem em produtos (se não, preenche com 0)
    for col in ['cmc', 'qtd_min_estoque_fisico', 'qtd_pendente', 'qtd_reservado', 'qtd_saldo']:
        if col not in df_prod.columns:
            df_prod[col] = 0
    # Serviços
    df_serv = pd.read_parquet(ARQ_SILVER_SERV)
    df_serv = adaptar_servicos(df_serv)
    df_serv['tipo'] = 'servico'
    # Padroniza as colunas
    colunas_gold = COLUNAS_PADRAO + ['tp_item', 'tipo']
    df_gold = pd.concat(
        [df_prod[colunas_gold], df_serv[colunas_gold]], ignore_index=True)
    df_gold = df_gold.rename(columns={'codigo': 'cod_prod'})
    df_gold.to_parquet(ARQ_GOLD, index=False)
    print(f"[OK] Gold padronizado gerado! Arquivo: {ARQ_GOLD}")

    # Faz upload para o GCS
    print(f"\n[GCS] Iniciando upload para o Google Cloud Storage...")
    upload_success = upload_to_gcs(ARQ_GOLD, GCS_BUCKET_NAME, GCS_PATH)

    if upload_success:
        print(
            f"[GCS] [OK] Arquivo disponível no GCS: gs://{GCS_BUCKET_NAME}/{GCS_PATH}")
    else:
        print(
            f"[GCS] [AVISO] Upload falhou, mas arquivo local está disponível: {ARQ_GOLD}")

    print(f"\n[INFO] Registros: {len(df_gold)}")
    print(f"[INFO] Colunas: {list(df_gold.columns)}")
    print("[INFO] Amostra dos dados:")
    print(df_gold.tail(20))


if __name__ == "__main__":
    main()
