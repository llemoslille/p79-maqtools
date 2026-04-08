import pandas as pd
import os
from novo_projeto.config import GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_PATH
from gcs_utils import upload_to_gcs

ARQUIVO_SILVER = 'maq_prod_estoque/silver/pedidos/silver_pedido.parquet'
ARQUIVO_GOLD = 'maq_prod_estoque/gold/pedidos/gold_pedido.parquet'
os.makedirs('maq_prod_estoque/gold/pedidos', exist_ok=True)

# Configurações do GCS
GCS_PATH = "gold/pedidos/gold_pedido.parquet"


# Funcao upload_to_gcs movida para gcs_utils.py


def main():
    print("[INFO] Carregando dados silver de pedidos...")
    df = pd.read_parquet(ARQUIVO_SILVER)
    print(f"[INFO] Shape do DataFrame: {df.shape}")
    print(f"[INFO] Colunas disponíveis: {list(df.columns)}")

    # Verifica se existem colunas relacionadas aos detalhes dos pedidos
    colunas_det = [col for col in df.columns if col.startswith('det_')]
    print(f"[INFO] Colunas de detalhes encontradas: {colunas_det}")

    if not colunas_det:
        print("[AVISO] Nenhuma coluna de detalhes encontrada. Criando gold básico...")
        # Cria gold básico com informações do cabeçalho apenas
        gold = pd.DataFrame({
            'codigo_pedido': df.get('codigo_pedido', pd.NA),
            'numero_pedido': df.get('numero_pedido', pd.NA),
            'codigo_cliente': df.get('codigo_cliente', pd.NA),
            'codigo_empresa': df.get('codigo_empresa', pd.NA),
            'data_previsao': df.get('data_previsao', pd.NA),
            'etapa': df.get('etapa', pd.NA),
            'quantidade_itens': df.get('quantidade_itens', pd.NA),
            'valor_total_pedido': df.get('total_pedido_valor_total_pedido', pd.NA),
        })
    else:
        print("[INFO] Processando detalhes dos pedidos...")
        # Cria gold com informações dos detalhes
        gold = pd.DataFrame({
            'codigo_pedido': df.get('codigo_pedido', pd.NA),
            'numero_pedido': df.get('numero_pedido', pd.NA),
            'codigo_cliente': df.get('codigo_cliente', pd.NA),
            'codigo_empresa': df.get('codigo_empresa', pd.NA),
            'data_previsao': df.get('data_previsao', pd.NA),
            'etapa': df.get('etapa', pd.NA),
            'codigo_item': df.get('det_ide_codigo_item', pd.NA),
            'codigo_produto': df.get('det_produto_codigo_produto', pd.NA),
            'descricao': df.get('det_produto_descricao', pd.NA),
            'quantidade': df.get('det_produto_quantidade', pd.NA),
            'valor_unitario': df.get('det_produto_valor_unitario', pd.NA),
            'valor_total': df.get('det_produto_valor_total', pd.NA),
            'valor_mercadoria': df.get('det_produto_valor_mercadoria', pd.NA),
            'valor_ipi': df.get('det_imposto_ipi_valor_ipi', pd.NA),
            'valor_pis': df.get('det_imposto_pis_padrao_valor_pis', pd.NA),
            'valor_cofins': df.get('det_imposto_cofins_padrao_valor_cofins', pd.NA),
            'valor_credito_icms_sn': df.get('det_imposto_icms_sn_valor_credito_icms_sn', pd.NA),
            'ncm': df.get('det_produto_ncm', pd.NA),
            'cfop': df.get('det_produto_cfop', pd.NA),
        })

    # Remove linhas duplicadas
    gold = gold.drop_duplicates()

    # Salva o arquivo gold
    gold.to_parquet(ARQUIVO_GOLD, index=False)
    print(f"[OK] Gold gerado com sucesso! Arquivo: {ARQUIVO_GOLD}")

    # Faz upload para o GCS
    print(f"\n[GCS] Iniciando upload para o Google Cloud Storage...")
    upload_success = upload_to_gcs(ARQUIVO_GOLD, GCS_BUCKET_NAME, GCS_PATH)

    if upload_success:
        print(
            f"[GCS] [OK] Arquivo disponível no GCS: gs://{GCS_BUCKET_NAME}/{GCS_PATH}")
    else:
        print(
            f"[GCS] [AVISO] Upload falhou, mas arquivo local está disponível: {ARQUIVO_GOLD}")

    print(f"\n[INFO] Shape do gold: {gold.shape}")
    print(f"[INFO] Colunas do gold: {list(gold.columns)}")
    print("[INFO] Primeiras linhas:")
    print(gold.head())


if __name__ == '__main__':
    main()
