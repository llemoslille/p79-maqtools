#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gold Layer - Validação de dados de produto-fornecedor
Gera arquivo parquet para validação
"""

import pandas as pd
import os
from novo_projeto.config import GCS_BUCKET_NAME, GCS_PROJECT_ID, GCS_CREDENTIALS_PATH
from gcs_utils import upload_to_gcs

# Caminhos dos arquivos
ARQUIVO_GOLD_FULL = 'maq_prod_estoque/gold/servicos_produtos_fornecedor/gold_produto_fornecedor_full.parquet'
ARQUIVO_VALIDACAO = 'maq_prod_estoque/gold/servicos_produtos_fornecedor/gold_produto_fornecedor_validado.parquet'

# Garante que o diretório de saída existe
os.makedirs('maq_prod_estoque/gold/servicos_produtos_fornecedor', exist_ok=True)

# Configurações do GCS
GCS_PATH = "gold/servicos_produtos_fornecedor/gold_produto_fornecedor_validado.parquet"


# Funcao upload_to_gcs movida para gcs_utils.py


def main():
    """Função principal para validação dos dados gold"""

    print("[VALIDACAO] Iniciando validação de dados gold produto-fornecedor...")

    # Verifica se o arquivo gold full existe
    if not os.path.exists(ARQUIVO_GOLD_FULL):
        print(f"[ERRO] Arquivo gold full não encontrado: {ARQUIVO_GOLD_FULL}")
        return

    # Carrega dados gold
    df_gold = pd.read_parquet(ARQUIVO_GOLD_FULL)
    print(f"[INFO] Registros carregados: {len(df_gold)}")

    # Seleciona apenas registros com fornecedor válido para validação
    df_validacao = df_gold[
        (df_gold['cnpj_fornecedor'].notna()) &
        (df_gold['cnpj_fornecedor'] != '') &
        (df_gold['nome_fantasia'].notna()) &
        (df_gold['nome_fantasia'] != '')
    ].copy()

    print(f"[INFO] Registros com fornecedor válido: {len(df_validacao)}")

    # Adiciona metadados de validação
    df_validacao['data_validacao'] = pd.Timestamp.now()
    df_validacao['tipo_validacao'] = 'fornecedor_completo'

    # Salva arquivo de validação
    df_validacao.to_parquet(ARQUIVO_VALIDACAO, index=False)
    print(f"[OK] Arquivo de validação gerado: {ARQUIVO_VALIDACAO}")

    # Faz upload para o GCS
    print(f"\n[GCS] Iniciando upload para o Google Cloud Storage...")
    upload_success = upload_to_gcs(
        ARQUIVO_VALIDACAO, GCS_BUCKET_NAME, GCS_PATH)

    if upload_success:
        print(
            f"[GCS] [OK] Arquivo disponível no GCS: gs://{GCS_BUCKET_NAME}/{GCS_PATH}")
    else:
        print(
            f"[GCS] [AVISO] Upload falhou, mas arquivo local está disponível: {ARQUIVO_VALIDACAO}")

    # Estatísticas
    print(f"\n[ESTATISTICAS]")
    print(f"- Total de registros gold: {len(df_gold)}")
    print(f"- Registros com fornecedor válido: {len(df_validacao)}")
    print(
        f"- Fornecedores únicos: {df_validacao['cnpj_fornecedor'].nunique()}")
    print(f"- Produtos únicos: {df_validacao['codigo_produto'].nunique()}")

    # Mostra amostra dos dados
    print(f"\n[AMOSTRA] Amostra dos dados validados:")
    print(df_validacao[['codigo_produto', 'descricao',
          'cnpj_fornecedor', 'nome_fantasia']].head())

    print("\n[SUCESSO] Validação concluída com sucesso!")


if __name__ == "__main__":
    main()
